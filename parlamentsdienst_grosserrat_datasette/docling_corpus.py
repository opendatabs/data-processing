"""The Grosser Rat documents and transcripts as Markdown, JSON and pictures, converted by docling-serve.

For every document (100313) and every full session transcript (Vollprotokoll, 100348) it writes:

    data/markdown/gr_dokumente_md_docling-serve/<dok_laufnr>_docling-serve.md      Markdown with page markers
    data/docling/gr_dokumente_docling-serve/<dok_laufnr>_docling-serve.json.gz     docling's structured document
    data/images/gr_dokumente_docling-serve/<dok_laufnr>/image_*.png                the pictures, kept for later

and the same under `gr_vollprotokoll_*`, named by the session date (`tag1`). A document counts as
done once its Markdown and its JSON are there: Markdown alone is pdf_converter's older output in the
same folder and is converted again.

Three kinds of failure are told apart:

- the service is unusable (docling or the source unreachable after retries, a wrong key, a missing
  preset): the run stops and the task fails; no document is charged for it;
- the PDF no longer exists: counted against the document, but never fails the task;
- docling cannot convert this PDF: counted against the document.

Counts are kept in `docling/gr_*_docling-serve.failures.json`; a document is given up after three.

docling-serve is the service behind DOCLING_HTTP_CLIENT (with DOCLING_API_KEY). It has to be DCC's
image (ghcr.io/dcc-bs/dcc-docling-serve) with external plugins allowed and the PP-DocLayout-V3
layout preset; the run checks that before the first document.
"""

from __future__ import annotations

import base64
import copy
import gzip
import io
import json
import logging
import os
import re
import shutil
import time
import zipfile
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

import httpx
import pandas as pd
from common.retry import retry

METHOD = "docling-serve"
PAGE_BREAK = "<!-- page break -->"
IMAGE_PLACEHOLDER = "<!-- image -->"
MAX_FAILURES = 3

# What changes the text is sent explicitly; the table preset (only speed) is the server's choice.
# docling-serve 1.38 ignores the server's default OCR preset when a request leaves it out.
DOCLING_OPTIONS = {
    "to_formats": ["md", "json"],
    "md_page_break_placeholder": PAGE_BREAK,
    # DCC's setup for print: PP-OCRv6 with PP-DocLayout-V3. The layout keeps letterheads and
    # addresses out of the headings and tables of contents out of tables.
    "ocr_preset": "pp-ocrv6",
    "layout_preset": "ppdoclayout-v3",
    # Pictures as files beside the text; the Markdown gets IMAGE_PLACEHOLDER where they stand.
    "include_images": True,
    "image_export_mode": "referenced",
}
# Pictures come back as files only in a zip.
TARGET = {"kind": "zip"}
# The stage of /v1/capabilities that offers each preset we send.
PRESET_STAGES = {"ocr_preset": "ocr", "layout_preset": "layout"}
IMAGE_LINK = re.compile(r"!\[Image\]\(artifacts/[^)]+\)")

TIMEOUT = httpx.Timeout(120.0, connect=20.0)
RESULT_TIMEOUT = httpx.Timeout(300.0, connect=20.0)
TASK_TIMEOUT = 1800.0
POLL_WAIT = 5.0


class Unavailable(RuntimeError):
    """The service or the source cannot be used at all: the run stops, no document is charged."""


class DocumentFailed(RuntimeError):
    """This one document could not be converted."""


class SourceMissing(DocumentFailed):
    """The PDF no longer exists at the source: the document's problem, not docling's."""


class _Transient(Exception):
    """A server error (5xx) worth asking again."""


# Network errors and server errors are asked again, with the values common.requests_get uses.
_with_retry = retry((httpx.TransportError, _Transient), tries=6, delay=5, backoff=1, logger=logging.getLogger())


@_with_retry
def _request(client: httpx.Client, method: str, url: str, **kwargs) -> httpx.Response:
    """One request, asked again on network and server errors; any other status comes back as is."""
    response = client.request(method, url, **kwargs)
    if response.status_code >= 500:
        raise _Transient(f"{response.status_code} from {url}")
    return response


def _call(client: httpx.Client, method: str, url: str, **kwargs) -> httpx.Response:
    """A request that must succeed: otherwise the service is unusable."""
    try:
        response = _request(client, method, url, **kwargs)
    except (httpx.TransportError, _Transient) as exc:
        raise Unavailable(f"{url} does not answer: {exc}") from exc
    if response.status_code == httpx.codes.REQUEST_ENTITY_TOO_LARGE:
        # This PDF, not the service: stopping here would stop every run at the same document.
        raise DocumentFailed(f"the PDF is too large for {url}")
    if response.is_error:
        raise Unavailable(f"{url} answers {response.status_code}: {response.text[:200]}")
    return response


# ---------------------------------------------------------------- the PDF


def download_pdf(client: httpx.Client, pdf_url: str) -> bytes:
    """The PDF the open data names; SourceMissing if it is not there any more."""
    try:
        response = _request(client, "GET", pdf_url, follow_redirects=True)
    except (httpx.TransportError, _Transient) as exc:
        raise Unavailable(f"the source does not answer: {exc}") from exc
    if response.is_error:
        raise SourceMissing(f"the source answers {response.status_code} for {pdf_url}")
    # url_dok is a landing URL (?dnr=…): for a document that no longer exists it ends on an HTML
    # page with status 200, not a 404. The PDF header may sit anywhere in the first 1024 bytes.
    if b"%PDF" not in response.content[:1024]:
        kind = response.headers.get("content-type", "unknown").split(";")[0]
        raise SourceMissing(f"the source is no PDF ({kind}): the document no longer exists at {response.url}")
    return response.content


# ---------------------------------------------------------------- docling


@dataclass(frozen=True)
class Conversion:
    """docling's answer in our form: Markdown with placeholders, the document, the pictures."""

    markdown: str
    document: dict
    images: dict[str, bytes] = field(default_factory=dict)

    @classmethod
    def from_zip(cls, content: bytes) -> Conversion:
        """docling's zip: `<name>.md`, `<name>.json` and the pictures under `artifacts/`."""
        with zipfile.ZipFile(io.BytesIO(content)) as archive:
            names = archive.namelist()
            markdown = next((n for n in names if n.endswith(".md")), None)
            document = next((n for n in names if n.endswith(".json")), None)
            if markdown is None or document is None:
                raise DocumentFailed(f"docling's answer lacks Markdown or JSON: {names}")
            return cls(
                markdown=IMAGE_LINK.sub(IMAGE_PLACEHOLDER, archive.read(markdown).decode("utf-8")),
                document=json.loads(archive.read(document)),
                images={n: archive.read(n) for n in names if n.startswith("artifacts/") and not n.endswith("/")},
            )


def missing_presets(capabilities: dict) -> list[str]:
    """The presets we send that the service does not offer."""
    stages = capabilities.get("stages", {})
    missing = []
    for option, stage in PRESET_STAGES.items():
        offered = {preset.get("id") for preset in stages.get(stage, {}).get("presets", [])}
        if DOCLING_OPTIONS[option] not in offered:
            missing.append(DOCLING_OPTIONS[option])
    return missing


class Docling:
    """docling-serve behind DOCLING_HTTP_CLIENT. The API key goes only to this service."""

    def __init__(self, url: str, api_key: str = ""):
        self.url = url.rstrip("/")
        headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
        self.client = httpx.Client(headers=headers, timeout=TIMEOUT)

    @classmethod
    def from_env(cls) -> Docling:
        url = os.getenv("DOCLING_HTTP_CLIENT", "")
        if not url:
            raise Unavailable("DOCLING_HTTP_CLIENT is not set")
        return cls(url, os.getenv("DOCLING_API_KEY", ""))

    def __enter__(self) -> Docling:
        return self

    def __exit__(self, *exc) -> None:
        self.client.close()

    def check(self) -> None:
        """Stop before the first document if the service is absent or lacks DCC's presets."""
        _call(self.client, "GET", f"{self.url}/health")
        missing = missing_presets(_call(self.client, "GET", f"{self.url}/v1/capabilities").json())
        if missing:
            raise Unavailable(
                f"docling-serve at {self.url} does not offer {', '.join(missing)}: it has to be "
                "ghcr.io/dcc-bs/dcc-docling-serve with external plugins and the ppdoclayout-v3 layout preset"
            )
        try:  # only for the log
            version = _call(self.client, "GET", f"{self.url}/version").json()
            logging.info("docling-serve %s, docling %s", version.get("docling-serve"), version.get("docling"))
        except (Unavailable, ValueError) as exc:
            logging.warning("docling-serve does not tell its version: %s", exc)

    def convert(self, pdf: bytes, name: str) -> Conversion:
        """Send a PDF as base64 and wait for docling's answer."""
        payload = {
            "options": DOCLING_OPTIONS,
            "sources": [{"kind": "file", "filename": f"{name}.pdf", "base64_string": base64.b64encode(pdf).decode()}],
            "target": TARGET,
        }
        task_id = _call(self.client, "POST", f"{self.url}/v1/convert/source/async", json=payload).json()["task_id"]
        deadline = time.monotonic() + TASK_TIMEOUT
        while True:
            polled = _call(self.client, "GET", f"{self.url}/v1/status/poll/{task_id}", params={"wait": POLL_WAIT})
            status = polled.json().get("task_status", "")
            if status == "success":
                break
            if status in ("failure", "skipped"):
                raise DocumentFailed(f"docling reports {status}")
            if time.monotonic() > deadline:
                raise DocumentFailed(f"docling was not done after {TASK_TIMEOUT:.0f}s")
        result = _call(self.client, "GET", f"{self.url}/v1/result/{task_id}", timeout=RESULT_TIMEOUT)
        try:
            return Conversion.from_zip(result.content)
        except (zipfile.BadZipFile, json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise DocumentFailed(f"unexpected answer from docling: {exc}") from exc


# ---------------------------------------------------------------- the files


@dataclass(frozen=True)
class Collection:
    """Documents or transcripts: where their files go and how a row is named."""

    name: str  # gr_dokumente or gr_vollprotokoll
    name_column: str
    url_column: str
    name_of: Callable[[object], str]  # a row's value of name_column -> the name its files carry
    root: Path

    @property
    def markdown_dir(self) -> Path:
        return self.root / "markdown" / f"{self.name}_md_{METHOD}"

    @property
    def docling_dir(self) -> Path:
        return self.root / "docling" / f"{self.name}_{METHOD}"

    @property
    def images_dir(self) -> Path:
        return self.root / "images" / f"{self.name}_{METHOD}"

    @property
    def failures_path(self) -> Path:
        return self.docling_dir.with_name(f"{self.docling_dir.name}.failures.json")

    def markdown_path(self, name: str) -> Path:
        return self.markdown_dir / f"{name}_{METHOD}.md"

    def document_path(self, name: str) -> Path:
        return self.docling_dir / f"{name}_{METHOD}.json.gz"

    def is_done(self, name: str) -> bool:
        return self.markdown_path(name).exists() and self.document_path(name).exists()


def _atomic(path: Path, payload: bytes) -> None:
    """Write beside first, then move, so no half-written file is ever left behind."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".part")
    tmp.write_bytes(payload)
    tmp.replace(path)


def write(collection: Collection, name: str, conversion: Conversion) -> None:
    """Store pictures, JSON and Markdown; the Markdown last, since it marks the document as done.

    The JSON names each picture by its path relative to `data/`.
    """
    folder = collection.images_dir / name
    shutil.rmtree(folder, ignore_errors=True)
    stored = {}
    for path, payload in conversion.images.items():
        target = folder / Path(path).name
        _atomic(target, payload)
        stored[path] = target.relative_to(collection.root).as_posix()
    document = copy.deepcopy(conversion.document)
    for picture in document.get("pictures", []):
        image = picture.get("image") or {}
        if image.get("uri") in stored:
            image["uri"] = stored[image["uri"]]
    _atomic(collection.document_path(name), gzip.compress(json.dumps(document, ensure_ascii=False).encode("utf-8")))
    _atomic(collection.markdown_path(name), conversion.markdown.encode("utf-8"))


def _load_failures(collection: Collection) -> dict[str, int]:
    try:
        data = json.loads(collection.failures_path.read_text(encoding="utf-8"))
        return {str(k): int(v) for k, v in data.items()} if isinstance(data, dict) else {}
    except (OSError, ValueError, TypeError):
        return {}


def _save_failures(collection: Collection, failures: dict[str, int]) -> None:
    if not failures:
        collection.failures_path.unlink(missing_ok=True)
        return
    collection.failures_path.parent.mkdir(parents=True, exist_ok=True)
    collection.failures_path.write_text(json.dumps(failures, indent=2, sort_keys=True), encoding="utf-8")


# ---------------------------------------------------------------- the run


def convert_collection(
    docling: Docling, source: httpx.Client, df: pd.DataFrame, collection: Collection
) -> dict[str, int]:
    """Convert what is missing and count what failed; Unavailable stops it without counting."""
    rows = df[[collection.name_column, collection.url_column]].dropna()
    rows = rows[rows[collection.url_column].astype(str).str.startswith("http")]
    pending: dict[str, str] = {}
    for value, url in zip(rows[collection.name_column], rows[collection.url_column]):
        pending.setdefault(collection.name_of(value), url)  # one PDF per name, the first listed, as before

    failures = _load_failures(collection)
    todo = {
        name: url
        for name, url in pending.items()
        if not collection.is_done(name) and failures.get(name, 0) < MAX_FAILURES
    }
    logging.info(
        "%s: %d to convert, %d given up after %d attempts",
        collection.name,
        len(todo),
        sum(1 for count in failures.values() if count >= MAX_FAILURES),
        MAX_FAILURES,
    )
    counts = {"converted": 0, "failed": 0, "source_missing": 0}
    for name, pdf_url in todo.items():
        try:
            write(collection, name, docling.convert(download_pdf(source, pdf_url), name))
        except DocumentFailed as exc:
            failures[name] = failures.get(name, 0) + 1
            counts["source_missing" if isinstance(exc, SourceMissing) else "failed"] += 1
            logging.warning("%s %s: %s (%d/%d)", collection.name, name, exc, failures[name], MAX_FAILURES)
        else:
            failures.pop(name, None)
            counts["converted"] += 1
        _save_failures(collection, failures)
    logging.info("%s: %s", collection.name, counts)
    return counts


def run(documents: pd.DataFrame, sessions: pd.DataFrame, root: Path = Path("data")) -> dict[str, dict[str, int]]:
    """Bring documents and transcripts up to date.

    Raises Unavailable if the service cannot be used, and if docling converted none of the PDFs
    it was sent: then it is broken for every document, not for one.
    """
    collections = [
        (documents, Collection("gr_dokumente", "dok_laufnr", "url_dok", lambda nr: str(int(nr)), root)),
        (sessions, Collection("gr_vollprotokoll", "tag1", "url_vollprotokoll", lambda day: str(day)[:10], root)),
    ]
    with Docling.from_env() as docling, httpx.Client(timeout=TIMEOUT) as source:
        docling.check()
        result = {
            collection.name: convert_collection(docling, source, df, collection) for df, collection in collections
        }
    sent = sum(c["converted"] + c["failed"] for c in result.values())
    if sent and not any(c["converted"] for c in result.values()):
        raise Unavailable(f"docling converted none of the {sent} PDFs it was sent; see the warnings above")
    return result
