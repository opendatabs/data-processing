"""Publish changed StatRhena files and rebuild the HTML viewers.

Files in ``data_orig`` are uploaded to the FTP folder ``statrhena`` and the
matching Huwise/ODS dataset is republished when the file content changes.
``data/index.html`` and the language-specific pages are rebuilt from those
files on every run.
"""

from __future__ import annotations

import base64
import csv
import gzip
import html
import io
import json
import logging
import mimetypes
import os
import pathlib
import re
import time
import unicodedata

import common
import common.change_tracking as ct
import httpx
import pandas as pd
from dotenv import load_dotenv
from huwise_utils_py import HuwiseDataset, get_uid_by_id, list_dataset_field_configurations
from huwise_utils_py.config import HuwiseConfig
from huwise_utils_py.http import HttpClient

load_dotenv()

ROOT = pathlib.Path(__file__).resolve().parent
DATA_ORIG = ROOT / "data_orig"
OUT_DIR = ROOT / "data"
ASSETS = ROOT / "assets"
WEBP_DIR = DATA_ORIG / "webp"
DATA_FILE = DATA_ORIG / "100553_statrhena_all_databs.csv"
META_SHORT_FILE = DATA_ORIG / "100554_statrhena_metadaten_1_databs.csv"
META_LONG_FILE = DATA_ORIG / "100555_statrhena_metadaten_2_databs.csv"
LABELS_FILE = DATA_ORIG / "statrhena_spaltennamen.xlsx"
LANGS = ("de", "fr", "en")
# Fields that exist once per language as <Field>_DE / _FR / _EN.
VAR_FIELDS = {
    "topic": "Topic",
    "subtopic": "Subtopic",
    "title": "Title",
    "variable": "Variable",
    "desc": "Variable_Description",
    "unit": "Measure_Unit",
}
PLACE_FIELDS = ("land_code", "land", "region_code", "region", "kanton_code", "kanton_kreis_departement")
# Country tabs in the long metadata: page key -> column prefix (columns are <Field>_<prefix>1/2).
COUNTRIES = (("ch", "CH"), ("de", "DE"), ("fr", "FR"))
COUNTRY_FIELDS = {
    "title": "Title",
    "statistic": "Statistic",
    "source": "Source",
    "database": "Database",
    "code": "Code",
    "stichtag": "Stichtag",
    "copyright": "Copyright",
    "info": "Specific_Information",
}
# Data-tree images per topic tile: data_orig/webp/<prefix>_<TREE_VARIANT>_<LANG>.webp.
# "live" shows the indicators published in 100553; "all" also contains not yet released ones.
TREE_FILES = {
    "1": "Datenbaum_Bevölkerung",
    "2": "Datenbaum_Wirtschaft_und_Arbeit",
    "3": None,
    "4": None,
}
TREE_VARIANT = "live"
FTP_REMOTE_PATH = "statrhena"
_DATASET_FILE = re.compile(r"^(\d+)_.*\.csv$")

LANG_SWITCHER_HTML = """\
  <div class="lang" id="langSwitch">
    <button type="button" class="lang-btn" id="langBtn" aria-haspopup="listbox" aria-expanded="false">
      <img id="langFlag" alt="" />
      <span id="langLabel">Deutsch</span>
      <span class="caret">▾</span>
    </button>
    <div class="lang-menu" role="listbox" id="langMenu"></div>
  </div>"""


def b64(path: pathlib.Path) -> str:
    return base64.b64encode(path.read_bytes()).decode("ascii")


def data_uri(path: pathlib.Path) -> str:
    mime = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
    if path.suffix == ".svg":
        mime = "image/svg+xml"
    elif path.suffix == ".webp":
        mime = "image/webp"
    return f"data:{mime};base64,{b64(path)}"


def nav_icon(filename: str) -> str:
    raw = (ASSETS / filename).read_text(encoding="utf-8")
    match = re.search(r"<svg[^>]*>(.*)</svg>", raw, flags=re.DOTALL)
    body = match.group(1).strip() if match else ""
    return f'<svg viewBox="0 0 24 24" aria-hidden="true">{body}</svg>'


def render_html(font_css: str, payload: dict, *, fixed_lang: str | None, show_lang: bool) -> str:
    html_lang = fixed_lang or "de"
    return (
        TEMPLATE.replace("__FONT_CSS__", font_css)
        .replace("__PAYLOAD__", json.dumps(payload, ensure_ascii=False))
        .replace("__HTML_LANG__", html_lang)
        .replace("__FIXED_LANG__", json.dumps(fixed_lang))
        .replace("__SHOW_LANG__", "true" if show_lang else "false")
        .replace("__LANG_SWITCHER__", LANG_SWITCHER_HTML if show_lang else "")
        .replace("__ICON_1__", nav_icon("users.svg"))
        .replace("__ICON_2__", nav_icon("settings.svg"))
        .replace("__ICON_3__", nav_icon("message-square-more.svg"))
        .replace("__ICON_4__", nav_icon("rotate-ccw.svg"))
    )


def _text(value: str | None) -> str:
    text = (value or "").strip()
    return "" if text == "NA" else text


def _per_lang(row: dict[str, str], field: str) -> dict[str, str]:
    return {lang: _text(row.get(f"{field}_{lang.upper()}")) for lang in LANGS}


def _country_value(row: dict[str, str], field: str, prefix: str) -> str:
    primary = _text(row.get(f"{field}_{prefix}1"))
    secondary = _text(row.get(f"{field}_{prefix}2"))
    if primary and secondary and primary != secondary:
        return f"{primary} · {secondary}"
    return primary or secondary


def _timespan(row: dict[str, str], prefix: str) -> list[str]:
    start = _text(row.get(f"Timespan_Min_{prefix}1"))
    end = _text(row.get(f"Timespan_Max_{prefix}1"))
    if not start and not end:
        start = _text(row.get(f"Timespan_Min_{prefix}2"))
        end = _text(row.get(f"Timespan_Max_{prefix}2"))
    return [start, end]


def _read_csv(path: pathlib.Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def build_data() -> dict:
    """Shape 100553 into a compact, column-oriented structure for the page.

    Indicator texts and places are stored once and referenced by index from
    every row, which keeps the embedded payload small and fast to parse.
    """
    var_index: dict[str, int] = {}
    variables: list[dict] = []
    place_index: dict[tuple, int] = {}
    places: list[list[str]] = []
    geo_index: dict[str, int] = {}
    geos: list[str] = []
    cols: dict[str, list] = {"v": [], "y": [], "g": [], "p": [], "x": [], "d": []}

    for row in _read_csv(DATA_FILE):
        key = _text(row["Variable_DE"])
        if key not in var_index:
            var_index[key] = len(variables)
            variables.append({name: _per_lang(row, field) for name, field in VAR_FIELDS.items()})
        place = tuple(_text(row[f]) for f in PLACE_FIELDS)
        if place not in place_index:
            place_index[place] = len(places)
            places.append(list(place))
        geo = _text(row["geo"])
        if geo not in geo_index:
            geo_index[geo] = len(geos)
            geos.append(geo)
        integer = _text(row["values_integer"])
        decimal = _text(row["values_decimal"])
        if decimal:
            value, is_decimal = float(decimal), 1
        else:
            value, is_decimal = (int(integer) if integer else None), 0
        year = _text(row["jahr_annee"])
        cols["v"].append(var_index[key])
        cols["y"].append(int(year) if year.isdigit() else year)
        cols["g"].append(geo_index[geo])
        cols["p"].append(place_index[place])
        cols["x"].append(value)
        cols["d"].append(is_decimal)

    logging.info("Data: %s rows, %s indicators, %s places", len(cols["v"]), len(variables), len(places))
    return {"vars": variables, "places": places, "geos": geos, "rows": cols}


def build_metadata() -> dict:
    """Metadata per indicator (keyed by Variable_DE) from 100554 (short) and 100555 (long)."""
    meta: dict[str, dict] = {}
    for row in _read_csv(META_SHORT_FILE):
        key = _text(row.get("Variable_DE"))
        if not key:
            continue
        meta[key] = {
            "min": _text(row.get("Timespan_Min")),
            "max": _text(row.get("Timespan_Max")),
            "source": _text(row.get("Source")),
            "footnote": _per_lang(row, "Footnote"),
            "countries": {},
        }
    for row in _read_csv(META_LONG_FILE):
        key = _text(row.get("Variable_DE"))
        if not key:
            continue
        entry = meta.setdefault(
            key,
            {
                "min": _text(row.get("Timespan_Min")),
                "max": _text(row.get("Timespan_Max")),
                "source": _text(row.get("Source")),
                "footnote": _per_lang(row, "Footnote"),
                "countries": {},
            },
        )
        for code, prefix in COUNTRIES:
            fields = {name: _country_value(row, field, prefix) for name, field in COUNTRY_FIELDS.items()}
            fields["timespan"] = _timespan(row, prefix)
            entry["countries"][code] = fields
    return meta


def build_labels() -> dict:
    """Display names per technical column from the Excel mapping (empty cells are skipped)."""
    if not LABELS_FILE.exists():
        logging.warning("Label file %s missing, using technical column names", LABELS_FILE.name)
        return {}
    frame = pd.read_excel(LABELS_FILE, sheet_name="Spaltennamen", dtype=str).fillna("")
    labels: dict[str, dict[str, str]] = {}
    for record in frame.to_dict("records"):
        column = record.get("spalte", "").strip()
        if not column:
            continue
        texts = {lang: str(record.get(f"label_{lang.upper()}", "")).strip() for lang in LANGS}
        texts = {lang: text for lang, text in texts.items() if text}
        if texts:
            labels[column] = texts
    return labels


def build_tree_images(langs: tuple[str, ...] = LANGS) -> dict:
    """Data-tree images per topic tile and language, embedded as data URIs."""
    # Match names Unicode-normalised: macOS may store "ö" decomposed (NFD).
    files = {}
    if WEBP_DIR.is_dir():
        files = {unicodedata.normalize("NFC", p.name).lower(): p for p in WEBP_DIR.glob("*.webp")}
    images: dict[str, dict[str, str | None]] = {}
    for topic, prefix in TREE_FILES.items():
        images[topic] = {}
        for lang in langs:
            path = None
            if prefix:
                name = unicodedata.normalize("NFC", f"{prefix}_{TREE_VARIANT}_{lang}.webp").lower()
                path = files.get(name)
                if path is None:
                    logging.warning("Data tree image missing: webp/%s_%s_%s.webp", prefix, TREE_VARIANT, lang.upper())
            images[topic][lang] = data_uri(path) if path else None
    return images


def gzip_b64(data: bytes) -> str:
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb", filename="", mtime=0) as handle:
        handle.write(data)
    return base64.b64encode(buffer.getvalue()).decode("ascii")


def _json_gz(obj) -> str:
    return gzip_b64(json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))


def build_pages() -> None:
    """Write the self-contained viewers into data/ from the current data_orig files."""
    fonts = json.loads((ASSETS / "fonts-b64.json").read_text())
    font_css = "\n".join(
        f"""@font-face {{
  font-family: 'Work Sans';
  font-style: normal;
  font-weight: {w};
  font-display: swap;
  src: url(data:font/woff2;base64,{fonts[w]}) format('woff2');
}}"""
        for w in ("400", "500", "600", "700")
        if w in fonts
    )

    payload = {
        "data_gz": _json_gz(build_data()),
        "metadata_gz": _json_gz(build_metadata()),
        "labels": build_labels(),
        "flags": {
            "de": data_uri(ASSETS / "de.svg"),
            "en": data_uri(ASSETS / "en.svg"),
            "fr": data_uri(ASSETS / "fr.svg"),
            "ch": data_uri(ASSETS / "ch.svg"),
        },
    }

    variants = [
        ("index.html", None, True),
        ("index_de.html", "de", False),
        ("index_en.html", "en", False),
        ("index_fr.html", "fr", False),
    ]
    # The images are large, so single-language pages only carry their own language.
    trees_all = build_tree_images()
    OUT_DIR.mkdir(exist_ok=True)
    for name, fixed_lang, show_lang in variants:
        trees = trees_all
        if fixed_lang:
            trees = {topic: {fixed_lang: imgs[fixed_lang]} for topic, imgs in trees_all.items()}
        out = OUT_DIR / name
        out.write_text(
            render_html(font_css, {**payload, "trees": trees}, fixed_lang=fixed_lang, show_lang=show_lang),
            encoding="utf-8",
        )
        logging.info("Wrote %s (%s KB)", out.name, round(out.stat().st_size / 1024))


def publish_changed_files() -> None:
    """Upload each changed source CSV and republish its Huwise/ODS dataset."""
    for name in sorted(os.listdir(DATA_ORIG)):
        match = _DATASET_FILE.match(name)
        if not match:
            continue
        path = str(DATA_ORIG / name)
        dataset_id = match.group(1)
        logging.info("Checking %s (dataset %s)", name, dataset_id)
        common.update_ftp_and_odsp(path, FTP_REMOTE_PATH, dataset_id)


HUWISE_DATASETS = {"100553": DATA_FILE, "100554": META_SHORT_FILE, "100555": META_LONG_FILE}
HUWISE_PROCESSOR_PREFIX = "StatRhena labels"
_HUWISE_IDLE_TIMEOUT = 600
_HUWISE_IDLE_POLL = 5
_LANG_SUFFIX = re.compile(r"^(.+)_(DE|FR|EN)$")
_LAND_SUFFIX = re.compile(r"^(.+)_((?:CH|DE|FR)\d)$")
_LANGUAGE_NAMES = {
    "DE": {"de": "Deutsch", "fr": "allemand", "en": "German"},
    "FR": {"de": "Französisch", "fr": "français", "en": "French"},
    "EN": {"de": "Englisch", "fr": "anglais", "en": "English"},
}
_LANGUAGE_NOTE = {"de": "Sprache: {}.", "fr": "Langue : {}.", "en": "Language: {}."}


def _excel_sheet(sheet: str) -> list[dict[str, str]]:
    frame = pd.read_excel(LABELS_FILE, sheet_name=sheet, dtype=str).fillna("")
    return [{key: str(value).strip() for key, value in record.items()} for record in frame.to_dict("records")]


def _description_html(text: str) -> str:
    paragraphs = [part.strip() for part in re.split(r"\n\s*\n", text) if part.strip()]
    return "".join(f"<p>{html.escape(part, quote=False)}</p>" for part in paragraphs)


def _dataset_texts(dataset_id: str) -> dict[str, dict[str, object]]:
    """Title, HTML description and keyword list per language for one dataset from the Excel."""
    texts: dict[str, dict[str, object]] = {lang: {} for lang in LANGS}
    for record in _excel_sheet("Datensaetze"):
        if record.get("datensatz") != dataset_id:
            continue
        for lang in LANGS:
            value = record.get(lang.upper(), "")
            if record.get("feld") == "titel":
                texts[lang]["title"] = value
            elif record.get("feld") == "beschreibung":
                texts[lang]["description"] = _description_html(value)
            elif record.get("feld") == "schluesselwoerter":
                texts[lang]["keywords"] = [word.strip() for word in value.split(";") if word.strip()]
    return texts


def _field_texts(header: str, columns: dict[str, dict[str, str]]) -> dict[str, dict[str, str]] | None:
    """Label and description per language for one CSV column (None if the Excel has no entry)."""
    record = columns.get(header)
    suffix = ""
    language = ""
    if record is None and (match := _LANG_SUFFIX.match(header)):
        record = columns.get(match.group(1))
        language = match.group(2)
        suffix = f" ({language})"
    if record is None and (match := _LAND_SUFFIX.match(header)):
        record = columns.get(f"{match.group(1)}_<LAND>")
        suffix = f" ({match.group(2)})"
    if record is None:
        return None
    texts = {}
    for lang in LANGS:
        label = record.get(f"label_{lang.upper()}", "")
        description = record.get(f"beschreibung_{lang.upper()}", "")
        if language:
            note = _LANGUAGE_NOTE[lang].format(_LANGUAGE_NAMES[language][lang])
            description = f"{description} {note}".strip()
        texts[lang] = {"label": f"{label}{suffix}" if label else "", "description": description}
    return texts


def _huwise_field_name(header: str, explore_names: list[str]) -> str:
    """Field name as ingested by Huwise (Explore API if available, else the lowercased header)."""
    by_lower = {name.lower(): name for name in explore_names}
    return by_lower.get(header.lower(), header.lower())


def _add_translation(translations: dict[str, dict[str, str]], texts: dict[str, str]) -> None:
    source = texts.get("de", "")
    targets = {lang: texts[lang] for lang in ("fr", "en") if texts.get(lang) and texts[lang] != source}
    if not source or not targets:
        return
    existing = translations.setdefault(source, {})
    for lang, text in targets.items():
        if existing.get(lang, text) != text:
            logging.warning("Conflicting %s translation for %r: keeping %r", lang, source, existing[lang])
            continue
        existing[lang] = text


def _wait_for_huwise_idle(client, dataset_uid: str, dataset_id: str) -> None:
    deadline = time.monotonic() + _HUWISE_IDLE_TIMEOUT
    while True:
        status = client.get(f"/datasets/{dataset_uid}/status").json().get("status")
        if status == "idle":
            return
        if time.monotonic() >= deadline:
            logging.warning("Dataset %s still %s after %ss, continuing", dataset_id, status, _HUWISE_IDLE_TIMEOUT)
            return
        time.sleep(_HUWISE_IDLE_POLL)


def _list_field_processors(dataset_id: str) -> list[dict]:
    results: list[dict] = []
    offset = 0
    while True:
        page = list_dataset_field_configurations(dataset_id=dataset_id, limit=100, offset=offset)
        batch = page.get("results") or []
        results.extend(item for item in batch if isinstance(item, dict))
        if not batch or not page.get("next"):
            return results
        offset += 100


def push_huwise_metadata(dataset_id: str, csv_path: pathlib.Path) -> None:
    """Set title, description, keywords, field labels/descriptions and FR/EN translations on Huwise.

    German is the source language; French and English go into the dataset's
    i18n translation dictionary, which Huwise applies when the dataset is published.
    """
    config = HuwiseConfig.from_env()
    client = HttpClient(config)
    dataset_uid = get_uid_by_id(dataset_id=dataset_id)
    translations: dict[str, dict[str, str]] = {}

    # Dataset metadata (default template, German).
    texts = _dataset_texts(dataset_id)
    for field in ("title", "description", "keywords"):
        value = texts["de"].get(field)
        if not value:
            continue
        api_field = "keyword" if field == "keywords" else field
        client.put(f"/datasets/{dataset_uid}/metadata/default/{api_field}/", json={"value": value})
        if field == "keywords":
            for position, keyword in enumerate(value):
                _add_translation(
                    translations,
                    {lang: (texts[lang].get("keywords") or [""] * len(value))[position] for lang in LANGS},
                )
        else:
            _add_translation(translations, {lang: texts[lang].get(field, "") for lang in LANGS})
    client.put(f"/datasets/{dataset_uid}/metadata/default/language/", json={"value": "de"})
    logging.info("Dataset %s: title, description and keywords set", dataset_id)

    # Field labels and descriptions as processors; drop the ones from earlier runs first.
    for processor in _list_field_processors(dataset_id):
        if str(processor.get("label", "")).startswith(HUWISE_PROCESSOR_PREFIX) and processor.get("uid"):
            client.delete(f"/datasets/{dataset_uid}/fields/{processor['uid']}/")

    try:
        response = httpx.get(
            f"https://{config.domain}/api/explore/v2.1/catalog/datasets/{dataset_id}",
            headers=config.headers,
            timeout=60,
        )
        response.raise_for_status()
        explore_names = [item.get("name", "") for item in response.json().get("fields", [])]
    except Exception as exc:
        logging.warning("Dataset %s: no Explore field names (%s), using lowercased headers", dataset_id, exc)
        explore_names = []

    columns = {record["spalte"]: record for record in _excel_sheet("Spaltennamen") if record.get("spalte")}
    with csv_path.open(encoding="utf-8-sig", newline="") as handle:
        headers = next(csv.reader(handle))
    created = 0
    for header in headers:
        field_texts = _field_texts(header, columns)
        if field_texts is None:
            logging.warning("Dataset %s: no label for column %s in %s", dataset_id, header, LABELS_FILE.name)
            continue
        field = _huwise_field_name(header, explore_names)
        label = field_texts["de"]["label"]
        description = field_texts["de"]["description"]
        processors = []
        if label:
            processors.append(
                {
                    "type": "rename",
                    "label": f"{HUWISE_PROCESSOR_PREFIX}: label {field}",
                    "from_name": field,
                    "to_name": field,
                    "field_label": label,
                }
            )
            _add_translation(translations, {lang: field_texts[lang]["label"] for lang in LANGS})
        if description:
            processors.append(
                {
                    "type": "description",
                    "label": f"{HUWISE_PROCESSOR_PREFIX}: description {field}",
                    "field": field,
                    "description": description,
                }
            )
            _add_translation(translations, {lang: field_texts[lang]["description"] for lang in LANGS})
        for processor in processors:
            try:
                client.post(f"/datasets/{dataset_uid}/fields/", json=processor)
                created += 1
            except Exception as exc:
                logging.warning(
                    "Dataset %s: %s processor for %s rejected: %s", dataset_id, processor["type"], field, exc
                )
    logging.info("Dataset %s: %s field processors created", dataset_id, created)

    # FR/EN translations of all texts above (keys are the German source texts).
    client.post(f"/datasets/{dataset_uid}/i18n/translations/", json=translations)
    logging.info("Dataset %s: %s translations set", dataset_id, len(translations))

    _wait_for_huwise_idle(client, dataset_uid, dataset_id)
    HuwiseDataset(uid=dataset_uid).publish()
    _wait_for_huwise_idle(client, dataset_uid, dataset_id)
    logging.info("Dataset %s: published", dataset_id)


def push_huwise_metadata_if_changed() -> None:
    """Push the Excel contents to Huwise for all StatRhena datasets when the Excel has changed."""
    if not LABELS_FILE.exists():
        logging.warning("Label file %s missing, skipping Huwise metadata", LABELS_FILE.name)
        return
    if not ct.has_changed(str(LABELS_FILE)):
        logging.info("%s unchanged, skipping Huwise metadata", LABELS_FILE.name)
        return
    failed = []
    for dataset_id, csv_path in HUWISE_DATASETS.items():
        try:
            push_huwise_metadata(dataset_id, csv_path)
        except Exception:
            logging.exception("Huwise metadata update failed for dataset %s", dataset_id)
            failed.append(dataset_id)
    if failed:
        logging.warning("Huwise metadata not complete (%s); will retry on the next run", ", ".join(failed))
        return
    ct.update_hash_file(str(LABELS_FILE))


def main() -> None:
    logging.info("Building HTML viewers from data_orig")
    build_pages()
    publish_changed_files()
    push_huwise_metadata_if_changed()


TEMPLATE = r"""<!DOCTYPE html>
<html lang="__HTML_LANG__">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>StatRhena: Daten und Metadaten</title>
<style>
__FONT_CSS__

:root {
  --blue: #116a8d;
  --blue-dark: #0e5673;
  --ink: #222531;
  --muted: #5b6070;
  --line: #e6e7eb;
  --soft: #f7f7f7;
  --font: "Work Sans", system-ui, sans-serif;
  --row-h: 36px;
}

*, *::before, *::after { box-sizing: border-box; }
html { scroll-behavior: smooth; }
body {
  margin: 0;
  font-family: var(--font);
  color: var(--ink);
  background: #fff;
  line-height: 1.5;
  -webkit-font-smoothing: antialiased;
}
a { color: inherit; text-decoration: none; }
img { max-width: 100%; display: block; }
button, select, input { font: inherit; }

.wrap {
  width: min(1600px, 100%);
  margin: 0 auto;
  padding: 40px 32px 48px;
}
@media (max-width: 560px) { .wrap { padding: 28px 16px 40px; } }

.page-bar {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 16px 24px;
  margin-bottom: 14px;
}
.page-title {
  margin: 0;
  flex: 1;
  min-width: 0;
  font-size: clamp(2.1rem, 4vw, 2.85rem);
  font-weight: 700;
  letter-spacing: -0.025em;
}
.intro {
  margin: 0 0 28px;
  max-width: 80ch;
  color: var(--muted);
  font-size: 1.05rem;
}

.lang { position: relative; flex: 0 0 auto; margin-top: 6px; }
.lang-btn {
  display: inline-flex; align-items: center; gap: 8px;
  padding: 7px 12px;
  border: 1px solid var(--line);
  border-radius: 4px;
  background: #fff;
  cursor: pointer;
  color: var(--ink);
  font-weight: 500;
  font-size: .92rem;
}
.lang-btn img { width: 22px; height: 16px; object-fit: cover; border-radius: 2px; }
.lang-btn .caret { font-size: .7rem; color: var(--muted); }
.lang-menu {
  display: none;
  position: absolute; right: 0; top: calc(100% + 6px);
  min-width: 160px;
  background: #fff;
  border: 1px solid var(--line);
  box-shadow: 0 12px 28px rgba(34,37,49,.1);
  padding: 6px 0;
  z-index: 60;
}
.lang.open .lang-menu { display: block; }
.lang-menu button {
  width: 100%;
  display: flex; align-items: center; gap: 10px;
  padding: 9px 14px;
  border: 0; background: none;
  cursor: pointer;
  text-align: left;
  font-weight: 500;
}
.lang-menu button:hover, .lang-menu button.active { background: var(--soft); }
.lang-menu img { width: 22px; height: 16px; object-fit: cover; border-radius: 2px; }

/* Topic tiles: each opens the data tree image */
.theme-nav {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 12px;
  margin-bottom: 40px;
}
@media (max-width: 800px) { .theme-nav { grid-template-columns: repeat(2, minmax(0, 1fr)); } }
.tile {
  position: relative;
  display: flex;
  flex-direction: column;
  gap: 14px;
  min-height: 132px;
  padding: 16px;
  border: 0;
  background: var(--blue);
  color: #fff;
  text-align: left;
  cursor: pointer;
  box-shadow: 0 1px 2px rgba(34,37,49,.12);
  transition: background .15s ease, transform .15s ease, box-shadow .15s ease;
}
.tile:hover, .tile:focus-visible {
  background: var(--blue-dark);
  transform: translateY(-3px);
  box-shadow: 0 10px 22px rgba(17,106,141,.28);
}
.tile:focus-visible { outline: 3px solid #f2b705; outline-offset: 2px; }
.tile svg {
  width: 30px; height: 30px;
  stroke: #fff; fill: none; stroke-width: 2;
  stroke-linecap: round; stroke-linejoin: round;
}
.tile .tile-name { font-weight: 600; font-size: 1rem; flex: 1; }
.tile .tile-cta {
  display: inline-flex; align-items: center; gap: 6px;
  font-size: .85rem; font-weight: 600;
  padding-top: 10px;
  border-top: 1px solid rgba(255,255,255,.35);
}
.tile .tile-cta .arrow { transition: transform .15s ease; }
.tile:hover .tile-cta .arrow { transform: translateX(4px); }
.tile .tile-zoom {
  position: absolute; top: 14px; right: 14px;
  width: 30px; height: 30px;
  display: grid; place-items: center;
  border-radius: 50%;
  background: rgba(255,255,255,.18);
}
.tile .tile-zoom svg { width: 16px; height: 16px; }

/* Lightbox */
dialog.lightbox {
  width: calc(100vw - 32px);
  max-width: 1600px;
  height: calc(100vh - 32px);
  max-height: none;
  padding: 0;
  border: 0;
  box-shadow: 0 24px 60px rgba(0,0,0,.35);
  overflow: hidden;
}
dialog.lightbox[open] { display: flex; flex-direction: column; }
dialog.lightbox::backdrop { background: rgba(20,22,30,.7); }
.lb-head {
  display: flex; align-items: center; justify-content: space-between; flex-wrap: wrap; gap: 8px 12px;
  padding: 10px 16px;
  border-bottom: 1px solid var(--line);
  background: #fff;
}
.lb-head h2 { margin: 0; font-size: 1.15rem; font-weight: 600; flex: 1 1 auto; min-width: 0; }
.lb-tools { display: flex; align-items: center; gap: 6px; }
.lb-btn {
  display: inline-grid; place-items: center;
  min-width: 36px; height: 36px; padding: 0 10px;
  border: 1px solid var(--line); background: #fff; cursor: pointer;
  font-size: 1.1rem; font-weight: 600; line-height: 1; color: var(--ink);
}
.lb-btn.text { font-size: .85rem; }
.lb-btn:hover:not(:disabled) { border-color: var(--blue); color: var(--blue); }
.lb-btn:disabled { opacity: .4; cursor: not-allowed; }
.lb-btn:focus-visible { outline: 2px solid var(--blue); outline-offset: 1px; }
.lb-zoom { min-width: 52px; text-align: center; font-size: .85rem; font-variant-numeric: tabular-nums; color: var(--muted); }
.lb-sep { width: 1px; height: 24px; background: var(--line); margin: 0 4px; }
.lb-view {
  position: relative;
  flex: 1 1 auto;
  min-height: 0;
  overflow: auto;
  background: var(--soft);
  cursor: grab;
  overscroll-behavior: contain;
}
.lb-view.dragging { cursor: grabbing; user-select: none; }
.lb-view img { max-width: none; margin: 0 auto; -webkit-user-drag: none; user-select: none; }
.lb-hint {
  padding: 6px 16px;
  border-top: 1px solid var(--line);
  color: var(--muted);
  font-size: .8rem;
  background: #fff;
}
.lb-missing { padding: 40px 16px; text-align: center; color: var(--muted); }

/* Sections */
.block { padding: 8px 0 36px; }
.block-title {
  margin: 0 0 14px;
  font-size: clamp(1.45rem, 2.4vw, 1.85rem);
  font-weight: 600;
  letter-spacing: -0.02em;
}

/* Search + filters */
.search-field { margin-bottom: 16px; }
.search-field input {
  width: 100%;
  padding: 11px 12px;
  border: 1px solid var(--line);
  font-size: 1rem;
  min-height: 46px;
}
.search-field input:focus { outline: 2px solid var(--blue); outline-offset: -1px; }
.flabel {
  font-size: .75rem;
  font-weight: 600;
  color: var(--muted);
  text-transform: uppercase;
  letter-spacing: .03em;
  line-height: 1.25;
  overflow-wrap: anywhere;
  margin-bottom: 6px;
}
.filters {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 14px 16px;
  align-items: end;
}
@media (max-width: 900px) { .filters { grid-template-columns: repeat(2, minmax(0, 1fr)); } }
@media (max-width: 560px) { .filters { grid-template-columns: 1fr; } }
.filter-field { min-width: 0; }
.ms { position: relative; }
.ms-btn {
  width: 100%;
  display: flex; align-items: center; justify-content: space-between; gap: 8px;
  padding: 9px 10px;
  border: 1px solid var(--line);
  background: #fff;
  cursor: pointer;
  text-align: left;
  font-size: .9rem;
  color: var(--ink);
  min-height: 40px;
}
.ms-summary {
  overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
  min-width: 0; flex: 1;
  color: var(--muted);
}
.ms-summary.has { color: var(--ink); font-weight: 500; }
.ms-summary.auto { font-style: italic; }
.ms-caret { color: var(--muted); flex: 0 0 auto; }
.ms.open .ms-btn { border-color: var(--blue); }
.ms-panel {
  display: none;
  position: absolute; left: 0; right: 0; top: calc(100% + 4px);
  background: #fff;
  border: 1px solid var(--line);
  box-shadow: 0 10px 28px rgba(34,37,49,.12);
  z-index: 40;
}
.ms.open .ms-panel { display: block; }
.ms-find { padding: 8px; border-bottom: 1px solid var(--line); }
.ms-find input { width: 100%; padding: 6px 8px; border: 1px solid var(--line); font-size: .85rem; }
.ms-opts { max-height: 260px; overflow: auto; padding: 6px 0; }
.ms-opts label {
  display: flex; align-items: flex-start; gap: 8px;
  padding: 7px 12px;
  font-size: .88rem;
  cursor: pointer;
}
.ms-opts label:hover { background: var(--soft); }
.ms-opts label[hidden], .ms-empty[hidden] { display: none; }
.ms-opts input { margin-top: 3px; flex: 0 0 auto; }
.ms-empty { padding: 8px 12px; color: var(--muted); font-size: .85rem; }
.filter-actions { margin-top: 14px; display: flex; gap: 8px; }
.btn-ghost {
  padding: 9px 14px;
  border: 1px solid var(--line);
  background: #fff;
  color: var(--muted);
  font-weight: 600;
  cursor: pointer;
}
.btn-ghost:hover { border-color: var(--blue); color: var(--blue); }

/* Virtualised table */
.table-wrap {
  overflow: auto;
  border: 1px solid var(--line);
  height: min(70vh, 640px);
  position: relative;
}
table.data {
  border-collapse: separate;
  border-spacing: 0;
  table-layout: fixed;
  font-size: .85rem;
}
table.data th, table.data td {
  padding: 0 10px;
  border-bottom: 1px solid var(--line);
  text-align: left;
}
table.data td {
  height: var(--row-h);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
table.data th {
  position: sticky; top: 0;
  background: var(--soft);
  font-weight: 600;
  font-size: .78rem;
  padding: 8px 10px;
  z-index: 2;
  cursor: pointer;
  user-select: none;
  vertical-align: bottom;
  overflow-wrap: anywhere;
  text-transform: uppercase;
  letter-spacing: .03em;
}
table.data th:hover { color: var(--blue); }
table.data th .sort-ind { margin-left: 4px; color: var(--muted); font-weight: 500; }
table.data th.sorted .sort-ind { color: var(--blue); }
table.data tr.spacer td { border: 0; padding: 0; height: auto; }
table.data tbody tr:not(.spacer):hover td { background: #fafafa; }
table.data td.num, table.data th.num { text-align: right; font-variant-numeric: tabular-nums; }
.table-foot { margin-top: 10px; color: var(--muted); font-size: .9rem; }
.table-none { padding: 24px 12px; color: var(--muted); }

/* Metadata accordion */
.meta-count { color: var(--muted); font-size: .9rem; margin: -6px 0 12px; }
.meta-list { display: grid; grid-template-columns: minmax(0, 1fr); gap: 8px; }
.meta-item { border: 1px solid var(--line); background: #fff; min-width: 0; }
.meta-item > summary {
  list-style: none;
  cursor: pointer;
  display: flex; align-items: center; gap: 14px;
  padding: 12px 14px;
}
.meta-item > summary::-webkit-details-marker { display: none; }
.meta-item > summary:hover { background: var(--soft); }
.meta-item > summary:focus-visible { outline: 2px solid var(--blue); outline-offset: -2px; }
.meta-item[open] > summary { border-bottom: 1px solid var(--line); background: var(--soft); }
.meta-head { flex: 1; min-width: 0; display: grid; gap: 2px; }
.meta-name { font-weight: 600; font-size: 1rem; overflow-wrap: anywhere; }
.meta-path { color: var(--muted); font-size: .82rem; }
.meta-toggle {
  flex: 0 0 auto;
  display: inline-flex; align-items: center; gap: 6px;
  padding: 5px 10px;
  border: 1px solid var(--blue);
  color: var(--blue);
  font-size: .8rem; font-weight: 600;
  border-radius: 999px;
  white-space: nowrap;
}
.meta-toggle .chev { display: inline-block; transition: transform .15s ease; }
.meta-item[open] .meta-toggle { background: var(--blue); color: #fff; }
.meta-item[open] .meta-toggle .chev { transform: rotate(180deg); }
.meta-body { padding: 12px 14px 16px; }
.kv {
  display: grid;
  grid-template-columns: minmax(110px, 170px) 1fr;
  gap: 2px 12px;
  margin-bottom: 6px;
  align-items: start;
}
.kv b { color: var(--muted); font-weight: 600; font-size: .78rem; text-transform: uppercase; letter-spacing: .03em; line-height: 1.4; padding-top: 2px; }
.kv span { font-size: .93rem; line-height: 1.45; white-space: pre-wrap; word-break: break-word; }
@media (max-width: 560px) { .kv { grid-template-columns: 1fr; } }
.meta-tabs { margin-top: 14px; }
.meta-tablist {
  display: flex; flex-wrap: wrap; gap: 4px;
  margin-bottom: 10px;
  border-bottom: 1px solid var(--line);
}
.meta-tab {
  display: inline-flex; align-items: center; gap: 6px;
  padding: 6px 12px;
  border: 1px solid transparent;
  border-bottom: 0;
  margin-bottom: -1px;
  background: transparent;
  cursor: pointer;
  color: var(--muted);
  font-weight: 500;
  font-size: .88rem;
}
.meta-tab img { width: 22px; height: 16px; object-fit: cover; border-radius: 2px; border: 1px solid var(--line); }
.meta-tab:hover { color: var(--ink); background: var(--soft); }
.meta-tab.active { color: var(--ink); background: #fff; border-color: var(--line); font-weight: 600; }
.meta-tabpanel { display: none; }
.meta-tabpanel.active { display: block; }
.meta-empty { color: var(--muted); font-size: .9rem; }

/* Downloads */
.download-box { padding: 4px 16px 16px; border: 1px solid var(--line); background: var(--soft); }
.download-row { padding: 14px 0; border-bottom: 1px solid var(--line); }
.download-row:last-child { border-bottom: 0; padding-bottom: 0; }
.download-row h3 { margin: 0 0 10px; font-size: .98rem; font-weight: 600; }
.exports { display: flex; flex-wrap: wrap; gap: 8px; align-items: center; }
.exports a {
  display: inline-flex; align-items: center; gap: 6px;
  padding: 8px 12px;
  border: 1px solid var(--line);
  background: #fff;
  font-weight: 600; font-size: .85rem; letter-spacing: .02em;
}
.exports a:hover { border-color: var(--blue); color: var(--blue); }
.api-note { flex-basis: 100%; color: var(--muted); font-size: .78rem; margin-top: 4px; word-break: break-all; }
.api-note code { font-size: .76rem; }

.status {
  padding: 12px 14px;
  background: #f0faf4;
  border: 1px solid #b9e0c8;
  color: #1f6b3f;
  margin-bottom: 12px;
  font-size: .9rem;
}
.status.error { background: #fff1f0; border-color: #f0c2c0; color: #8a2f2a; }
.loading { display: inline-flex; align-items: center; gap: 10px; color: var(--muted); }
.spinner {
  width: 16px; height: 16px; border: 2px solid var(--line);
  border-top-color: var(--blue); border-radius: 50%;
  animation: spin .8s linear infinite;
}
@keyframes spin { to { transform: rotate(360deg); } }

@media (max-width: 720px) {
  .page-bar { flex-direction: column; align-items: stretch; }
  .lang { align-self: flex-end; margin-top: 0; }
}
</style>
</head>
<body>
<main class="wrap">
  <div class="page-bar">
    <h1 class="page-title" data-i18n="page.title"></h1>
__LANG_SWITCHER__
  </div>
  <p class="intro" data-i18n="page.intro"></p>

  <nav class="theme-nav" aria-label="Themen">
    <button type="button" class="tile" data-tree="1">
      __ICON_1__
      <span class="tile-name" data-i18n="theme.1"></span>
      <span class="tile-cta"><span data-i18n="tree.open"></span><span class="arrow">→</span></span>
    </button>
    <button type="button" class="tile" data-tree="2">
      __ICON_2__
      <span class="tile-name" data-i18n="theme.2"></span>
      <span class="tile-cta"><span data-i18n="tree.open"></span><span class="arrow">→</span></span>
    </button>
    <button type="button" class="tile" data-tree="3">
      __ICON_3__
      <span class="tile-name" data-i18n="theme.3"></span>
      <span class="tile-cta"><span data-i18n="tree.open"></span><span class="arrow">→</span></span>
    </button>
    <button type="button" class="tile" data-tree="4">
      __ICON_4__
      <span class="tile-name" data-i18n="theme.4"></span>
      <span class="tile-cta"><span data-i18n="tree.open"></span><span class="arrow">→</span></span>
    </button>
  </nav>

  <dialog class="lightbox" id="lightbox">
    <div class="lb-head">
      <h2 id="lbTitle"></h2>
      <div class="lb-tools">
        <button type="button" class="lb-btn" id="lbOut" data-i18n-title="tree.zoomOut">−</button>
        <span class="lb-zoom" id="lbZoom" aria-live="polite">100%</span>
        <button type="button" class="lb-btn" id="lbIn" data-i18n-title="tree.zoomIn">+</button>
        <button type="button" class="lb-btn text" id="lbFit" data-i18n="tree.fit"></button>
        <span class="lb-sep"></span>
        <button type="button" class="lb-btn" id="lbClose" data-i18n-title="tree.close">×</button>
      </div>
    </div>
    <div class="lb-view" id="lbView"></div>
    <div class="lb-hint" data-i18n="tree.hint"></div>
  </dialog>

  <div id="status" hidden></div>

  <section class="block" id="filter">
    <h2 class="block-title" data-i18n="filters.title"></h2>
    <div class="search-field">
      <div class="flabel" id="searchLabel"></div>
      <input type="search" id="search" autocomplete="off" />
    </div>
    <div class="filters" id="filters"></div>
    <div class="filter-actions">
      <button type="button" class="btn-ghost" id="reset" data-i18n="filter.reset"></button>
    </div>
  </section>

  <section class="block" id="tabelle">
    <h2 class="block-title" data-i18n="table.title"></h2>
    <div class="table-wrap" id="tableWrap">
      <table class="data">
        <colgroup id="cols"></colgroup>
        <thead id="thead"></thead>
        <tbody id="tbody"></tbody>
      </table>
    </div>
    <div class="table-foot" id="tableInfo">–</div>
  </section>

  <section class="block" id="metadaten">
    <h2 class="block-title" data-i18n="meta.title"></h2>
    <p class="meta-count" id="metaCount"></p>
    <div class="meta-list" id="metaList"></div>
  </section>

  <section class="block" id="download">
    <h2 class="block-title" data-i18n="download.title"></h2>
    <div class="download-box">
      <div class="download-row">
        <h3 data-i18n="download.all"></h3>
        <div class="exports" id="exportsAll"></div>
      </div>
      <div class="download-row">
        <h3 data-i18n="download.filtered"></h3>
        <div class="exports" id="exportsFiltered"></div>
      </div>
      <div class="download-row">
        <h3 data-i18n="download.metaShort"></h3>
        <div class="exports" id="exportsMetaShort"></div>
      </div>
      <div class="download-row">
        <h3 data-i18n="download.metaLong"></h3>
        <div class="exports" id="exportsMetaLong"></div>
      </div>
    </div>
  </section>
</main>

<script>
const PAYLOAD = __PAYLOAD__;
const FIXED_LANG = __FIXED_LANG__;
const SHOW_LANG = __SHOW_LANG__;
const DATA_ID = "100553";
const META_SHORT_ID = "100554";
const META_LONG_ID = "100555";
const META_COUNTRIES = ["ch", "de", "fr"];
// Finest level first; without a RAUMEBENE filter the table shows the finest level present.
const GEO_HIERARCHY = ["Gemeinde", "Kanton/Kreis/Departement", "Region", "Land", "Oberrhein"];
const LANGS = [
  { id: "de", label: "Deutsch" },
  { id: "en", label: "English" },
  { id: "fr", label: "Français" },
];

const I18N = {
  de: {
    "page.title": "StatRhena: Daten und Metadaten",
    "page.intro": "Entdecken Sie die Daten zum Oberrhein. Mit der Suche und den Filtern finden Sie schnell, was Sie brauchen, und können es direkt ansehen, herunterladen oder automatisiert über unsere API abrufen.",
    "theme.1": "Bevölkerung",
    "theme.2": "Wirtschaft & Arbeit",
    "theme.3": "Gesellschaft",
    "theme.4": "Umwelt & Mobilität",
    "tree.open": "Datenbaum ansehen",
    "tree.title": "Datenbaum",
    "tree.missing": "Der Datenbaum für dieses Thema folgt in Kürze.",
    "tree.close": "Schliessen",
    "tree.zoomIn": "Vergrössern",
    "tree.zoomOut": "Verkleinern",
    "tree.fit": "Einpassen",
    "tree.hint": "Bild mit gedrückter Maustaste verschieben · Zoomen mit Doppelklick (Shift: verkleinern), + / − oder Strg + Mausrad",
    "filters.title": "Suche und Filter",
    "filter.searchPlaceholder": "Indikator suchen, z. B. Geburten…",
    "filter.all": "Alle",
    "filter.selected": "ausgewählt",
    "filter.auto": "Automatisch",
    "filter.find": "Optionen durchsuchen…",
    "filter.noOptions": "Keine Optionen",
    "filter.noMatch": "Keine Treffer",
    "filter.reset": "Alle Filter zurücksetzen",
    "table.title": "Tabelle",
    "table.rows": "Zeilen",
    "table.none": "Keine Treffer für die aktuelle Auswahl.",
    "table.geoAuto": "automatisch gewählte Raumebene",
    "meta.title": "Metadaten",
    "meta.count": "Indikatoren in der aktuellen Auswahl",
    "meta.details": "Details",
    "meta.none": "Keine Indikatoren für die aktuelle Auswahl.",
    "meta.unit": "Einheit",
    "meta.period": "Zeitraum",
    "meta.source": "Quelle",
    "meta.footnote": "Fussnote",
    "meta.sourceTitle": "Originaltitel",
    "meta.statistic": "Statistik",
    "meta.database": "Datenbank",
    "meta.code": "Code",
    "meta.stichtag": "Stichtag",
    "meta.copyright": "Copyright",
    "meta.info": "Methodik / Hinweise",
    "meta.empty": "Für dieses Land liegen keine Angaben vor.",
    "meta.country.ch": "Schweiz",
    "meta.country.de": "Deutschland",
    "meta.country.fr": "Frankreich",
    "download.title": "Download",
    "download.all": "Alle Daten",
    "download.filtered": "Gefilterte Daten",
    "download.metaShort": "Metadaten kurz",
    "download.metaLong": "Metadaten lang",
    "download.api": "API-Export:",
    "status.loading": "Lade Daten…",
  },
  en: {
    "page.title": "StatRhena: Data and Metadata",
    "page.intro": "Explore data on the Upper Rhine. Use the search and filters to quickly find what you need, then view it directly, download it, or retrieve it automatically through our API.",
    "theme.1": "Population",
    "theme.2": "Economy & Labour",
    "theme.3": "Society",
    "theme.4": "Environment & Mobility",
    "tree.open": "View data tree",
    "tree.title": "Data tree",
    "tree.missing": "The data tree for this topic will be available soon.",
    "tree.close": "Close",
    "tree.zoomIn": "Zoom in",
    "tree.zoomOut": "Zoom out",
    "tree.fit": "Fit",
    "tree.hint": "Drag the image to move it · Zoom with double-click (Shift: zoom out), + / − or Ctrl + mouse wheel",
    "filters.title": "Search and filters",
    "filter.searchPlaceholder": "Search indicators, e.g. births…",
    "filter.all": "All",
    "filter.selected": "selected",
    "filter.auto": "Automatic",
    "filter.find": "Search options…",
    "filter.noOptions": "No options",
    "filter.noMatch": "No matches",
    "filter.reset": "Reset all filters",
    "table.title": "Table",
    "table.rows": "rows",
    "table.none": "No results for the current selection.",
    "table.geoAuto": "geographic level chosen automatically",
    "meta.title": "Metadata",
    "meta.count": "indicators in the current selection",
    "meta.details": "Details",
    "meta.none": "No indicators for the current selection.",
    "meta.unit": "Unit",
    "meta.period": "Period",
    "meta.source": "Source",
    "meta.footnote": "Footnote",
    "meta.sourceTitle": "Original title",
    "meta.statistic": "Statistic",
    "meta.database": "Database",
    "meta.code": "Code",
    "meta.stichtag": "Reference date",
    "meta.copyright": "Copyright",
    "meta.info": "Methodology / notes",
    "meta.empty": "No information available for this country.",
    "meta.country.ch": "Switzerland",
    "meta.country.de": "Germany",
    "meta.country.fr": "France",
    "download.title": "Download",
    "download.all": "All data",
    "download.filtered": "Filtered data",
    "download.metaShort": "Metadata (short)",
    "download.metaLong": "Metadata (long)",
    "download.api": "API export:",
    "status.loading": "Loading data…",
  },
  fr: {
    "page.title": "StatRhena: Données et métadonnées",
    "page.intro": "Découvrez les données sur le Rhin supérieur. Grâce à la recherche et aux filtres, trouvez rapidement ce dont vous avez besoin, puis consultez-le directement, téléchargez-le ou récupérez-le automatiquement via notre API.",
    "theme.1": "Population",
    "theme.2": "Économie & Emploi",
    "theme.3": "Société",
    "theme.4": "Environnement & Mobilité",
    "tree.open": "Voir l’arborescence",
    "tree.title": "Arborescence des données",
    "tree.missing": "L’arborescence de ce thème sera bientôt disponible.",
    "tree.close": "Fermer",
    "tree.zoomIn": "Agrandir",
    "tree.zoomOut": "Réduire",
    "tree.fit": "Ajuster",
    "tree.hint": "Faites glisser l’image pour la déplacer · Zoom par double-clic (Maj : réduire), + / − ou Ctrl + molette",
    "filters.title": "Recherche et filtres",
    "filter.searchPlaceholder": "Rechercher un indicateur, p. ex. naissances…",
    "filter.all": "Tous",
    "filter.selected": "sélectionné(s)",
    "filter.auto": "Automatique",
    "filter.find": "Rechercher dans les options…",
    "filter.noOptions": "Aucune option",
    "filter.noMatch": "Aucun résultat",
    "filter.reset": "Réinitialiser tous les filtres",
    "table.title": "Tableau",
    "table.rows": "lignes",
    "table.none": "Aucun résultat pour la sélection actuelle.",
    "table.geoAuto": "échelle territoriale choisie automatiquement",
    "meta.title": "Métadonnées",
    "meta.count": "indicateurs dans la sélection actuelle",
    "meta.details": "Détails",
    "meta.none": "Aucun indicateur pour la sélection actuelle.",
    "meta.unit": "Unité",
    "meta.period": "Période",
    "meta.source": "Source",
    "meta.footnote": "Note",
    "meta.sourceTitle": "Titre original",
    "meta.statistic": "Statistique",
    "meta.database": "Base de données",
    "meta.code": "Code",
    "meta.stichtag": "Date de référence",
    "meta.copyright": "Copyright",
    "meta.info": "Méthodologie / notes",
    "meta.empty": "Aucune information disponible pour ce pays.",
    "meta.country.ch": "Suisse",
    "meta.country.de": "Allemagne",
    "meta.country.fr": "France",
    "download.title": "Téléchargement",
    "download.all": "Toutes les données",
    "download.filtered": "Données filtrées",
    "download.metaShort": "Métadonnées (courtes)",
    "download.metaLong": "Métadonnées (longues)",
    "download.api": "Export API :",
    "status.loading": "Chargement des données…",
  },
};

let lang = FIXED_LANG || (() => { try { return localStorage.getItem("statrhena_lang"); } catch (e) { return null; } })() || "de";
if (!I18N[lang]) lang = "de";

function t(key) { return (I18N[lang] && I18N[lang][key]) || I18N.de[key] || key; }
/** Display name for a technical column (from data_orig/statrhena_spaltennamen.xlsx). */
function colLabel(column, fallback) {
  const entry = PAYLOAD.labels[column];
  return (entry && (entry[lang] || entry.de)) || fallback || column;
}
function escapeHtml(s) {
  return String(s ?? "")
    .replaceAll("&", "&amp;").replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;").replaceAll('"', "&quot;").replaceAll("'", "&#39;");
}
function locale() { return lang === "fr" ? "fr-FR" : lang === "en" ? "en-GB" : "de-DE"; }
let NF_INT, NF_DEC, COLLATOR;
function initFormats() {
  NF_INT = new Intl.NumberFormat(locale(), { maximumFractionDigits: 0 });
  NF_DEC = new Intl.NumberFormat(locale(), { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  COLLATOR = new Intl.Collator(locale(), { numeric: true, sensitivity: "base" });
}

async function gunzipJson(b64) {
  const bin = atob(b64);
  const bytes = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
  const stream = new Blob([bytes]).stream().pipeThrough(new DecompressionStream("gzip"));
  return JSON.parse(await new Response(stream).text());
}

/* ---------- Data model ---------- */
let VARS, PLACES, GEOS, V, Y, G, P, X, D, N, META = {};
let DIMS = [];

/** Build a filter dimension: unique keys, a per-row code array and a display function. */
function makeDim(id, column, field, rowKey, label, order) {
  const keys = [];
  const index = new Map();
  const codes = new Int32Array(N);
  for (let i = 0; i < N; i++) {
    const k = rowKey(i);
    if (k === "" || k === null || k === undefined) { codes[i] = -1; continue; }
    let c = index.get(k);
    if (c === undefined) { c = keys.length; index.set(k, c); keys.push(k); }
    codes[i] = c;
  }
  return { id, column, field, keys, codes, label, order, selected: new Set(), mask: new Uint8Array(keys.length) };
}

function buildDims() {
  const varText = (prop) => (k) => {
    const v = VARS.find(x => x[prop].de === k);
    return (v && (v[prop][lang] || v[prop].de)) || k;
  };
  const appearance = (dim) => dim.keys.map((_, c) => c);
  const alpha = (dim) => dim.keys.map((_, c) => c).sort((a, b) => COLLATOR.compare(String(dim.label(dim.keys[a])), String(dim.label(dim.keys[b]))));
  const place = (n) => (i) => PLACES[P[i]][n];
  DIMS = [
    makeDim("topic", "Topic", "topic_de", i => VARS[V[i]].topic.de, varText("topic"), appearance),
    makeDim("subtopic", "Subtopic", "subtopic_de", i => VARS[V[i]].subtopic.de, varText("subtopic"), appearance),
    makeDim("title", "Title", "title_de", i => VARS[V[i]].title.de, varText("title"), appearance),
    makeDim("indicator", "Variable_Description", "variable_de", i => V[i], k => VARS[k].desc[lang] || VARS[k].desc.de, appearance),
    makeDim("geo", "geo", "geo", i => GEOS[G[i]], k => k,
      dim => dim.keys.map((_, c) => c).sort((a, b) => geoRank(dim.keys[b]) - geoRank(dim.keys[a]))),
    makeDim("land", "land", "land", place(1), k => k, alpha),
    makeDim("region", "region", "region", place(3), k => k, alpha),
    makeDim("kanton", "kanton_kreis_departement", "kanton_kreis_departement", place(5), k => k, alpha),
    makeDim("year", "jahr_annee", "jahr_annee", i => Y[i], k => String(k),
      dim => dim.keys.map((_, c) => c).sort((a, b) => dim.keys[a] - dim.keys[b])),
  ];
}
function dim(id) { return DIMS.find(d => d.id === id); }
function geoRank(name) {
  const r = GEO_HIERARCHY.indexOf(name);
  return r === -1 ? GEO_HIERARCHY.length : r;
}

/* ---------- Filter state ---------- */
const state = { search: "", searchMask: null, base: [], visible: null, autoGeo: null, sortCol: null, sortDir: 1 };

function updateSearchMask() {
  const q = state.search.trim().toLowerCase();
  if (!q) { state.searchMask = null; return; }
  state.searchMask = new Uint8Array(VARS.length);
  VARS.forEach((v, idx) => { state.searchMask[idx] = (v.desc[lang] || "").toLowerCase().includes(q) ? 1 : 0; });
}

/** True when row i passes every active filter and the search, ignoring dimension `skip`. */
function passes(i, skip) {
  if (state.searchMask && !state.searchMask[V[i]]) return false;
  for (const d of DIMS) {
    if (d === skip || !d.selected.size) continue;
    const c = d.codes[i];
    if (c < 0 || !d.mask[c]) return false;
  }
  return true;
}

function compute() {
  const base = [];
  for (let i = 0; i < N; i++) if (passes(i, null)) base.push(i);
  state.base = base;
  const geoDim = dim("geo");
  state.autoGeo = null;
  let visible = base;
  if (!geoDim.selected.size && base.length) {
    let best = Infinity, bestCode = -1;
    for (const i of base) {
      const r = geoRank(GEOS[G[i]]);
      if (r < best) { best = r; bestCode = G[i]; }
    }
    state.autoGeo = GEOS[bestCode];
    visible = base.filter(i => G[i] === bestCode);
  }
  state.visible = Int32Array.from(visible);
  sortVisible();
}

/** Option codes still reachable for a dimension, given all other filters. */
function availableCodes(d) {
  const seen = new Uint8Array(d.keys.length);
  for (let i = 0; i < N; i++) {
    const c = d.codes[i];
    if (c >= 0 && !seen[c] && passes(i, d)) seen[c] = 1;
  }
  return seen;
}

/* ---------- Filter UI ---------- */
function summaryText(d) {
  if (d.id === "geo" && !d.selected.size && state.autoGeo) return `${t("filter.auto")}: ${state.autoGeo}`;
  if (!d.selected.size) return t("filter.all");
  if (d.selected.size === 1) return d.label(d.keys[[...d.selected][0]]);
  return `${d.selected.size} ${t("filter.selected")}`;
}

function renderFilterShell() {
  const el = document.getElementById("filters");
  el.innerHTML = DIMS.map(d => `<div class="filter-field">
      <div class="flabel">${escapeHtml(colLabel(d.column))}</div>
      <div class="ms" data-dim="${d.id}">
        <button type="button" class="ms-btn" aria-haspopup="listbox" aria-expanded="false">
          <span class="ms-summary"></span><span class="ms-caret">▾</span>
        </button>
        <div class="ms-panel">
          ${d.keys.length > 6 ? `<div class="ms-find"><input type="search" placeholder="${escapeHtml(t("filter.find"))}" /></div>` : ""}
          <div class="ms-opts" role="listbox"></div>
        </div>
      </div>
    </div>`).join("");

  el.querySelectorAll(".ms").forEach(ms => {
    const d = dim(ms.dataset.dim);
    const btn = ms.querySelector(".ms-btn");
    btn.addEventListener("click", (e) => {
      e.stopPropagation();
      const wasOpen = ms.classList.contains("open");
      closePanels();
      if (!wasOpen) {
        ms.classList.add("open");
        btn.setAttribute("aria-expanded", "true");
        const find = ms.querySelector(".ms-find input");
        if (find) find.focus();
      }
    });
    ms.querySelector(".ms-panel").addEventListener("click", e => e.stopPropagation());
    const find = ms.querySelector(".ms-find input");
    if (find) find.addEventListener("input", () => {
      const q = find.value.trim().toLowerCase();
      let shown = 0;
      ms.querySelectorAll(".ms-opts label").forEach(l => {
        const hit = !q || l.textContent.toLowerCase().includes(q);
        l.hidden = !hit;
        if (hit) shown++;
      });
      const none = ms.querySelector(".ms-nohit");
      if (none) none.hidden = shown > 0;
    });
    ms.querySelector(".ms-opts").addEventListener("change", (e) => {
      const code = Number(e.target.value);
      if (e.target.checked) { d.selected.add(code); d.mask[code] = 1; }
      else { d.selected.delete(code); d.mask[code] = 0; }
      update(d);
    });
  });
}

function closePanels() {
  document.querySelectorAll(".ms.open").forEach(m => {
    m.classList.remove("open");
    m.querySelector(".ms-btn").setAttribute("aria-expanded", "false");
  });
}

function renderOptions(d) {
  const ms = document.querySelector(`.ms[data-dim="${d.id}"]`);
  const avail = availableCodes(d);
  const codes = d.order(d).filter(c => avail[c] || d.selected.has(c));
  const opts = ms.querySelector(".ms-opts");
  opts.innerHTML = codes.length
    ? codes.map(c => `<label><input type="checkbox" value="${c}" ${d.selected.has(c) ? "checked" : ""} /><span>${escapeHtml(d.label(d.keys[c]))}</span></label>`).join("")
      + `<div class="ms-empty ms-nohit" hidden>${escapeHtml(t("filter.noMatch"))}</div>`
    : `<div class="ms-empty">${escapeHtml(t("filter.noOptions"))}</div>`;
  const find = ms.querySelector(".ms-find input");
  if (find && find.value) find.dispatchEvent(new Event("input"));
}

function renderSummaries() {
  for (const d of DIMS) {
    const sum = document.querySelector(`.ms[data-dim="${d.id}"] .ms-summary`);
    sum.textContent = summaryText(d);
    sum.classList.toggle("has", d.selected.size > 0);
    sum.classList.toggle("auto", d.id === "geo" && !d.selected.size && !!state.autoGeo);
  }
}

/** Recompute after a change in dimension `changed` (null = everything). */
function update(changed) {
  compute();
  // A dimension's own options do not depend on its own selection.
  for (const d of DIMS) if (d !== changed) renderOptions(d);
  renderSummaries();
  renderTable(true);
  renderMetadata();
  renderDownloads();
}

/* ---------- Table (virtualised: all rows, one scroll area) ---------- */
const COLUMNS = [
  { col: "Topic", get: i => VARS[V[i]].topic[lang] },
  { col: "Subtopic", get: i => VARS[V[i]].subtopic[lang] },
  { col: "Title", get: i => VARS[V[i]].title[lang] },
  { col: "Variable", get: i => VARS[V[i]].variable[lang] },
  { col: "Variable_Description", get: i => VARS[V[i]].desc[lang] },
  { col: "geo", get: i => GEOS[G[i]] },
  { col: "land_code", get: i => PLACES[P[i]][0] },
  { col: "land", get: i => PLACES[P[i]][1] },
  { col: "region_code", get: i => PLACES[P[i]][2] },
  { col: "region", get: i => PLACES[P[i]][3] },
  { col: "kanton_code", get: i => PLACES[P[i]][4] },
  { col: "kanton_kreis_departement", get: i => PLACES[P[i]][5] },
  { col: "jahr_annee", get: i => String(Y[i]), num: true, sortKey: i => Y[i] },
  { col: "values", get: i => fmtValue(i), num: true, sortKey: i => X[i] ?? -Infinity },
];
function fmtValue(i) {
  const x = X[i];
  if (x === null || x === undefined) return "";
  return D[i] ? NF_DEC.format(x) : NF_INT.format(x);
}

function sortVisible() {
  if (state.sortCol === null) return;
  const c = COLUMNS[state.sortCol];
  const dir = state.sortDir;
  const arr = Array.from(state.visible);
  if (c.sortKey) arr.sort((a, b) => (c.sortKey(a) - c.sortKey(b)) * dir || a - b);
  else {
    const cache = new Map();
    const key = i => { let v = cache.get(i); if (v === undefined) { v = c.get(i) || ""; cache.set(i, v); } return v; };
    arr.sort((a, b) => COLLATOR.compare(key(a), key(b)) * dir || a - b);
  }
  state.visible = Int32Array.from(arr);
}

/** Fixed column widths from the longest value in the whole dataset, so scrolling never shifts columns. */
function renderTableHead() {
  const sample = new Map();
  const widths = COLUMNS.map((c, idx) => {
    let max = 4;
    if (idx <= 4) VARS.forEach((v, k) => {
      const prop = ["topic", "subtopic", "title", "variable", "desc"][idx];
      max = Math.max(max, (v[prop][lang] || "").length);
    });
    else if (c.col === "geo") GEOS.forEach(g => { max = Math.max(max, g.length); });
    else if (idx >= 6 && idx <= 11) PLACES.forEach(p => { max = Math.max(max, (p[idx - 6] || "").length); });
    else if (c.col === "values") max = 12;
    // Header labels are uppercase and wider per character; +2 leaves room for the sort arrow.
    max = Math.max(max, Math.ceil(colLabel(c.col).length * 1.05) + 2);
    const ch = Math.min(Math.max(max, 6), 46);
    return Math.round(ch * 7.4 + 24);
  });
  document.getElementById("cols").innerHTML = widths.map(w => `<col style="width:${w}px">`).join("");
  document.querySelector("table.data").style.width = widths.reduce((a, b) => a + b, 0) + "px";
  const thead = document.getElementById("thead");
  thead.innerHTML = `<tr>${COLUMNS.map((c, idx) => {
    const sorted = state.sortCol === idx;
    const arrow = !sorted ? "↕" : (state.sortDir === 1 ? "↑" : "↓");
    return `<th class="${c.num ? "num" : ""} ${sorted ? "sorted" : ""}" data-col="${idx}" title="${escapeHtml(c.col)}">${escapeHtml(colLabel(c.col))}<span class="sort-ind">${arrow}</span></th>`;
  }).join("")}</tr>`;
  thead.querySelectorAll("th").forEach(th => th.addEventListener("click", () => {
    const idx = Number(th.dataset.col);
    if (state.sortCol === idx) state.sortDir = -state.sortDir;
    else { state.sortCol = idx; state.sortDir = 1; }
    sortVisible();
    renderTableHead();
    renderTable(true);
  }));
}

let ROW_H = 36;
let rafPending = false;
function renderTable(resetScroll) {
  const wrap = document.getElementById("tableWrap");
  if (resetScroll) wrap.scrollTop = 0;
  const tbody = document.getElementById("tbody");
  const rows = state.visible;
  const total = rows.length;
  if (!total) {
    tbody.innerHTML = `<tr><td colspan="${COLUMNS.length}" class="table-none">${escapeHtml(t("table.none"))}</td></tr>`;
  } else {
    const headH = document.getElementById("thead").offsetHeight;
    const top = Math.max(0, wrap.scrollTop - headH);
    const start = Math.max(0, Math.floor(top / ROW_H) - 15);
    const end = Math.min(total, Math.ceil((top + wrap.clientHeight) / ROW_H) + 15);
    let html = `<tr class="spacer"><td colspan="${COLUMNS.length}" style="height:${start * ROW_H}px"></td></tr>`;
    for (let r = start; r < end; r++) {
      const i = rows[r];
      html += "<tr>" + COLUMNS.map(c => {
        const v = c.get(i) || "";
        return `<td${c.num ? ' class="num"' : ""} title="${escapeHtml(v)}">${escapeHtml(v)}</td>`;
      }).join("") + "</tr>";
    }
    html += `<tr class="spacer"><td colspan="${COLUMNS.length}" style="height:${(total - end) * ROW_H}px"></td></tr>`;
    tbody.innerHTML = html;
    const first = tbody.children[1];
    if (first && first.offsetHeight && Math.abs(first.offsetHeight - ROW_H) > 0.5) {
      ROW_H = first.offsetHeight;
      return renderTable(false);
    }
  }
  let info = `${NF_INT.format(total)} ${t("table.rows")}`;
  if (state.autoGeo) info += ` · ${colLabel("geo")}: ${state.autoGeo} (${t("table.geoAuto")})`;
  document.getElementById("tableInfo").textContent = info;
}

/* ---------- Metadata accordion ---------- */
let metaKey = "";
function renderMetadata(force) {
  const present = new Set();
  for (const i of state.base) present.add(V[i]);
  const ids = [...present].sort((a, b) => a - b);
  const key = lang + "|" + ids.join(",");
  if (!force && key === metaKey) return;
  metaKey = key;
  document.getElementById("metaCount").textContent = ids.length ? `${ids.length} ${t("meta.count")}` : "";
  const list = document.getElementById("metaList");
  if (!ids.length) { list.innerHTML = `<p class="meta-empty">${escapeHtml(t("meta.none"))}</p>`; return; }
  list.innerHTML = ids.map(k => {
    const v = VARS[k];
    const path = [v.topic[lang], v.subtopic[lang], v.title[lang]].filter(Boolean).join(" › ");
    return `<details class="meta-item" data-var="${k}">
      <summary>
        <span class="meta-head">
          <span class="meta-name">${escapeHtml(v.desc[lang] || v.desc.de)}</span>
          <span class="meta-path">${escapeHtml(path)}</span>
        </span>
        <span class="meta-toggle">${escapeHtml(t("meta.details"))} <span class="chev">▾</span></span>
      </summary>
      <div class="meta-body"></div>
    </details>`;
  }).join("");
  list.querySelectorAll(".meta-item").forEach(item => {
    item.addEventListener("toggle", () => {
      if (item.open && !item.dataset.filled) {
        item.querySelector(".meta-body").innerHTML = metaBody(Number(item.dataset.var));
        item.dataset.filled = "1";
        wireTabs(item);
      }
    });
  });
}

function kv(label, value) {
  return value ? `<div class="kv"><b>${escapeHtml(label)}</b><span>${escapeHtml(value)}</span></div>` : "";
}

function metaBody(k) {
  const v = VARS[k];
  const m = META[v.variable.de] || {};
  const period = [m.min, m.max].filter(Boolean).join("–");
  const head = kv(colLabel("Variable", "Code"), v.variable[lang])
    + kv(colLabel("Measure_Unit", t("meta.unit")), v.unit[lang])
    + kv(colLabel("Timespan", t("meta.period")), period)
    + kv(colLabel("Source", t("meta.source")), m.source)
    + kv(colLabel("Footnote", t("meta.footnote")), m.footnote && (m.footnote[lang] || m.footnote.de));
  const countries = m.countries || {};
  const tabs = META_COUNTRIES.map((c, i) => `<button type="button" class="meta-tab ${i === 0 ? "active" : ""}" role="tab" data-country="${c}" aria-selected="${i === 0}">
      <img src="${PAYLOAD.flags[c]}" alt="" /><span>${escapeHtml(t("meta.country." + c))}</span></button>`).join("");
  const panels = META_COUNTRIES.map((c, i) => `<div class="meta-tabpanel ${i === 0 ? "active" : ""}" role="tabpanel" data-country="${c}">${countryPanel(countries[c])}</div>`).join("");
  return `${head}<div class="meta-tabs"><div class="meta-tablist" role="tablist">${tabs}</div>${panels}</div>`;
}

function countryPanel(c) {
  if (!c) return `<p class="meta-empty">${escapeHtml(t("meta.empty"))}</p>`;
  const L = (field, key) => colLabel(`${field}_<LAND>`, t(key));
  const ts = (c.timespan || []).filter(Boolean).join("–");
  const html = kv(L("Title", "meta.sourceTitle"), c.title)
    + kv(L("Statistic", "meta.statistic"), c.statistic)
    + kv(L("Source", "meta.source"), c.source)
    + kv(L("Database", "meta.database"), c.database)
    + kv(L("Code", "meta.code"), c.code)
    + kv(L("Timespan", "meta.period"), ts)
    + kv(L("Stichtag", "meta.stichtag"), c.stichtag)
    + kv(L("Copyright", "meta.copyright"), c.copyright)
    + kv(L("Specific_Information", "meta.info"), c.info);
  return html || `<p class="meta-empty">${escapeHtml(t("meta.empty"))}</p>`;
}

function wireTabs(root) {
  const btns = root.querySelectorAll(".meta-tab");
  const panels = root.querySelectorAll(".meta-tabpanel");
  btns.forEach(btn => btn.addEventListener("click", () => {
    const c = btn.dataset.country;
    btns.forEach(b => { const on = b.dataset.country === c; b.classList.toggle("active", on); b.setAttribute("aria-selected", on); });
    panels.forEach(p => p.classList.toggle("active", p.dataset.country === c));
  }));
}

/* ---------- Downloads ---------- */
function odsLiteral(value) { return `"${String(value).replaceAll("\\", "\\\\").replaceAll('"', '\\"')}"`; }

/** Export URL for the current filters: one value -> refine, several -> where IN, search -> where search(). */
function buildExportUrl(id, format, withFilters) {
  const params = new URLSearchParams();
  params.set("timezone", "Europe/Zurich");
  if (withFilters) {
    const where = [];
    for (const d of DIMS) {
      let values = [...d.selected].map(c => d.id === "indicator" ? VARS[d.keys[c]].variable.de : d.keys[c]);
      if (d.id === "geo" && !values.length && state.autoGeo) values = [state.autoGeo];
      if (!values.length) continue;
      if (values.length === 1) params.append("refine", `${d.field}:${values[0]}`);
      else where.push(`${d.field} in (${values.map(odsLiteral).join(", ")})`);
    }
    const q = state.search.trim();
    if (q) where.push(`search(variable_description_${lang}, ${odsLiteral(q)})`);
    if (where.length) params.set("where", where.join(" and "));
  }
  return `https://data.bs.ch/api/explore/v2.1/catalog/datasets/${id}/exports/${format}/?${params.toString()}`;
}

function exportsHtml(id, withFilters) {
  const formats = [["csv", "CSV"], ["json", "JSON"], ["xlsx", "XLSX"], ["parquet", "Parquet"]];
  const links = formats.map(([fmt, label]) =>
    `<a href="${escapeHtml(buildExportUrl(id, fmt, withFilters))}" target="_blank" rel="noopener">↓ ${label}</a>`).join("");
  return links + `<div class="api-note">${escapeHtml(t("download.api"))} <code>${escapeHtml(buildExportUrl(id, "csv", withFilters))}</code></div>`;
}

function renderDownloads() {
  document.getElementById("exportsAll").innerHTML = exportsHtml(DATA_ID, false);
  document.getElementById("exportsFiltered").innerHTML = exportsHtml(DATA_ID, true);
  document.getElementById("exportsMetaShort").innerHTML = exportsHtml(META_SHORT_ID, false);
  document.getElementById("exportsMetaLong").innerHTML = exportsHtml(META_LONG_ID, false);
}

/* ---------- Data tree lightbox ---------- */
/* Zoom/pan viewer: scale is relative to the image's natural size; "fit" shows the whole tree. */
const tree = { img: null, scale: 1, fit: 1 };
const ZOOM_STEP = 1.25;

function treeMax() { return Math.max(1, tree.fit); }

function computeFit() {
  const view = document.getElementById("lbView");
  const img = tree.img;
  return Math.min((view.clientWidth - 24) / img.naturalWidth, (view.clientHeight - 24) / img.naturalHeight);
}

/** Set the zoom level, keeping the image point under view coordinates (cx, cy) in place. */
function setZoom(next, cx, cy) {
  const img = tree.img;
  if (!img) return;
  const view = document.getElementById("lbView");
  next = Math.min(Math.max(next, tree.fit), treeMax());
  if (cx === undefined) { cx = view.clientWidth / 2; cy = view.clientHeight / 2; }
  const ix = (view.scrollLeft + cx - img.offsetLeft) / tree.scale;
  const iy = (view.scrollTop + cy - img.offsetTop) / tree.scale;
  tree.scale = next;
  const w = img.naturalWidth * next;
  const h = img.naturalHeight * next;
  img.style.width = `${w}px`;
  img.style.height = `${h}px`;
  // Centre the image while it is smaller than the viewer.
  img.style.marginTop = `${Math.max(0, (view.clientHeight - h) / 2)}px`;
  view.scrollLeft = ix * next + img.offsetLeft - cx;
  view.scrollTop = iy * next + img.offsetTop - cy;
  document.getElementById("lbZoom").textContent = `${Math.round((next / tree.fit) * 100)}%`;
  document.getElementById("lbOut").disabled = next <= tree.fit + 1e-6;
  document.getElementById("lbIn").disabled = next >= treeMax() - 1e-6;
}

function fitTree() {
  if (!tree.img) return;
  tree.fit = computeFit();
  setZoom(tree.fit);
}

function initLightbox() {
  const dlg = document.getElementById("lightbox");
  const view = document.getElementById("lbView");
  const tools = [document.getElementById("lbIn"), document.getElementById("lbOut"), document.getElementById("lbFit")];

  document.querySelectorAll(".tile").forEach(tile => tile.addEventListener("click", () => {
    const topic = tile.dataset.tree;
    const imgs = PAYLOAD.trees[topic] || {};
    const src = imgs[lang] || imgs.de;
    const title = `${t("tree.title")}: ${t("theme." + topic)}`;
    document.getElementById("lbTitle").textContent = title;
    tree.img = null;
    view.innerHTML = src
      ? `<img src="${src}" alt="${escapeHtml(title)}" draggable="false" />`
      : `<p class="lb-missing">${escapeHtml(t("tree.missing"))}</p>`;
    tools.forEach(b => { b.disabled = !src; });
    document.getElementById("lbZoom").textContent = src ? "…" : "";
    dlg.showModal();
    const img = view.querySelector("img");
    if (img) img.decode().catch(() => {}).then(() => { tree.img = img; tree.scale = 1; fitTree(); });
  }));

  document.getElementById("lbIn").addEventListener("click", () => setZoom(tree.scale * ZOOM_STEP));
  document.getElementById("lbOut").addEventListener("click", () => setZoom(tree.scale / ZOOM_STEP));
  document.getElementById("lbFit").addEventListener("click", fitTree);
  document.getElementById("lbClose").addEventListener("click", () => dlg.close());
  dlg.addEventListener("click", (e) => { if (e.target === dlg) dlg.close(); });
  dlg.addEventListener("close", () => { tree.img = null; view.innerHTML = ""; });
  dlg.addEventListener("keydown", (e) => {
    if (!tree.img || e.target.closest("input")) return;
    if (e.key === "+" || e.key === "=") { e.preventDefault(); setZoom(tree.scale * ZOOM_STEP); }
    else if (e.key === "-") { e.preventDefault(); setZoom(tree.scale / ZOOM_STEP); }
    else if (e.key === "0") { e.preventDefault(); fitTree(); }
  });

  // Ctrl/⌘ + wheel (and trackpad pinch) zooms around the cursor; plain wheel scrolls.
  view.addEventListener("wheel", (e) => {
    if (!tree.img || !(e.ctrlKey || e.metaKey)) return;
    e.preventDefault();
    const rect = view.getBoundingClientRect();
    setZoom(tree.scale * Math.exp(-e.deltaY * 0.002), e.clientX - rect.left, e.clientY - rect.top);
  }, { passive: false });

  // Double-click zooms in around the cursor (Shift: zoom out); at maximum zoom it fits again.
  view.addEventListener("dblclick", (e) => {
    if (!tree.img) return;
    e.preventDefault();
    const rect = view.getBoundingClientRect();
    const cx = e.clientX - rect.left;
    const cy = e.clientY - rect.top;
    if (e.shiftKey) setZoom(tree.scale / (ZOOM_STEP * ZOOM_STEP), cx, cy);
    else if (tree.scale >= treeMax() - 1e-6) fitTree();
    else setZoom(tree.scale * ZOOM_STEP * ZOOM_STEP, cx, cy);
  });

  // Click and drag to pan (mouse/pen); touch keeps native scrolling.
  let drag = null;
  view.addEventListener("pointerdown", (e) => {
    if (!tree.img || e.button !== 0 || e.pointerType === "touch") return;
    drag = { x: e.clientX, y: e.clientY, left: view.scrollLeft, top: view.scrollTop, id: e.pointerId };
    view.setPointerCapture(e.pointerId);
    view.classList.add("dragging");
  });
  view.addEventListener("pointermove", (e) => {
    if (!drag || e.pointerId !== drag.id) return;
    view.scrollLeft = drag.left - (e.clientX - drag.x);
    view.scrollTop = drag.top - (e.clientY - drag.y);
  });
  const endDrag = (e) => {
    if (!drag || e.pointerId !== drag.id) return;
    drag = null;
    view.classList.remove("dragging");
  };
  view.addEventListener("pointerup", endDrag);
  view.addEventListener("pointercancel", endDrag);

  window.addEventListener("resize", () => {
    if (!tree.img) return;
    const wasFit = Math.abs(tree.scale - tree.fit) < 1e-6;
    tree.fit = computeFit();
    setZoom(wasFit ? tree.fit : tree.scale);
  });
}

/* ---------- Language ---------- */
function applyStaticI18n() {
  document.documentElement.lang = lang;
  document.title = t("page.title");
  document.querySelectorAll("[data-i18n]").forEach(el => { el.textContent = t(el.getAttribute("data-i18n")); });
  document.querySelectorAll("[data-i18n-title]").forEach(el => {
    const text = t(el.getAttribute("data-i18n-title"));
    el.title = text;
    el.setAttribute("aria-label", text);
  });
  document.getElementById("searchLabel").textContent = colLabel("Variable_Description");
  document.getElementById("search").placeholder = t("filter.searchPlaceholder");
  if (SHOW_LANG) {
    document.getElementById("langFlag").src = PAYLOAD.flags[lang];
    document.getElementById("langLabel").textContent = LANGS.find(l => l.id === lang).label;
  }
}

function setLang(next) {
  lang = next;
  try { localStorage.setItem("statrhena_lang", lang); } catch (e) { /* ignore */ }
  initFormats();
  applyStaticI18n();
  renderLangMenu();
  if (!N) return;
  updateSearchMask();
  renderFilterShell();
  renderTableHead();
  update(null);
}

function renderLangMenu() {
  if (!SHOW_LANG) return;
  const menu = document.getElementById("langMenu");
  menu.innerHTML = LANGS.map(l => `<button type="button" role="option" data-lang="${l.id}" class="${l.id === lang ? "active" : ""}">
      <img src="${PAYLOAD.flags[l.id]}" alt="" /> ${escapeHtml(l.label)}</button>`).join("");
  menu.querySelectorAll("button").forEach(btn => btn.addEventListener("click", () => {
    document.getElementById("langSwitch").classList.remove("open");
    document.getElementById("langBtn").setAttribute("aria-expanded", "false");
    setLang(btn.dataset.lang);
  }));
}

function initLangSwitcher() {
  if (!SHOW_LANG) return;
  const langBtn = document.getElementById("langBtn");
  const langSwitch = document.getElementById("langSwitch");
  langBtn.addEventListener("click", (e) => {
    e.stopPropagation();
    const open = langSwitch.classList.toggle("open");
    langBtn.setAttribute("aria-expanded", open ? "true" : "false");
  });
  document.addEventListener("click", (e) => {
    if (!langSwitch.contains(e.target)) {
      langSwitch.classList.remove("open");
      langBtn.setAttribute("aria-expanded", "false");
    }
  });
}

/* ---------- Boot ---------- */
async function boot() {
  initFormats();
  applyStaticI18n();
  renderLangMenu();
  initLangSwitcher();
  initLightbox();

  const status = document.getElementById("status");
  status.hidden = false;
  status.className = "status";
  status.innerHTML = `<span class="loading"><span class="spinner"></span> ${escapeHtml(t("status.loading"))}</span>`;
  try {
    const [data, meta] = await Promise.all([gunzipJson(PAYLOAD.data_gz), gunzipJson(PAYLOAD.metadata_gz)]);
    META = meta;
    VARS = data.vars; PLACES = data.places; GEOS = data.geos;
    ({ v: V, y: Y, g: G, p: P, x: X, d: D } = data.rows);
    N = V.length;
    buildDims();
    status.hidden = true;
  } catch (err) {
    status.className = "status error";
    status.textContent = String(err.message || err);
    return;
  }

  renderFilterShell();
  renderTableHead();
  update(null);

  document.addEventListener("click", closePanels);
  document.addEventListener("keydown", e => { if (e.key === "Escape") closePanels(); });
  let searchTimer;
  document.getElementById("search").addEventListener("input", (e) => {
    clearTimeout(searchTimer);
    searchTimer = setTimeout(() => { state.search = e.target.value; updateSearchMask(); update(null); }, 200);
  });
  document.getElementById("reset").addEventListener("click", () => {
    for (const d of DIMS) { d.selected.clear(); d.mask.fill(0); }
    state.search = "";
    document.getElementById("search").value = "";
    updateSearchMask();
    update(null);
  });
  document.getElementById("tableWrap").addEventListener("scroll", () => {
    if (rafPending) return;
    rafPending = true;
    requestAnimationFrame(() => { rafPending = false; renderTable(false); });
  }, { passive: true });
}

boot().catch(console.error);
</script>
</body>
</html>
"""


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info("Executing %s...", __file__)
    main()
    logging.info("Job successful.")
