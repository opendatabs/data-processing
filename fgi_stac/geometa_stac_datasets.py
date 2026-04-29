"""Resolve Dataspot dataset UUIDs per STAC collection from Basel Geometa HTML previews.

The public HTML at
``/geometa/v1/metadata_details/dataset/preview/html/<COLLECTION>`` lists each
sub-dataset with ``<a href="#<uuid>"><li>Layer name</li></a>`` and/or
``<h3 id="<uuid>">Title</h3>``. This matches how portal URLs use fragments
(e.g. ``.../html/AFBA#a396da69-...``).
"""

from __future__ import annotations

import logging
import re
import httpx

GEOMETA_HTML_URL = "https://api.geo.bs.ch/geometa/v1/metadata_details/dataset/preview/html/{collection_id}"

# Sidebar / "Auf dieser Seite" links
_NAV_ANCHOR_RE = re.compile(
    r'<a\s+href="#([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})"\s*>\s*<li>([^<]*)</li>',
    re.IGNORECASE | re.DOTALL,
)
# Section headings tied to a dataset id
_H3_ID_RE = re.compile(
    r'<h3\s+id="([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})"\s*>\s*([^<]*)',
    re.IGNORECASE | re.DOTALL,
)
# Any fragment or id attribute holding a dataset UUID (fallback)
_REF_UUID_RE = re.compile(
    r'(?:href="#|id=")([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})"',
    re.IGNORECASE,
)


def fetch_geometa_collection_html(
    collection_id: str,
    *,
    timeout: float = 90.0,
    retries: int = 2,
) -> str | None:
    cid = collection_id.strip()
    if not cid:
        return None
    url = GEOMETA_HTML_URL.format(collection_id=cid)
    last_exc: Exception | None = None
    for attempt in range(retries + 1):
        try:
            with httpx.Client(timeout=timeout, follow_redirects=True) as client:
                response = client.get(url)
                response.raise_for_status()
                return response.text
        except Exception as exc:
            last_exc = exc
            if attempt < retries:
                logging.info(
                    "Geometa retry %s/%s for collection_id=%s: %s",
                    attempt + 1,
                    retries,
                    cid,
                    exc,
                )
    logging.warning(
        "Geometa HTML fetch failed for collection_id=%s (%s): %s",
        cid,
        url,
        last_exc,
    )
    return None


def parse_geometa_dataset_instances(html: str, collection_title: str) -> list[dict[str, str]]:
    """Return ordered ``dataspot_uuid`` + ``geo_dataset`` labels for one collection page."""
    ordered: list[dict[str, str]] = []
    seen: set[str] = set()
    fallback_title = (collection_title or "").strip() or "Datensatz"

    def add(uuid: str, label: str) -> None:
        u = uuid.lower()
        if u in seen:
            return
        seen.add(u)
        text = (label or "").strip() or fallback_title
        ordered.append({"dataspot_uuid": u, "geo_dataset": text})

    if html:
        for m in _NAV_ANCHOR_RE.finditer(html):
            add(m.group(1), m.group(2))
        for m in _H3_ID_RE.finditer(html):
            add(m.group(1), m.group(2))
        for m in _REF_UUID_RE.finditer(html):
            add(m.group(1), fallback_title)

    return ordered


def discover_instances_for_collection(
    collection_id: str,
    collection_title: str,
    *,
    html: str | None = None,
) -> list[dict[str, str]]:
    """Fetch (unless ``html`` is provided) and parse dataset instances for one STAC collection."""
    payload = html if html is not None else fetch_geometa_collection_html(collection_id)
    if not payload:
        return []
    return parse_geometa_dataset_instances(payload, collection_title)
