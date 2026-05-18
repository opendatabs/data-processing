"""Single entrypoint for catalog refresh + publish."""

from __future__ import annotations

import asyncio
import argparse
import html
import io
import json
import logging
import os
import re
import time
import urllib.parse
import zipfile
from pathlib import Path
from typing import Any

import geopandas as gpd
import httpx
import yaml
from dotenv import load_dotenv

from dataspot_auth import DataspotAuth
from http_retry import await_with_http_retry, with_http_retry

load_dotenv()

STAC_V1_BASE_URL = "https://api.geo.bs.ch/stac/v1"
STAC_COLLECTIONS_URL = f"{STAC_V1_BASE_URL}/collections"
GEOMETA_HTML_URL = "https://api.geo.bs.ch/geometa/v1/metadata_details/dataset/published/html/{collection_id}"
GEOMETA_JSON_URL = "https://api.geo.bs.ch/geometa/v1/metadata_details/dataset/published/json/{collection_id}"


def _api_geo_bs_headers() -> dict[str, str]:
    """Headers required by api.geo.bs.ch endpoints that need authentication."""
    api_key = os.getenv("API_KEY_MAPBS", "")
    return {"apikey": api_key} if api_key else {}


DATASPOT_COMPOSITIONS_URL = "https://bs.dataspot.io/rest/prod/datasets/{dataset_id}/compositions"
DATASPOT_DATASET_URL = "https://bs.dataspot.io/rest/prod/datasets/{dataset_id}"
DATASPOT_ATTRIBUTE_URL = "https://bs.dataspot.io/rest/prod/attributes/{attribute_id}"
DATASPOT_RANGE_ASSET_URL = "https://bs.dataspot.io/rest/prod/assets/{asset_id}"
DATA_DIR = Path("data")
CATALOG_FILE = DATA_DIR / "publish_catalog.yaml"
DATASETS_DIR = DATA_DIR / "datasets"
SCHEMA_FILES_DIR = DATA_DIR / "schema_files"
HTTP_TIMEOUT = httpx.Timeout(60.0, connect=20.0)
HTTP_TIMEOUT_LONG = httpx.Timeout(180.0, connect=30.0)
HTTP_LIMITS = httpx.Limits(max_connections=20, max_keepalive_connections=10)
_DATATYPE_MAP = {
    "code": "text",
    "datum": "date",
    "dezimalzahl": "double",
    "formatierter text": "text",
    "ganzzahl": "int",
    "geometrie (punkt)": "geo_point_2d",
    "geometrie (linie)": "geo_shape",
    "geometrie (fläche)": "geo_shape",
    "geometrie (flaeche)": "geo_shape",
    "geometrie": "geo_shape",
    "ja/nein": "text",
    "text": "text",
    "uhrzeit": "text",
    "url": "text",
    "zeitpunkt": "datetime",
}


def _clean(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _schema_export_value(value: Any, *, default: bool) -> bool:
    """Interpret YAML ``export``; preserved across ETL while Dataspot refreshes other fields."""
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = _clean(value).lower()
    if text in {"", "none"}:
        return default
    return text not in {"false", "0", "no", "off"}


def _custom_block_from_preserved_row(item: dict[str, Any]) -> dict[str, Any]:
    """Build ``custom`` from a saved schema row (the only top-level keys ETL preserves)."""
    old_custom = item.get("custom")
    if isinstance(old_custom, dict):
        return {
            "technical_name": _clean(old_custom.get("technical_name")),
            "name": _clean(old_custom.get("name")),
            "description": _clean(old_custom.get("description")),
            "datentyp": _clean(old_custom.get("datentyp")),
            "mehrwertigkeit": _clean(old_custom.get("mehrwertigkeit")),
        }
    return {
        "technical_name": _clean(old_custom),
        "name": "",
        "description": "",
        "datentyp": "",
        "mehrwertigkeit": "",
    }


@with_http_retry
def _http_get_bytes(
    url: str,
    *,
    headers: dict[str, str] | None = None,
    timeout: httpx.Timeout | None = None,
) -> bytes:
    """Fetch response body bytes with retries on transient failures."""
    with httpx.Client(timeout=timeout or HTTP_TIMEOUT, limits=HTTP_LIMITS) as client:
        response = client.get(url, headers=headers)
    response.raise_for_status()
    return response.content


@with_http_retry
def _http_get_json(
    url: str,
    *,
    headers: dict[str, str] | None = None,
    timeout: httpx.Timeout | None = None,
    allow_404: bool = False,
) -> dict[str, Any] | None:
    """Fetch one JSON payload with explicit timeout and status handling."""
    with httpx.Client(timeout=timeout or HTTP_TIMEOUT, limits=HTTP_LIMITS) as client:
        response = client.get(url, headers=headers)
    if response.status_code in {404, 410} and allow_404:
        return None
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        return None
    return payload


async def _http_get_json_async(
    client: httpx.AsyncClient,
    url: str,
    *,
    headers: dict[str, str] | None = None,
    allow_404: bool = False,
) -> dict[str, Any] | None:
    """Fetch one JSON payload asynchronously with consistent handling."""

    async def _fetch() -> dict[str, Any] | None:
        response = await client.get(url, headers=headers)
        if response.status_code in {404, 410} and allow_404:
            return None
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            return None
        return payload

    return await await_with_http_retry(_fetch)


def _fetch_stac_collections() -> list[dict[str, Any]]:
    headers = _api_geo_bs_headers()
    payload = _http_get_json(STAC_COLLECTIONS_URL, headers=headers, timeout=HTTP_TIMEOUT)
    if payload is None:
        raise ValueError("Invalid STAC response: expected JSON object")
    collections = payload.get("collections", [])
    if not isinstance(collections, list):
        raise ValueError("Invalid STAC response: collections missing")
    return collections


def _extract_links(collection: dict[str, Any]) -> dict[str, str]:
    links: dict[str, str] = {}
    for item in collection.get("links", []) or []:
        if not isinstance(item, dict):
            continue
        rel = _clean(item.get("rel"))
        href = _clean(item.get("href"))
        if rel and href:
            links[rel] = href
    return links


def _extract_orgs(providers: list[dict[str, Any]] | None) -> tuple[str, str]:
    producer: list[str] = []
    publisher: list[str] = []
    for provider in providers or []:
        if not isinstance(provider, dict):
            continue
        roles = provider.get("roles", [])
        if not isinstance(roles, list):
            roles = []
        name = _clean(provider.get("name"))
        if not name:
            continue
        if "producer" in roles:
            producer.append(name)
        if "host" in roles or "licensor" in roles:
            publisher.append(name)
    return "; ".join(producer), "; ".join(publisher)


def _fetch_geometa_collection_json(collection_id: str) -> dict[str, Any]:
    """Read the Geometa JSON document for a STAC collection.

    Returns the parsed document. The collection-level JSON exposes
    a top-level ``datasets`` list, each entry carrying ``dataset_uuid`` and
    ``label`` (already filtered to real dataset objects, no Wertebereiche).
    """
    url = GEOMETA_JSON_URL.format(collection_id=collection_id)
    headers = _api_geo_bs_headers()
    payload = _http_get_json(url, headers=headers, timeout=HTTP_TIMEOUT)
    if payload is None:
        raise ValueError(f"Geometa JSON for collection {collection_id!r} returned a non-object payload")
    return payload


def _discover_instances_for_collection(collection_id: str, collection_title: str) -> list[dict[str, str]]:
    """Return ``[{dataspot_uuid, geo_dataset}]`` for every dataset instance in a collection.

    Reads the Geometa JSON document (the structured twin of the HTML preview).
    Fails loudly when STAC announces a collection but Geometa lists zero datasets,
    so that silent drops from upstream API regressions become visible.
    """
    document = _fetch_geometa_collection_json(collection_id)
    datasets = document.get("datasets", [])
    if not isinstance(datasets, list):
        raise ValueError(f"Geometa JSON for {collection_id!r}: 'datasets' is not a list")
    if not datasets:
        raise ValueError(
            f"Geometa JSON for {collection_id!r} returned 0 datasets but STAC lists the collection. "
            "Refusing to silently drop rows; check the upstream Geometa import."
        )
    seen: set[str] = set()
    instances: list[dict[str, str]] = []
    for entry in datasets:
        if not isinstance(entry, dict):
            continue
        uuid = _clean(entry.get("dataset_uuid")).lower()
        if not uuid or uuid in seen:
            continue
        seen.add(uuid)
        label = _clean(entry.get("label")) or collection_title or "Datensatz"
        instances.append({"dataspot_uuid": uuid, "geo_dataset": label})
    return instances


def _dataspot_get(auth: DataspotAuth, url: str, *, allow_404: bool = False) -> dict[str, Any] | None:
    return _http_get_json(url, headers=auth.get_headers(), allow_404=allow_404)


def _normalize_optional_date(value: Any) -> str:
    text = _clean(value)
    if not text:
        return ""
    if text.isdigit():
        timestamp = int(text)
        if timestamp > 10_000_000_000:
            timestamp = timestamp // 1000
        try:
            return time.strftime("%Y-%m-%d", time.gmtime(timestamp))
        except Exception:
            return text
    return text


def _extract_string_list(value: Any) -> list[str]:
    def _is_placeholder(text: str) -> bool:
        lowered = text.strip().lower()
        return lowered in {"[]", "[ ]", "null", "none", "nan"}

    if isinstance(value, list):
        items: list[str] = []
        for item in value:
            if isinstance(item, dict):
                label = _clean(item.get("label") or item.get("title") or item.get("name"))
                if label and not _is_placeholder(label):
                    items.append(label)
            else:
                text = _clean(item)
                if text and not _is_placeholder(text):
                    items.append(text)
        return items
    text = _clean(value)
    if not text or _is_placeholder(text):
        return []
    return [text]


def _dataspot_metadata(auth: DataspotAuth, dataspot_dataset_id: str) -> dict[str, Any]:
    payload = _dataspot_get(auth, DATASPOT_DATASET_URL.format(dataset_id=dataspot_dataset_id), allow_404=True) or {}
    if not isinstance(payload, dict):
        payload = {}
    custom = payload.get("customProperties", {})
    if not isinstance(custom, dict):
        custom = {}
    keywords = _extract_string_list(payload.get("tags"))
    publisher_path = _clean(payload.get("producerOrganization") or payload.get("publishingOrganization") or payload.get("publisher"))
    return {
        "object_type": _clean(payload.get("_type")),
        "title": _clean(payload.get("label") or payload.get("title")),
        "description": _clean(payload.get("description")),
        "keyword_values": keywords,
        "publisher_path": publisher_path,
        "created": _normalize_optional_date(custom.get("creationDate")),
        "modified": _normalize_optional_date(payload.get("lastUpdate") or payload.get("modified")),
        "issued": _normalize_optional_date(custom.get("publicationDate")),
        "accrualperiodicity": _clean(payload.get("accrualPeriodicity")),
    }


_HTML_MARKER_RE = re.compile(
    r"</[a-zA-Z][\w-]*\s*>"          # closing tag, e.g. </p>
    r"|<[a-zA-Z][\w-]*[^<>]*?/\s*>"  # self-closing tag, e.g. <br/>
    r"|<[a-zA-Z][\w-]*\s+[^<>]*?>"   # opening tag with attributes, e.g. <a href=...>
)


def _description_to_html(value: Any) -> str:
    """Convert a Dataspot description to HUWISE-ready HTML.

    Uses a stricter HTML-detection regex than the previous heuristic so
    expressions like ``1 < 2`` (which the old ``<[a-zA-Z]`` test mis-
    classified as HTML) are correctly escaped. The regex requires either a
    closing tag, a self-closing tag, or an opening tag with attributes –
    none of which occur in plain mathematical text.
    """
    text = _clean(value)
    if not text:
        return ""
    if _HTML_MARKER_RE.search(text):
        return text
    paragraphs = [part.strip() for part in re.split(r"\n\s*\n", text) if part.strip()]
    if not paragraphs:
        return html.escape(text).replace("\n", "<br>")
    return "\n".join(f"<p>{html.escape(part).replace('\n', '<br>')}</p>" for part in paragraphs)


def _third_path_segment(path_value: Any) -> str:
    path = _clean(path_value)
    parts = [part.strip() for part in path.split("/") if part.strip()]
    return parts[2] if len(parts) > 2 else ""


def _dataspot_schema(auth: DataspotAuth, dataset_id: str, old_schema: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    old_by_name = {}
    if isinstance(old_schema, list):
        old_by_name = {_clean(item.get("technical_name")): item for item in old_schema if isinstance(item, dict)}

    compositions_data = _dataspot_get(auth, DATASPOT_COMPOSITIONS_URL.format(dataset_id=dataset_id), allow_404=True) or {}
    compositions = compositions_data.get("_embedded", {}).get("compositions", [])
    if not isinstance(compositions, list):
        compositions = []

    def _map_datatype(value: str) -> str:
        normalized = _clean(value).lower()
        if not normalized:
            return "text"
        if "geometrie" in normalized and "punkt" in normalized:
            return "geo_point_2d"
        if "geometrie" in normalized:
            return "geo_shape"
        for key, mapped in _DATATYPE_MAP.items():
            if key in normalized:
                return mapped
        return "text"

    def _old_value(old_item: dict[str, Any], *keys: str) -> Any:
        for key in keys:
            if key in old_item:
                return old_item[key]
        return None

    def _multivalued_separator_from_attribute(attribute: dict[str, Any]) -> str:
        cardinality = _clean(attribute.get("cardinality") or attribute.get("hasCardinality")).lower()
        if cardinality in {"n", "*", "0..n", "1..n", "many", "multiple"}:
            return ";"
        return ""

    def _compose_row(
        *,
        technical_name: str,
        field_name: str,
        description: str,
        datatype_label: str,
        multivalued_separator: str,
    ) -> dict[str, Any]:
        old = old_by_name.get(technical_name, {})
        custom_payload = _custom_block_from_preserved_row(old)
        row: dict[str, Any] = {
            "technical_name": technical_name,
            "name": field_name or technical_name,
            "description": description,
            "mehrwertigkeit": multivalued_separator,
            "datentyp": _map_datatype(datatype_label),
            "export": _schema_export_value(
                _old_value(old, "export"), default=technical_name.lower() != "gdh_fid"
            ),
            "custom": custom_payload,
        }
        if not _clean(custom_payload.get("technical_name")):
            row["custom"]["technical_name"] = _normalize_huwise_field_name(technical_name)
        return row

    async def _build_rows() -> list[dict[str, Any]]:
        headers = auth.get_headers()
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT, limits=HTTP_LIMITS) as client:
            composition_items: list[tuple[dict[str, Any], str]] = []
            attribute_tasks: list[asyncio.Future] = []
            for composition in compositions:
                if not isinstance(composition, dict):
                    continue
                attribute_id = _clean(composition.get("composedOf"))
                if not attribute_id:
                    continue
                composition_items.append((composition, attribute_id))
                attribute_tasks.append(
                    _http_get_json_async(
                        client,
                        DATASPOT_ATTRIBUTE_URL.format(attribute_id=attribute_id),
                        headers=headers,
                        allow_404=True,
                    )
                )

            attributes = await asyncio.gather(*attribute_tasks) if attribute_tasks else []
            datatype_tasks: list[asyncio.Future] = []
            datatype_ids: list[str] = []
            for attribute_payload in attributes:
                has_range_id = _clean((attribute_payload or {}).get("hasRange"))
                if has_range_id:
                    datatype_ids.append(has_range_id)
                    datatype_tasks.append(
                        _http_get_json_async(
                            client,
                            DATASPOT_RANGE_ASSET_URL.format(asset_id=has_range_id),
                            headers=headers,
                            allow_404=True,
                        )
                    )
            datatype_assets = await asyncio.gather(*datatype_tasks) if datatype_tasks else []
            datatype_by_id = {datatype_ids[idx]: datatype_assets[idx] for idx in range(len(datatype_ids))}

        rows_local: list[dict[str, Any]] = []
        for idx, (composition, _) in enumerate(composition_items):
            attribute_payload = attributes[idx] if idx < len(attributes) else None
            if not attribute_payload:
                continue
            has_range_id = _clean(attribute_payload.get("hasRange"))
            datatype_payload = datatype_by_id.get(has_range_id)
            datatype_label = _clean((datatype_payload or {}).get("label")) or _clean((datatype_payload or {}).get("title"))
            technical_name = _clean(composition.get("title")) or _clean(composition.get("label"))
            if not technical_name:
                continue
            if "geometr" in technical_name.lower():
                technical_name = "geometry"
            field_name = _clean(composition.get("label")) or technical_name
            description = _clean(attribute_payload.get("description")) or _clean(composition.get("description"))
            multivalued_separator = _multivalued_separator_from_attribute(attribute_payload)
            rows_local.append(
                _compose_row(
                    technical_name=technical_name,
                    field_name=field_name,
                    description=description,
                    datatype_label=datatype_label,
                    multivalued_separator=multivalued_separator,
                )
            )
        return rows_local

    rows = asyncio.run(_build_rows())
    return rows


def _normalize_name(value: str) -> str:
    text = _clean(value).lower()
    replacements = {
        "ä": "ae",
        "ö": "oe",
        "ü": "ue",
        "ß": "ss",
    }
    for src, dst in replacements.items():
        text = text.replace(src, dst)
    return re.sub(r"[^a-z0-9]", "", text)


def _normalize_geo_dataset_match_keys(geo_dataset: str) -> list[str]:
    """Normalized forms of geo_dataset for matching local GeoJSON stems.

    Filenames often drop the word *und* between title parts (e.g. ``Tagesheime_Kitas``)
    while STAC/geo metadata uses the full phrase *Tagesheime und Kitas*. Catalog
    labels also sometimes carry a parenthetical municipality disambiguator
    (e.g. ``Überlagernde Festlegung (Basel)``) that the STAC ZIP entries omit,
    so a parenthetical-stripped variant is emitted as a fallback key.
    """
    keys: list[str] = []
    seen: set[str] = set()
    base = _normalize_name(geo_dataset)
    if base and base not in seen:
        seen.add(base)
        keys.append(base)
    text = _clean(geo_dataset)
    if text and re.search(r"\bund\b", text, re.IGNORECASE):
        parts = re.split(r"\s+und\s+", text, flags=re.IGNORECASE)
        merged = "".join(_normalize_name(part) for part in parts if _clean(part))
        if merged and merged not in seen:
            seen.add(merged)
            keys.append(merged)
    if text and "(" in text:
        stripped = re.sub(r"\s*\([^)]*\)\s*", " ", text)
        stripped_key = _normalize_name(stripped)
        if stripped_key and stripped_key not in seen:
            seen.add(stripped_key)
            keys.append(stripped_key)
    return keys


def _normalize_huwise_field_name(value: str) -> str:
    text = _clean(value).lower()
    text = re.sub(r"\s+", "_", text)
    text = re.sub(r"[^a-z0-9_]", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text


def _resolve_geojson_file_for_dataset(geo_dataset: str) -> Path | None:
    if not DATASETS_DIR.exists():
        return None
    candidates = sorted(DATASETS_DIR.glob("*.geojson"))
    match_keys = _normalize_geo_dataset_match_keys(geo_dataset)
    if not match_keys:
        return None
    for candidate in candidates:
        stem_normalized = _normalize_name(candidate.stem)
        for key in match_keys:
            if stem_normalized.endswith(key):
                return candidate
    for candidate in candidates:
        stem_normalized = _normalize_name(candidate.stem)
        for key in match_keys:
            if key in stem_normalized:
                return candidate
    return None


def _read_geojson_properties(path: Path) -> list[str]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    features = payload.get("features", [])
    if not features:
        return []
    properties = features[0].get("properties", {})
    if not isinstance(properties, dict):
        return []
    return [str(key) for key in properties.keys()]


def _append_preserved_fields_missing_from_dataspot(
    fields: list[dict[str, Any]],
    preserved_schema: list[dict[str, Any]] | None,
    geojson_properties: list[str],
) -> list[dict[str, Any]]:
    """Re-attach GeoJSON-only columns so ``export`` and ``custom`` survive ETL.

    Top-level ``name`` / ``description`` / ``datentyp`` / ``mehrwertigkeit`` come from the
    task (synthetic defaults, then ``map_links`` fixed copy); only ``custom`` and ``export``
    are taken from the saved YAML.
    """
    if not preserved_schema or not geojson_properties:
        return fields
    geo_names = {str(p) for p in geojson_properties}
    existing: set[str] = set()
    for item in fields:
        if isinstance(item, dict):
            tn = _clean(item.get("technical_name"))
            if tn:
                existing.add(tn)
    extra: list[dict[str, Any]] = []
    for item in preserved_schema:
        if not isinstance(item, dict):
            continue
        tn = _clean(item.get("technical_name"))
        if not tn or tn not in geo_names or tn in existing:
            continue
        export_default = tn.lower() != "gdh_fid"
        if tn.lower() == "map_links":
            export_default = False
        custom_payload = _custom_block_from_preserved_row(item)
        row: dict[str, Any] = {
            "technical_name": tn,
            "name": tn,
            "description": "",
            "mehrwertigkeit": "",
            "datentyp": "text",
            "export": _schema_export_value(item.get("export"), default=export_default),
            "custom": custom_payload,
        }
        if not _clean(custom_payload.get("technical_name")):
            row["custom"]["technical_name"] = _normalize_huwise_field_name(tn)
        extra.append(row)
        existing.add(tn)
    return fields + extra if extra else fields


def _reconcile_schema_fields_with_geojson(fields: list[dict[str, Any]], geojson_properties: list[str]) -> list[dict[str, Any]]:
    by_name: dict[str, dict[str, Any]] = {}
    for item in fields:
        if not isinstance(item, dict):
            continue
        technical_name = _clean(item.get("technical_name"))
        if technical_name:
            by_name[technical_name] = item
    merged: list[dict[str, Any]] = []
    for property_name in geojson_properties:
        row = dict(by_name.get(property_name, {}))
        if not row:
            export_default = property_name.lower() != "gdh_fid"
            if property_name.lower() == "map_links":
                export_default = False
            row = {
                "technical_name": property_name,
                "name": property_name,
                "description": "",
                "mehrwertigkeit": "",
                "datentyp": "text",
                "export": export_default,
                "custom": {
                    "technical_name": _normalize_huwise_field_name(property_name),
                    "name": "",
                    "description": "",
                    "datentyp": "",
                    "mehrwertigkeit": "",
                },
            }
        custom = row.get("custom")
        if not isinstance(custom, dict):
            custom = {}
        custom.setdefault("technical_name", _normalize_huwise_field_name(_clean(row.get("technical_name"))))
        custom.setdefault("name", "")
        custom.setdefault("description", "")
        custom.setdefault("datentyp", "")
        custom.setdefault("mehrwertigkeit", "")
        row["custom"] = custom
        if _clean(row.get("technical_name")).lower() == "gdh_fid" and row.get("export") is None:
            row["export"] = False
        merged.append(row)
    if "geometry" in by_name and "geometry" not in {item.get("technical_name") for item in merged}:
        geometry_row = dict(by_name["geometry"])
        custom = geometry_row.get("custom")
        if not isinstance(custom, dict):
            custom = {}
        custom.setdefault("technical_name", "geometry")
        custom.setdefault("name", "")
        custom.setdefault("description", "")
        custom.setdefault("datentyp", "")
        custom.setdefault("mehrwertigkeit", "")
        geometry_row["custom"] = custom
        merged.append(geometry_row)
    return merged


def _apply_map_links_schema_field(
    fields: list[dict[str, Any]],
    mapbs_url: str,
    *,
    create_map_links: bool,
) -> list[dict[str, Any]]:
    """Ensure the synthetic ``map_links`` row exists in the schema YAML.

    The row's ``export`` flag *is* the per-dataset ``create_map_links``
    toggle. When a row already exists we preserve the user-edited
    ``export`` (so manual toggles survive an ETL refresh); when injecting
    a fresh row, we seed ``export`` from the resolved
    ``create_map_links`` value passed in by the caller.
    """
    _ = mapbs_url  # Kept for compatibility at call site.
    fixed_name = "Zum Objekt navigieren"
    fixed_description = "URL zur Navigation des Standorts in einer Karten-App"
    updated: list[dict[str, Any]] = []
    has_map_links = False
    for row in fields:
        if not isinstance(row, dict):
            updated.append(row)
            continue
        r = dict(row)
        if _clean(r.get("technical_name")).lower() == "map_links":
            has_map_links = True
            r["name"] = fixed_name
            r["description"] = fixed_description
            r["datentyp"] = "text"
            r["mehrwertigkeit"] = ""
            custom = r.get("custom")
            if not isinstance(custom, dict):
                custom = {}
            custom = dict(custom)
            custom.setdefault("technical_name", "map_links")
            custom.setdefault("name", "")
            custom.setdefault("description", "")
            custom.setdefault("datentyp", "")
            custom.setdefault("mehrwertigkeit", "")
            r["custom"] = custom
            # Preserve the user-edited export flag if present; otherwise
            # fall back to the resolved create_map_links default.
            if "export" not in r:
                r["export"] = bool(create_map_links)
        updated.append(r)
    if has_map_links:
        return updated
    inject: dict[str, Any] = {
        "technical_name": "map_links",
        "name": fixed_name,
        "description": fixed_description,
        "mehrwertigkeit": "",
        "datentyp": "text",
        "export": bool(create_map_links),
        "custom": {
            "technical_name": "map_links",
            "name": "",
            "description": "",
            "datentyp": "",
            "mehrwertigkeit": "",
        },
    }
    idx = next(
        (
            i
            for i, r in enumerate(updated)
            if isinstance(r, dict) and _clean(r.get("technical_name")).lower() == "geometry"
        ),
        len(updated),
    )
    updated.insert(idx, inject)
    return updated


def _create_map_links(geometry: Any, p1: str, p2: str) -> str | None:
    """Build opendatabs map-links URL from a feature geometry and MapBS layer parameters."""
    p1q = urllib.parse.quote(p1)
    p2q = urllib.parse.quote(p2)
    if geometry is None:
        return None
    if geometry.geom_type == "Polygon":
        centroid = geometry.centroid
    else:
        centroid = geometry
    lat, lon = centroid.y, centroid.x
    return f"https://opendatabs.github.io/map-links/?lat={lat}&lon={lon}&p1={p1q}&p2={p2q}"


def _extract_map_params_from_mapbs_link(link: str) -> tuple[str | None, str | None]:
    """Follow MapBS URL redirects and read ``tree_groups`` / ``tree_group_layers_*`` query params."""
    if not _clean(link):
        return None, None
    try:

        @with_http_retry
        def _fetch_redirect() -> httpx.Response:
            with httpx.Client(follow_redirects=True, timeout=60.0) as client:
                return client.get(link)

        response = _fetch_redirect()
        redirect_link = str(response.url)
        parsed = urllib.parse.urlparse(redirect_link)
        query_params = urllib.parse.parse_qs(parsed.query)
        p1 = query_params.get("tree_groups", [None])[0]
        p2 = None
        for key, values in query_params.items():
            if key.startswith("tree_group_layers_"):
                p2 = values[0] if values else None
                break
        return p1, p2
    except Exception as exc:
        logging.warning("Could not extract MapBS redirect parameters: %s", exc)
        return None, None


def _add_map_links_to_dataset(dataset_file: Path, mapbs_link: str) -> bool:
    """Add a ``map_links`` attribute to each feature using MapBS layer tree parameters (GeoJSON in place)."""
    p1, p2 = _extract_map_params_from_mapbs_link(mapbs_link)
    if not p1 or not p2:
        logging.warning("Map link parameters missing for %s", dataset_file)
        return False
    try:
        gdf = gpd.read_file(dataset_file)
        gdf_transformed = gdf.copy()
        gdf_transformed = gdf_transformed.to_crs("EPSG:4326")
        if "geometry" not in gdf_transformed.columns:
            logging.warning("No geometry column in %s", dataset_file)
            return False
        gdf_transformed["map_links"] = gdf_transformed["geometry"].apply(
            lambda geom: _create_map_links(geom, p1, p2) if geom is not None else None
        )
        gdf["map_links"] = gdf_transformed["map_links"]
        # Atomic write via .tmp + os.replace so a crash mid-write does not
        # corrupt the local GeoJSON (publishing reads it again later).
        tmp_path = dataset_file.with_suffix(dataset_file.suffix + ".tmp")
        gdf.to_file(tmp_path, driver="GeoJSON")
        os.replace(tmp_path, dataset_file)
        logging.info("map_links added: %s", dataset_file)
        return True
    except Exception as exc:
        logging.error("map_links update failed for %s: %s", dataset_file, exc)
        return False


def _coerce_create_map_links_flag(
    preserved: dict[str, Any] | None,
    legacy: dict[str, Any] | None,
    mapbs_url: str,
) -> bool:
    """Whether to run MapBS enrichment for this dataset.

    The flag is encoded in the schema YAML via the ``export`` value on the
    synthetic ``map_links`` field row: ``export: true`` means "produce
    map_links and publish them to HUWISE"; ``export: false`` (or no row)
    means "do not enrich the GeoJSON".

    Priority:

    1. ``map_links.export`` on the preserved schema YAML (the source of
       truth users edit).
    2. ``create_map_links`` on the previous catalog row (one-time
       migration fallback for older catalogs that still carry the key).
    3. Truthiness of ``mapbs_url`` as the default for collections that
       ship a MapBS link.
    """

    def _coerce(raw: Any) -> bool | None:
        if isinstance(raw, bool):
            return raw
        s = _clean(raw).lower()
        if s in ("false", "0", "no", "off"):
            return False
        if s in ("true", "1", "yes", "on"):
            return True
        return None

    if isinstance(preserved, dict):
        for field in preserved.get("fields", []) if isinstance(preserved.get("fields"), list) else []:
            if not isinstance(field, dict):
                continue
            if _clean(field.get("technical_name")).lower() != "map_links":
                continue
            resolved = _coerce(field.get("export"))
            if resolved is not None:
                return resolved
            break
    if isinstance(legacy, dict) and "create_map_links" in legacy:
        resolved = _coerce(legacy.get("create_map_links"))
        if resolved is not None:
            return resolved
    return bool(_clean(mapbs_url))


def _safe_layer_filename_stem(geo_dataset: str) -> str:
    """Build a STAC/FGI-style file stem part from a layer title (no path separators)."""
    text = _clean(geo_dataset)
    replacements = {
        "ä": "ae",
        "ö": "oe",
        "ü": "ue",
        "Ä": "Ae",
        "Ö": "Oe",
        "Ü": "Ue",
        "ß": "ss",
    }
    for src, dst in replacements.items():
        text = text.replace(src, dst)
    text = re.sub(r"[^A-Za-z0-9_]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text or "layer"


def _expected_stac_download_path(collection_id: str, geo_dataset: str) -> Path:
    """Path used by :func:`_download_stac_layer_geojson` for a collection layer."""
    return DATASETS_DIR / f"{_clean(collection_id)}_{_safe_layer_filename_stem(geo_dataset)}.geojson"


def _find_zip_entry_for_geo_layer(zip_names: list[str], geo_dataset: str) -> str | None:
    """Match a GeoJSON member inside the STAC ``latest/geojson`` ZIP to a Geometa layer label."""
    if not zip_names or not _clean(geo_dataset):
        return None
    wanted = str(geo_dataset).strip()
    wlow = wanted.lower()
    for name in zip_names:
        if name.endswith("/"):
            continue
        file_name = Path(name).name
        stem = Path(file_name).stem
        if file_name.lower() == wlow or stem.lower() == wlow:
            return name
    for name in zip_names:
        if name.endswith("/"):
            continue
        stem = Path(name).stem
        s_norm = _normalize_name(stem)
        for key in _normalize_geo_dataset_match_keys(geo_dataset):
            if not key:
                continue
            if s_norm == key or s_norm.endswith(key):
                return name
    geojsons = [n for n in zip_names if n.lower().endswith(".geojson") and not n.endswith("/")]
    if len(geojsons) == 1:
        return geojsons[0]
    return None


def _atomic_write_bytes(path: Path, data: bytes) -> None:
    """Write ``data`` to ``path`` via a ``.tmp`` + :func:`os.replace`.

    Prevents corrupt half-written files when the process is killed mid-write.
    """
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_bytes(data)
    os.replace(tmp_path, path)


def _download_stac_layer_geojson(
    collection_id: str,
    geo_dataset: str,
    out_dir: Path,
    *,
    zip_cache: dict[str, bytes] | None = None,
) -> Path | None:
    """Download STAC collection GeoJSON archive and extract the layer matching ``geo_dataset``.

    Source: ``/stac/v1/download/{collection_id}/latest/geojson`` (ZIP). The
    ``zip_cache`` argument lets callers reuse the same archive bytes across
    multiple layer extractions (one HTTP roundtrip per collection instead of
    one per layer); the cache is keyed by collection id. The extracted
    GeoJSON is written via :func:`_atomic_write_bytes` so a crash mid-write
    never leaves a corrupted local file behind.
    """
    cid = _clean(collection_id)
    if not cid or not _clean(geo_dataset):
        return None
    out_dir.mkdir(parents=True, exist_ok=True)
    output_file = out_dir / f"{cid}_{_safe_layer_filename_stem(geo_dataset)}.geojson"

    cache_key = cid.lower()
    cached_zip = zip_cache.get(cache_key) if zip_cache is not None else None
    if cached_zip is None:
        url = f"{STAC_V1_BASE_URL}/download/{cid}/latest/geojson"
        headers = _api_geo_bs_headers()
        try:
            cached_zip = _http_get_bytes(url, headers=headers, timeout=HTTP_TIMEOUT_LONG)
        except Exception as exc:
            logging.warning("STAC GeoJSON download failed for %s / %r: %s", cid, geo_dataset, exc)
            return None
        if zip_cache is not None:
            zip_cache[cache_key] = cached_zip

    try:
        with zipfile.ZipFile(io.BytesIO(cached_zip), "r") as zf:
            names = [n for n in zf.namelist() if n and not n.endswith("/")]
            if not names:
                raise ValueError("STAC GeoJSON ZIP has no file entries")
            matched = _find_zip_entry_for_geo_layer(names, geo_dataset)
            if not matched:
                raise ValueError(
                    f"No GeoJSON entry matches layer {geo_dataset!r}; archive contains: {names!r}"
                )
            with zf.open(matched) as source:
                _atomic_write_bytes(output_file, source.read())
        logging.info("STAC GeoJSON saved: %s", output_file)
        return output_file
    except Exception as exc:
        logging.warning("STAC GeoJSON extract failed for %s / %r: %s", cid, geo_dataset, exc)
        return None


def _schema_file_slug(value: str) -> str:
    text = _clean(value)
    replacements = {
        "ä": "ae",
        "ö": "oe",
        "ü": "ue",
        "Ä": "Ae",
        "Ö": "Oe",
        "Ü": "Ue",
        "ß": "ss",
    }
    for src, dst in replacements.items():
        text = text.replace(src, dst)
    slug = re.sub(r"[^A-Za-z0-9_-]+", "_", text).strip("_")
    return slug or "dataset"


def _schema_yaml_path(schema_basename: str) -> Path:
    return SCHEMA_FILES_DIR / f"{_schema_file_slug(schema_basename)}.yaml"


def _yaml_str_representer(dumper: yaml.SafeDumper, value: str) -> yaml.ScalarNode:
    """Emit long / multi-line strings as folded block scalars (``>-``).

    Folded blocks are robust against manual edits: indentation alone scopes
    the value, so adding lines or wrapping does not break the file. This
    replaces the previous ``_recover_yaml_broken_continuations`` hack that
    tried to repair dedented continuation lines after the fact.
    """
    if "\n" in value or len(value) > 120:
        return dumper.represent_scalar("tag:yaml.org,2002:str", value, style=">")
    return dumper.represent_scalar("tag:yaml.org,2002:str", value)


yaml.SafeDumper.add_representer(str, _yaml_str_representer)


def _load_preserved_schema_payload(
    path: Path, *, huwise_id: str, dataspot_dataset_id: str
) -> dict[str, Any] | None:
    """Load the existing schema YAML so user edits survive an ETL refresh.

    Returns the full payload (with at minimum ``fields`` and optionally
    ``create_map_links``) when the file exists and the ``huwise_id`` /
    ``dataspot_dataset_id`` match. ``publish_catalog.yaml`` does not embed
    per-dataset schema; this is the only mechanism that carries
    ``fields[].custom`` and ``create_map_links`` across runs.

    If the YAML is malformed (e.g. after a botched manual edit), fail loudly
    so we do not silently overwrite user data on the next ETL run.
    """
    if not path.is_file():
        return None
    try:
        raw_text = path.read_text(encoding="utf-8")
    except OSError as exc:
        logging.warning("Could not read schema file %s for merge: %s", path, exc)
        return None
    try:
        payload = yaml.safe_load(raw_text)
    except yaml.YAMLError as exc:
        logging.error(
            "Schema file %s has invalid YAML; refusing to merge so user edits "
            "are not silently overwritten. Fix the file and re-run. Error: %s",
            path,
            exc,
        )
        raise RuntimeError(
            f"Cannot parse schema file {path}: invalid YAML. "
            f"Fix the file manually and re-run. Original error: {exc}"
        ) from exc
    if not isinstance(payload, dict):
        return None
    file_ds = _clean(payload.get("dataspot_dataset_id")).lower()
    if file_ds and file_ds != _clean(dataspot_dataset_id).lower():
        logging.warning(
            "Not merging schema from %s: dataspot_dataset_id mismatch (file=%s, run=%s)",
            path,
            file_ds,
            dataspot_dataset_id,
        )
        return None
    file_hw = _clean(payload.get("huwise_id"))
    if file_hw and file_hw != _clean(huwise_id):
        logging.warning(
            "Not merging schema from %s: huwise_id mismatch (file=%s, run=%s)",
            path,
            file_hw,
            huwise_id,
        )
        return None
    return payload


def _write_schema_file(
    *,
    huwise_id: str,
    dataspot_dataset_id: str,
    schema_basename: str,
    fields: list[dict[str, Any]],
) -> str:
    """Write the schema YAML. ``create_map_links`` is *not* a separate
    top-level key; the synthetic ``map_links`` field's ``export`` value is
    the single source of truth (see :func:`_coerce_create_map_links_flag`).
    """
    path = _schema_yaml_path(schema_basename)
    payload = {
        "huwise_id": huwise_id,
        "dataspot_dataset_id": dataspot_dataset_id,
        "fields": fields,
    }
    path.write_text(
        yaml.safe_dump(payload, allow_unicode=True, sort_keys=False, width=10_000),
        encoding="utf-8",
    )
    return str(path)


def _load_existing_catalog() -> dict[str, Any]:
    if not CATALOG_FILE.exists():
        return {"version": 1, "datasets": []}
    payload = yaml.safe_load(CATALOG_FILE.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        return {"version": 1, "datasets": []}
    datasets = payload.get("datasets", [])
    if not isinstance(datasets, list):
        datasets = []
    return {"version": 1, "datasets": datasets}


# Catalog-vs-Dataspot precedence table for ``_metadata_block``.
#
# Each row is "catalog edit wins, Dataspot is the fallback" (EDITORIAL) –
# this matches today's behaviour and lets a human override the automated
# pull by editing ``publish_catalog.yaml`` (or, more commonly, by editing
# the corresponding metadata in HUWISE which the publish layer's three-way
# merge then keeps; see ``publish_dataset._set_metadata_fields``).
#
# Fields not in this table are either purely derived (URL composition, tag
# defaults) or always come from a single side (e.g. ``modified_updates_on_data_change``).
_METADATA_PRECEDENCE_DOC = """
Precedence table (catalog-side key -> Dataspot key, all EDITORIAL):
    default.title              -> dataspot_meta["title"]            (fallback geo_dataset)
    default.description        -> dataspot_meta["description"]      (HTML-escaped via _description_to_html)
    default.keyword            -> dataspot_meta["keyword_values"]   (collection.keywords seeded first)
    default.publisher          -> producer_organization / dataspot_meta["publisher_path"]
    default.modified           -> dataspot_meta["modified"]
    dcat.created               -> dataspot_meta["created"]
    dcat.issued                -> dataspot_meta["issued"]
    dcat.accrualperiodicity    -> dataspot_meta["accrualperiodicity"]
    custom.publizierende_organisation -> derived publisher_from_path

MANAGED (always overwritten from upstream, no catalog override):
    custom.geodaten_modellbeschreibung -> {stac_url}#{dataspot_dataset_id}
    custom.tags                        -> [opendata.swiss, stac_collection_id]
    dcat.creator                       -> publisher_from_path
    dcat.relation                      -> [stac_browser_url, mapbs_url, ...]
    internal.license                   -> "CC BY 4.0"
""".strip()


def _metadata_block(
    dataset: dict[str, Any],
    *,
    dataspot_meta: dict[str, Any],
    dataspot_dataset_id: str,
    stac_collection_id: str,
    geo_dataset: str,
    producer_organization: str,
    collection_keywords: list[str],
    stac_url: str,
    stac_browser_url: str,
    mapbs_url: str,
) -> dict[str, Any]:
    # See ``_METADATA_PRECEDENCE_DOC`` for the EDITORIAL / MANAGED split that
    # drives the ``_clean(default.get("X")) or dataspot_meta["X"]`` pattern
    # below.
    metadata = dataset.get("metadata", {})
    if not isinstance(metadata, dict):
        metadata = {}
    default = metadata.get("default", {})
    dcat = metadata.get("dcat", {})
    custom = metadata.get("custom", {})
    if not isinstance(default, dict):
        default = {}
    if not isinstance(dcat, dict):
        dcat = {}
    if not isinstance(custom, dict):
        custom = {}
    relation_values_raw = dcat.get("relation", [])
    if isinstance(relation_values_raw, list):
        relation_values = [value.strip() for value in relation_values_raw if _clean(value)]
    else:
        relation_values = [value.strip() for value in _clean(relation_values_raw).split(";") if value.strip()]
    relation_values_final: list[str] = []
    for url in [stac_browser_url, mapbs_url, *relation_values]:
        cleaned = _clean(url)
        if cleaned and cleaned not in relation_values_final:
            relation_values_final.append(cleaned)
    default_publisher = _clean(default.get("publisher"))
    publisher_from_path = (
        _third_path_segment(default_publisher)
        or _third_path_segment(producer_organization)
        or _third_path_segment(dataspot_meta["publisher_path"])
    )
    if not publisher_from_path:
        publisher_from_path = default_publisher or _clean(producer_organization) or dataspot_meta["publisher_path"]
    publizierende_organisation = publisher_from_path
    keyword_values = [item for item in collection_keywords if _clean(item)]
    if not keyword_values:
        keyword_values_raw = default.get("keyword")
        if isinstance(keyword_values_raw, list):
            keyword_values = [item.strip() for item in keyword_values_raw if _clean(item)]
        else:
            keyword_values = [item.strip() for item in _clean(keyword_values_raw).split(";") if item.strip()]
    if not keyword_values:
        keyword_values = [item for item in dataspot_meta["keyword_values"] if _clean(item)]
    keyword_values = [item for item in keyword_values if _clean(item).lower() != _clean(stac_collection_id).lower()]
    tags = [item for item in ["opendata.swiss", stac_collection_id] if _clean(item)]
    expected_geodaten_modellbeschreibung = f"{stac_url}#{dataspot_dataset_id}"
    custom_geodaten_modellbeschreibung = _clean(custom.get("geodaten_modellbeschreibung"))
    geodaten_modellbeschreibung = (
        custom_geodaten_modellbeschreibung
        if custom_geodaten_modellbeschreibung.endswith(f"#{dataspot_dataset_id}")
        else expected_geodaten_modellbeschreibung
    )
    return {
        "default": {
            "title": _clean(default.get("title")) or dataspot_meta["title"] or geo_dataset,
            "description": _description_to_html(_clean(default.get("description")) or dataspot_meta["description"]),
            "keyword": keyword_values,
            "language": "de",
            "publisher": publisher_from_path,
            "modified": _clean(default.get("modified")) or dataspot_meta["modified"],
            "modified_updates_on_data_change": False,
        },
        "internal": {
            "license": "CC BY 4.0",
        },
        "dcat": {
            "creator": publisher_from_path,
            "created": _clean(dcat.get("created")) or dataspot_meta["created"],
            "issued": _clean(dcat.get("issued")) or dataspot_meta["issued"],
            "accrualperiodicity": _clean(dcat.get("accrualperiodicity")) or dataspot_meta["accrualperiodicity"],
            "relation": relation_values_final,
        },
        "custom": {
            "publizierende_organisation": _clean(custom.get("publizierende_organisation")) or publizierende_organisation,
            "geodaten_modellbeschreibung": geodaten_modellbeschreibung,
            "tags": tags,
        },
    }


def rebuild_catalog(*, skip_geojson_download: bool = False, skip_map_links: bool = False) -> dict[str, Any]:
    existing_payload = _load_existing_catalog()
    existing = existing_payload.get("datasets", [])
    by_uuid: dict[str, dict[str, Any]] = {}
    for collection in existing:
        if not isinstance(collection, dict):
            continue
        for geo_item in collection.get("geo_datasets", []) if isinstance(collection.get("geo_datasets"), list) else []:
            if not isinstance(geo_item, dict):
                continue
            key = _clean(geo_item.get("dataspot_dataset_id")).lower()
            if key:
                by_uuid[key] = geo_item

    auth = DataspotAuth()
    dataspot_meta_cache: dict[str, dict[str, Any]] = {}
    stac_zip_cache: dict[str, bytes] = {}
    output_collections: list[dict[str, Any]] = []
    for collection in _fetch_stac_collections():
        collection_id = _clean(collection.get("id"))
        collection_title = _clean(collection.get("title"))
        if not collection_id:
            continue
        links = _extract_links(collection)
        mapbs_url = _clean(links.get("related", ""))
        collection_keywords = _extract_string_list(collection.get("keywords"))
        producer_organization, _ = _extract_orgs(collection.get("providers"))
        instances = _discover_instances_for_collection(collection_id, collection_title)
        geo_rows: list[dict[str, Any]] = []
        for instance in instances:
            dataspot_uuid = _clean(instance.get("dataspot_uuid")).lower()
            geo_dataset = _clean(instance.get("geo_dataset")) or collection_title
            if not dataspot_uuid:
                continue
            dataspot_meta = dataspot_meta_cache.get(dataspot_uuid)
            if dataspot_meta is None:
                dataspot_meta = _dataspot_metadata(auth, dataspot_uuid)
                dataspot_meta_cache[dataspot_uuid] = dataspot_meta
            if _clean(dataspot_meta.get("object_type")).lower() != "dataset":
                logging.info(
                    "Skipping non-dataset Dataspot object for %s: %s (%s, _type=%s)",
                    collection_id,
                    geo_dataset,
                    dataspot_uuid,
                    _clean(dataspot_meta.get("object_type")) or "unknown",
                )
                continue
            old = by_uuid.get(dataspot_uuid, {})
            huwise_id = _clean(old.get("huwise_id"))

            preserved_payload: dict[str, Any] | None = None
            schema_basename: str | None = None
            if huwise_id:
                geojson_file_hint = _resolve_geojson_file_for_dataset(geo_dataset)
                if geojson_file_hint:
                    schema_basename = geojson_file_hint.stem
                else:
                    schema_basename = f"{collection_id}_{_schema_file_slug(geo_dataset)}"
                preserved_payload = _load_preserved_schema_payload(
                    _schema_yaml_path(schema_basename),
                    huwise_id=huwise_id,
                    dataspot_dataset_id=dataspot_uuid,
                )

            create_map_links_flag = bool(huwise_id) and _coerce_create_map_links_flag(
                preserved_payload, old, mapbs_url
            )
            if huwise_id and not skip_geojson_download:
                _download_stac_layer_geojson(
                    collection_id, geo_dataset, DATASETS_DIR, zip_cache=stac_zip_cache
                )
            if (
                huwise_id
                and not skip_map_links
                and create_map_links_flag
                and mapbs_url
            ):
                layer_path = _expected_stac_download_path(collection_id, geo_dataset)
                if not layer_path.is_file():
                    alt = _resolve_geojson_file_for_dataset(geo_dataset)
                    if alt is not None:
                        layer_path = alt
                if layer_path.is_file():
                    _add_map_links_to_dataset(layer_path, mapbs_url)
            row: dict[str, Any] = {
                "huwise_id": huwise_id,
                "dataspot_dataset_id": dataspot_uuid,
                "dataspot_asset_url": f"https://bs.dataspot.io/web/prod/assets/{dataspot_uuid}",
                "geo_dataset": geo_dataset,
                "metadata": _metadata_block(
                    old,
                    dataspot_meta=dataspot_meta,
                    dataspot_dataset_id=dataspot_uuid,
                    stac_collection_id=collection_id,
                    geo_dataset=geo_dataset,
                    producer_organization=producer_organization,
                    collection_keywords=collection_keywords,
                    stac_url=GEOMETA_HTML_URL.format(collection_id=collection_id),
                    stac_browser_url=f"https://radiantearth.github.io/stac-browser/#/external/api.geo.bs.ch/stac/v1/collections/{collection_id}",
                    mapbs_url=mapbs_url,
                ),
            }
            if huwise_id and schema_basename is not None:
                preserved_fields = None
                if isinstance(preserved_payload, dict):
                    candidate = preserved_payload.get("fields")
                    if isinstance(candidate, list):
                        preserved_fields = candidate
                geojson_file = _resolve_geojson_file_for_dataset(geo_dataset)
                geojson_properties = _read_geojson_properties(geojson_file) if geojson_file else []
                # Schema reconciliation is intentionally split into four
                # stages with single, auditable responsibilities:
                #   1. ``_dataspot_schema`` – fetch upstream Dataspot
                #      compositions/attributes/datatypes and re-attach any
                #      preserved ``custom``/``export`` blocks for matching
                #      fields (so manual edits survive a refresh).
                #   2. ``_append_preserved_fields_missing_from_dataspot`` –
                #      re-introduce preserved fields that Dataspot no longer
                #      lists but still appear in the local GeoJSON
                #      (transient upstream gaps must not erase columns).
                #   3. ``_reconcile_schema_fields_with_geojson`` – add
                #      GeoJSON-only properties that were never in Dataspot
                #      (so the schema is a superset of what gets uploaded).
                #   4. ``_apply_map_links_schema_field`` – inject the
                #      synthetic ``map_links`` row with the fixed default
                #      label/datatype the publisher expects.
                fields = _dataspot_schema(auth, dataspot_uuid, preserved_fields)
                fields = _append_preserved_fields_missing_from_dataspot(
                    fields, preserved_fields, geojson_properties
                )
                fields = _reconcile_schema_fields_with_geojson(fields, geojson_properties)
                fields = _apply_map_links_schema_field(
                    fields, mapbs_url, create_map_links=create_map_links_flag
                )
                _write_schema_file(
                    huwise_id=huwise_id,
                    dataspot_dataset_id=dataspot_uuid,
                    schema_basename=schema_basename,
                    fields=fields,
                )
            geo_rows.append(row)

        if not geo_rows:
            continue
        geo_rows.sort(key=lambda item: _clean(item.get("geo_dataset")).lower())
        output_collections.append(
            {
                "stac_collection_id": collection_id,
                "stac_url": GEOMETA_HTML_URL.format(collection_id=collection_id),
                "stac_browser_url": f"https://radiantearth.github.io/stac-browser/#/external/api.geo.bs.ch/stac/v1/collections/{collection_id}",
                "mapbs_url": mapbs_url,
                "geo_datasets": geo_rows,
            }
        )

    output_collections.sort(key=lambda item: _clean(item.get("stac_collection_id")))
    payload = {"version": 1, "datasets": output_collections}
    CATALOG_FILE.write_text(yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8")
    return payload


def run_publish(*, dry_run: bool, force_metadata_sync: bool = False, force_field_recreate: bool = False) -> None:
    """Run the publish pipeline in-process (no second Python startup)."""
    import publish_dataset

    publish_dataset.run(
        dry_run=dry_run,
        force_metadata_sync=force_metadata_sync,
        force_field_recreate=force_field_recreate,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refresh YAML catalog from STAC + run publish.")
    parser.add_argument("--dry-run", action="store_true", help="Run publish in dry-run mode.")
    parser.add_argument("--refresh-only", action="store_true", help="Only rebuild YAML catalog.")
    parser.add_argument(
        "--skip-geojson-download",
        action="store_true",
        help="Do not fetch per-layer GeoJSON from the STAC download endpoint (offline / use existing files in data/datasets).",
    )
    parser.add_argument(
        "--skip-map-links",
        action="store_true",
        help="Do not call MapBS (httpx) to enrich GeoJSON with map_links (no redirect fetch).",
    )
    parser.add_argument(
        "--force-metadata-sync",
        action="store_true",
        help="Forward --force-metadata-sync to the publish step.",
    )
    parser.add_argument(
        "--force-field-recreate",
        action="store_true",
        help="Forward --force-field-recreate to the publish step (delete/recreate HUWISE fields with changed datentyp).",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    args = parse_args()
    payload = rebuild_catalog(
        skip_geojson_download=args.skip_geojson_download,
        skip_map_links=args.skip_map_links,
    )
    print(f"Catalog updated: {CATALOG_FILE} ({len(payload.get('datasets', []))} datasets)")
    if args.refresh_only:
        return
    run_publish(
        dry_run=args.dry_run,
        force_metadata_sync=args.force_metadata_sync,
        force_field_recreate=args.force_field_recreate,
    )


if __name__ == "__main__":
    main()
