"""Single entrypoint for catalog refresh + publish."""

from __future__ import annotations

import argparse
import html
import json
import subprocess
import sys
import re
import time
from pathlib import Path
from typing import Any

import requests
import yaml

from dataspot_auth import DataspotAuth

STAC_COLLECTIONS_URL = "https://api.geo.bs.ch/stac/v1/collections"
GEOMETA_HTML_URL = "https://api.geo.bs.ch/geometa/v1/metadata_details/dataset/preview/html/{collection_id}"
DATASPOT_COMPOSITIONS_URL = "https://bs.dataspot.io/rest/prod/datasets/{dataset_id}/compositions"
DATASPOT_DATASET_URL = "https://bs.dataspot.io/rest/prod/datasets/{dataset_id}"
DATASPOT_ATTRIBUTE_URL = "https://bs.dataspot.io/rest/prod/attributes/{attribute_id}"
DATASPOT_RANGE_ASSET_URL = "https://bs.dataspot.io/rest/prod/assets/{asset_id}"
DATA_DIR = Path("data")
CATALOG_FILE = DATA_DIR / "publish_catalog.yaml"
DATASETS_DIR = DATA_DIR / "datasets"
SCHEMA_FILES_DIR = DATA_DIR / "schema_files"
PUB_DATASETS_XLSX = DATA_DIR / "pub_datasets.xlsx"
_NAV_ANCHOR_RE = re.compile(
    r'<a\s+href="#([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})"\s*>\s*<li>([^<]*)</li>',
    re.IGNORECASE | re.DOTALL,
)
_H3_ID_RE = re.compile(
    r'<h3\s+id="([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})"\s*>\s*([^<]*)',
    re.IGNORECASE | re.DOTALL,
)
_DATATYPE_MAP = {
    "date": "date",
    "datum": "date",
    "datetime": "datetime",
    "zeitstempel": "datetime",
    "uhrzeit": "datetime",
    "ganzzahl": "int",
    "integer": "int",
    "int": "int",
    "number": "number",
    "decimal": "number",
    "kommazahl": "number",
    "dezimal": "number",
    "float": "number",
    "double": "number",
    "geometrie (punkt)": "geo_point_2d",
    "geometrie (linie)": "geo_shape",
    "geometrie (fläche)": "geo_shape",
    "geometrie (flaeche)": "geo_shape",
    "boolean": "boolean",
    "bool": "boolean",
    "geometry": "geometry",
    "geometrie": "geometry",
    "datei": "file",
    "file": "file",
    "text": "text",
    "formatierter text": "text",
    "string": "text",
}


def _clean(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _fetch_stac_collections() -> list[dict[str, Any]]:
    response = requests.get(STAC_COLLECTIONS_URL, timeout=90)
    response.raise_for_status()
    payload = response.json()
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


def _fetch_geometa_collection_html(collection_id: str) -> str:
    url = GEOMETA_HTML_URL.format(collection_id=collection_id)
    response = requests.get(url, timeout=90)
    response.raise_for_status()
    return response.text


def _discover_instances_for_collection(collection_id: str, collection_title: str) -> list[dict[str, str]]:
    html = _fetch_geometa_collection_html(collection_id)
    seen: set[str] = set()
    instances: list[dict[str, str]] = []

    def _add(uuid: str, label: str) -> None:
        key = uuid.lower()
        if key in seen:
            return
        seen.add(key)
        instances.append({"dataspot_uuid": key, "geo_dataset": _clean(label) or collection_title or "Datensatz"})

    for match in _NAV_ANCHOR_RE.finditer(html):
        _add(match.group(1), match.group(2))
    for match in _H3_ID_RE.finditer(html):
        _add(match.group(1), match.group(2))
    return instances


def _dataspot_get(auth: DataspotAuth, url: str, *, allow_404: bool = False) -> dict[str, Any] | None:
    response = requests.get(url=url, headers=auth.get_headers(), timeout=60)
    if response.status_code == 404 and allow_404:
        return None
    response.raise_for_status()
    return response.json()


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
        "title": _clean(payload.get("label") or payload.get("title")),
        "description": _clean(payload.get("description")),
        "keyword_values": keywords,
        "publisher_path": publisher_path,
        "created": _normalize_optional_date(custom.get("creationDate")),
        "issued": _normalize_optional_date(custom.get("publicationDate")),
        "accrualperiodicity": _clean(payload.get("accrualPeriodicity")),
    }


def _description_to_html(value: Any) -> str:
    text = _clean(value)
    if not text:
        return ""
    if re.search(r"<[a-zA-Z][^>]*>", text):
        return text
    paragraphs = [part.strip() for part in re.split(r"\n\s*\n", text) if part.strip()]
    if not paragraphs:
        return html.escape(text).replace("\n", "<br>")
    return "\n".join(f"<p>{html.escape(part).replace('\n', '<br>')}</p>" for part in paragraphs)


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

    def _bool_like(value: Any, *, default: bool) -> bool:
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        text = _clean(value).lower()
        if text in {"", "none"}:
            return default
        return text not in {"false", "0", "no", "off"}

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
        old_custom = _old_value(old, "custom")
        if isinstance(old_custom, dict):
            custom_payload = {
                "technical_name": _clean(old_custom.get("technical_name")),
                "name": _clean(old_custom.get("name")),
                "description": _clean(old_custom.get("description")),
            }
        else:
            custom_payload = {
                "technical_name": _clean(old_custom),
                "name": "",
                "description": "",
            }
        row: dict[str, Any] = {
            "technical_name": technical_name,
            "name": field_name or technical_name,
            "description": description,
            "mehrwertigkeit": _clean(_old_value(old, "mehrwertigkeit")) or multivalued_separator,
            "datentyp": _map_datatype(datatype_label),
            "custom": custom_payload,
        }
        if not _clean(custom_payload.get("technical_name")):
            row["custom"]["technical_name"] = _normalize_huwise_field_name(technical_name)
        export_default = technical_name.lower() != "gdh_fid"
        row["export"] = _bool_like(_old_value(old, "export"), default=export_default)
        return row

    rows: list[dict[str, Any]] = []
    for composition in compositions:
        if not isinstance(composition, dict):
            continue
        attribute_id = _clean(composition.get("composedOf"))
        if not attribute_id:
            continue
        attribute_payload = _dataspot_get(auth, DATASPOT_ATTRIBUTE_URL.format(attribute_id=attribute_id), allow_404=True)
        if not attribute_payload:
            continue
        has_range_id = _clean(attribute_payload.get("hasRange"))
        datatype_label = ""
        if has_range_id:
            range_asset = _dataspot_get(auth, DATASPOT_RANGE_ASSET_URL.format(asset_id=has_range_id), allow_404=True)
            if range_asset:
                datatype_label = _clean(range_asset.get("label")) or _clean(range_asset.get("title"))
        technical_name = _clean(composition.get("title")) or _clean(composition.get("label"))
        if not technical_name:
            continue
        if "geometr" in technical_name.lower():
            technical_name = "geometry"
        field_name = _clean(composition.get("label")) or technical_name
        description = _clean(attribute_payload.get("description")) or _clean(composition.get("description"))
        multivalued_separator = _multivalued_separator_from_attribute(attribute_payload)
        rows.append(
            _compose_row(
                technical_name=technical_name,
                field_name=field_name,
                description=description,
                datatype_label=datatype_label,
                multivalued_separator=multivalued_separator,
            )
        )
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
    while STAC/geo metadata uses the full phrase *Tagesheime und Kitas*.
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
            row = {
                "technical_name": property_name,
                "name": property_name,
                "description": "",
                "mehrwertigkeit": "",
                "datentyp": "text",
                "custom": {
                    "technical_name": _normalize_huwise_field_name(property_name),
                    "name": "",
                    "description": "",
                },
                "export": property_name.lower() != "gdh_fid",
            }
        custom = row.get("custom")
        if not isinstance(custom, dict):
            custom = {}
        custom.setdefault("technical_name", _normalize_huwise_field_name(_clean(row.get("technical_name"))))
        custom.setdefault("name", "")
        custom.setdefault("description", "")
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
        geometry_row["custom"] = custom
        merged.append(geometry_row)
    return merged


def ensure_output_dirs() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    DATASETS_DIR.mkdir(parents=True, exist_ok=True)
    SCHEMA_FILES_DIR.mkdir(parents=True, exist_ok=True)


def _load_schema_from_file(schema_file: str) -> list[dict[str, Any]]:
    path = Path(schema_file)
    if not path.is_absolute():
        path = Path(schema_file)
    if not path.exists():
        return []
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        return []
    fields = payload.get("fields", [])
    return fields if isinstance(fields, list) else []


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


def _write_schema_file(*, huwise_id: str, dataspot_dataset_id: str, schema_basename: str, fields: list[dict[str, Any]]) -> str:
    path = SCHEMA_FILES_DIR / f"{_schema_file_slug(schema_basename)}.yaml"
    payload = {
        "huwise_id": huwise_id,
        "dataspot_dataset_id": dataspot_dataset_id,
        "fields": fields,
    }
    path.write_text(yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8")
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


def _metadata_block(
    dataset: dict[str, Any],
    *,
    auth: DataspotAuth,
    dataspot_dataset_id: str,
    geo_dataset: str,
    producer_organization: str,
    collection_keywords: list[str],
    stac_url: str,
    stac_browser_url: str,
    mapbs_url: str,
) -> dict[str, Any]:
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
    dataspot_meta = _dataspot_metadata(auth, dataspot_dataset_id)
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
    producer_path = _clean(default.get("publisher")) or _clean(producer_organization) or dataspot_meta["publisher_path"]
    producer_parts = [part.strip() for part in producer_path.split("/") if part.strip()]
    publisher_last = producer_parts[-1] if producer_parts else ""
    publizierende_organisation = producer_parts[1] if len(producer_parts) > 1 else ""
    keyword_values = [item for item in collection_keywords if _clean(item)]
    if not keyword_values:
        keyword_values_raw = default.get("keyword")
        if isinstance(keyword_values_raw, list):
            keyword_values = [item.strip() for item in keyword_values_raw if _clean(item)]
        else:
            keyword_values = [item.strip() for item in _clean(keyword_values_raw).split(";") if item.strip()]
    if not keyword_values:
        keyword_values = [item for item in dataspot_meta["keyword_values"] if _clean(item)]
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
            "publisher": publisher_last,
            "modified_updates_on_data_change": bool(default.get("modified_updates_on_data_change", True)),
        },
        "internal": {
            "license": "CC BY 4.0",
        },
        "dcat": {
            "creator": _clean(dcat.get("creator")) or publisher_last,
            "created": _clean(dcat.get("created")) or dataspot_meta["created"],
            "issued": _clean(dcat.get("issued")) or dataspot_meta["issued"],
            "accrualperiodicity": _clean(dcat.get("accrualperiodicity")) or dataspot_meta["accrualperiodicity"],
            "relation": relation_values_final,
        },
        "custom": {
            "publizierende_organisation": _clean(custom.get("publizierende_organisation")) or publizierende_organisation,
            "geodaten_modellbeschreibung": geodaten_modellbeschreibung,
            "tags": custom.get("tags") if isinstance(custom.get("tags"), list) else ["opendata.swiss"],
        },
    }


def _legacy_huwise_map() -> dict[str, str]:
    if not PUB_DATASETS_XLSX.exists():
        return {}
    try:
        import pandas as pd
    except Exception:
        return {}
    frame = pd.read_excel(PUB_DATASETS_XLSX).fillna("")
    mapping: dict[str, str] = {}
    for _, row in frame.iterrows():
        dataset_id = _clean(row.get("id")).lower()
        huwise_id = _clean(row.get("ods_id"))
        if dataset_id and huwise_id:
            mapping[dataset_id] = huwise_id
    return mapping


def rebuild_catalog() -> dict[str, Any]:
    ensure_output_dirs()
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
                if not isinstance(geo_item.get("schema"), list):
                    schema_file = _clean(geo_item.get("schema_file"))
                    if schema_file:
                        geo_item = dict(geo_item)
                        geo_item["schema"] = _load_schema_from_file(schema_file)
                by_uuid[key] = geo_item

    auth = DataspotAuth()
    legacy_huwise = _legacy_huwise_map()
    output_collections: list[dict[str, Any]] = []
    for collection in _fetch_stac_collections():
        collection_id = _clean(collection.get("id"))
        collection_title = _clean(collection.get("title"))
        if not collection_id:
            continue
        links = _extract_links(collection)
        collection_keywords = _extract_string_list(collection.get("keywords"))
        producer_organization, _ = _extract_orgs(collection.get("providers"))
        instances = _discover_instances_for_collection(collection_id, collection_title)
        geo_rows: list[dict[str, Any]] = []
        for instance in instances:
            dataspot_uuid = _clean(instance.get("dataspot_uuid")).lower()
            geo_dataset = _clean(instance.get("geo_dataset")) or collection_title
            if not dataspot_uuid:
                continue
            old = by_uuid.get(dataspot_uuid, {})
            row: dict[str, Any] = {
                "dataspot_dataset_id": dataspot_uuid,
                "dataspot_asset_url": f"https://bs.dataspot.io/web/prod/assets/{dataspot_uuid}",
                "geo_dataset": geo_dataset,
                "metadata": _metadata_block(
                    old,
                    auth=auth,
                    dataspot_dataset_id=dataspot_uuid,
                    geo_dataset=geo_dataset,
                    producer_organization=producer_organization,
                    collection_keywords=collection_keywords,
                    stac_url=GEOMETA_HTML_URL.format(collection_id=collection_id),
                    stac_browser_url=f"https://radiantearth.github.io/stac-browser/#/external/api.geo.bs.ch/stac/v1/collections/{collection_id}",
                    mapbs_url=links.get("related", ""),
                ),
            }
            huwise_id = _clean(old.get("huwise_id")) or _clean(legacy_huwise.get(dataspot_uuid))
            if huwise_id:
                row["huwise_id"] = huwise_id
                fields = _dataspot_schema(auth, dataspot_uuid, old.get("schema"))
                geojson_file = _resolve_geojson_file_for_dataset(geo_dataset)
                geojson_properties = _read_geojson_properties(geojson_file) if geojson_file else []
                fields = _reconcile_schema_fields_with_geojson(fields, geojson_properties)
                if geojson_file:
                    schema_basename = geojson_file.stem
                else:
                    schema_basename = f"{collection_id}_{_schema_file_slug(geo_dataset)}"
                row["schema_file"] = _write_schema_file(
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
                "mapbs_url": links.get("related", ""),
                "geo_datasets": geo_rows,
            }
        )

    output_collections.sort(key=lambda item: _clean(item.get("stac_collection_id")))
    payload = {"version": 1, "datasets": output_collections}
    CATALOG_FILE.write_text(yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8")
    return payload


def run_publish(*, dry_run: bool) -> None:
    command = [sys.executable, "publish_dataset.py"]
    if dry_run:
        command.append("--dry-run")
    subprocess.run(command, check=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refresh YAML catalog from STAC + run publish.")
    parser.add_argument("--dry-run", action="store_true", help="Run publish in dry-run mode.")
    parser.add_argument("--refresh-only", action="store_true", help="Only rebuild YAML catalog.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    payload = rebuild_catalog()
    print(f"Catalog updated: {CATALOG_FILE} ({len(payload.get('datasets', []))} datasets)")
    if args.refresh_only:
        return
    run_publish(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
