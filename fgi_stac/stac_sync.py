"""STAC catalog rebuild, GeoJSON download, schema sync, and map links."""

from __future__ import annotations

import asyncio
import io
import logging
import os
import re
import urllib.parse
import zipfile
from pathlib import Path
from typing import Any

import geopandas as gpd
import httpx
import yaml
from catalog import (
    active_huwise_ids_from_bindings,
    existing_geo_by_dataspot_uuid,
    load_active_dataset_rows,
    load_bindings_with_fallback,
    prune_all_publish_artifacts,
    write_bindings_workbook,
    write_flat_publish_catalog,
)
from dataspot_api import (
    DATASPOT_ATTRIBUTE_URL,
    DATASPOT_COMPOSITIONS_URL,
    DATASPOT_RANGE_ASSET_URL,
    dataspot_get,
    dataspot_metadata,
)
from dataspot_auth import DataspotAuth
from http_client import (
    HTTP_TIMEOUT,
    HTTP_TIMEOUT_LONG,
    http_get_bytes,
    http_get_json,
    http_get_json_async,
    with_http_retry,
)
from metadata import build_metadata_block
from paths import (
    BINDINGS_FILE,
    LEGACY_CATALOG_FILE,
    ORIG_CATALOG_FILE,
    ORIG_DATASETS_DIR,
    PUBLISH_DATASETS_DIR,
    ensure_layout_dirs,
)
from schema_merge import (
    build_orig_schema_payload,
    geometa_stac_url,
    override_fields_for_etl,
    schema_export_value,
    schema_file_slug,
    schema_orig_path,
    sync_user_schema_file,
)
from transform_runner import publish_geojson_path, run_transform
from yaml_io import dump_yaml

from util import (
    clean,
    extract_string_list,
    normalize_geo_dataset_match_keys,
    normalize_huwise_field_name,
    normalize_name,
    read_geojson_properties,
)

STAC_V1_BASE_URL = "https://api.geo.bs.ch/stac/v1"
STAC_COLLECTIONS_URL = f"{STAC_V1_BASE_URL}/collections"
GEOMETA_HTML_URL = "https://api.geo.bs.ch/geometa/v1/metadata_details/dataset/published/html/{collection_id}"
GEOMETA_JSON_URL = "https://api.geo.bs.ch/geometa/v1/metadata_details/dataset/published/json/{collection_id}"


def _api_geo_bs_headers() -> dict[str, str]:
    """Headers required by api.geo.bs.ch endpoints that need authentication."""
    api_key = os.getenv("API_KEY_MAPBS", "")
    return {"apikey": api_key} if api_key else {}


CATALOG_FILE = ORIG_CATALOG_FILE
DATASETS_DIR = ORIG_DATASETS_DIR
PUBLISH_GEOJSON_DIR = PUBLISH_DATASETS_DIR
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


def _custom_block_from_preserved_row(item: dict[str, Any]) -> dict[str, Any]:
    """Build ``custom`` from a saved schema row (the only top-level keys ETL preserves)."""
    old_custom = item.get("custom")
    if isinstance(old_custom, dict):
        return {
            "technical_name": clean(old_custom.get("technical_name")),
            "name": clean(old_custom.get("name")),
            "description": clean(old_custom.get("description")),
            "datentyp": clean(old_custom.get("datentyp")),
            "mehrwertigkeit": clean(old_custom.get("mehrwertigkeit")),
        }
    return {
        "technical_name": clean(old_custom),
        "name": "",
        "description": "",
        "datentyp": "",
        "mehrwertigkeit": "",
    }


def _fetch_stac_collections() -> list[dict[str, Any]]:
    headers = _api_geo_bs_headers()
    payload = http_get_json(STAC_COLLECTIONS_URL, headers=headers, timeout=HTTP_TIMEOUT)
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
        rel = clean(item.get("rel"))
        href = clean(item.get("href"))
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
        name = clean(provider.get("name"))
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
    payload = http_get_json(url, headers=headers, timeout=HTTP_TIMEOUT)
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
        uuid = clean(entry.get("dataset_uuid")).lower()
        if not uuid or uuid in seen:
            continue
        seen.add(uuid)
        label = clean(entry.get("label")) or collection_title or "Datensatz"
        instances.append({"dataspot_uuid": uuid, "geo_dataset": label})
    return instances


def _dataspot_schema(
    auth: DataspotAuth, dataset_id: str, old_schema: list[dict[str, Any]] | None
) -> list[dict[str, Any]]:
    old_by_name = {}
    if isinstance(old_schema, list):
        old_by_name = {clean(item.get("technical_name")): item for item in old_schema if isinstance(item, dict)}

    compositions_data = (
        dataspot_get(auth, DATASPOT_COMPOSITIONS_URL.format(dataset_id=dataset_id), allow_404=True) or {}
    )
    compositions = compositions_data.get("_embedded", {}).get("compositions", [])
    if not isinstance(compositions, list):
        compositions = []

    def _map_datatype(value: str) -> str:
        normalized = clean(value).lower()
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
        cardinality = clean(attribute.get("cardinality") or attribute.get("hasCardinality")).lower()
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
            "export": schema_export_value(_old_value(old, "export"), default=technical_name.lower() != "gdh_fid"),
            "custom": custom_payload,
        }
        if not clean(custom_payload.get("technical_name")):
            row["custom"]["technical_name"] = normalize_huwise_field_name(technical_name)
        return row

    async def _build_rows() -> list[dict[str, Any]]:
        headers = auth.get_headers()
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT, limits=HTTP_LIMITS) as client:
            composition_items: list[tuple[dict[str, Any], str]] = []
            attribute_tasks: list[asyncio.Future] = []
            for composition in compositions:
                if not isinstance(composition, dict):
                    continue
                attribute_id = clean(composition.get("composedOf"))
                if not attribute_id:
                    continue
                composition_items.append((composition, attribute_id))
                attribute_tasks.append(
                    http_get_json_async(
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
                has_range_id = clean((attribute_payload or {}).get("hasRange"))
                if has_range_id:
                    datatype_ids.append(has_range_id)
                    datatype_tasks.append(
                        http_get_json_async(
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
            has_range_id = clean(attribute_payload.get("hasRange"))
            datatype_payload = datatype_by_id.get(has_range_id)
            datatype_label = clean((datatype_payload or {}).get("label")) or clean(
                (datatype_payload or {}).get("title")
            )
            technical_name = clean(composition.get("title")) or clean(composition.get("label"))
            if not technical_name:
                continue
            if "geometr" in technical_name.lower():
                technical_name = "geometry"
            field_name = clean(composition.get("label")) or technical_name
            description = clean(attribute_payload.get("description")) or clean(composition.get("description"))
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


def _resolve_geojson_file_for_dataset(geo_dataset: str) -> Path | None:
    if not PUBLISH_GEOJSON_DIR.exists():
        return None
    candidates = sorted(PUBLISH_GEOJSON_DIR.glob("*.geojson"))
    match_keys = normalize_geo_dataset_match_keys(geo_dataset)
    if not match_keys:
        return None
    for candidate in candidates:
        stem_normalized = normalize_name(candidate.stem)
        for key in match_keys:
            if stem_normalized.endswith(key):
                return candidate
    for candidate in candidates:
        stem_normalized = normalize_name(candidate.stem)
        for key in match_keys:
            if key in stem_normalized:
                return candidate
    return None


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
            tn = clean(item.get("technical_name"))
            if tn:
                existing.add(tn)
    extra: list[dict[str, Any]] = []
    for item in preserved_schema:
        if not isinstance(item, dict):
            continue
        tn = clean(item.get("technical_name"))
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
            "export": schema_export_value(item.get("export"), default=export_default),
            "custom": custom_payload,
        }
        if not clean(custom_payload.get("technical_name")):
            row["custom"]["technical_name"] = normalize_huwise_field_name(tn)
        extra.append(row)
        existing.add(tn)
    return fields + extra if extra else fields


def _reconcile_schema_fields_with_geojson(
    fields: list[dict[str, Any]], geojson_properties: list[str]
) -> list[dict[str, Any]]:
    by_name: dict[str, dict[str, Any]] = {}
    for item in fields:
        if not isinstance(item, dict):
            continue
        technical_name = clean(item.get("technical_name"))
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
                    "technical_name": normalize_huwise_field_name(property_name),
                    "name": "",
                    "description": "",
                    "datentyp": "",
                    "mehrwertigkeit": "",
                },
            }
        custom = row.get("custom")
        if not isinstance(custom, dict):
            custom = {}
        custom.setdefault("technical_name", normalize_huwise_field_name(clean(row.get("technical_name"))))
        custom.setdefault("name", "")
        custom.setdefault("description", "")
        custom.setdefault("datentyp", "")
        custom.setdefault("mehrwertigkeit", "")
        row["custom"] = custom
        if clean(row.get("technical_name")).lower() == "gdh_fid" and row.get("export") is None:
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
        if clean(r.get("technical_name")).lower() == "map_links":
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
            if isinstance(r, dict) and clean(r.get("technical_name")).lower() == "geometry"
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
    if not clean(link):
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
        s = clean(raw).lower()
        if s in ("false", "0", "no", "off"):
            return False
        if s in ("true", "1", "yes", "on"):
            return True
        return None

    if isinstance(preserved, dict):
        for field in preserved.get("fields", []) if isinstance(preserved.get("fields"), list) else []:
            if not isinstance(field, dict):
                continue
            if clean(field.get("technical_name")).lower() != "map_links":
                continue
            resolved = _coerce(field.get("export"))
            if resolved is not None:
                return resolved
            break
    if isinstance(legacy, dict) and "create_map_links" in legacy:
        resolved = _coerce(legacy.get("create_map_links"))
        if resolved is not None:
            return resolved
    return bool(clean(mapbs_url))


def _safe_layer_filename_stem(geo_dataset: str) -> str:
    """Build a STAC/FGI-style file stem part from a layer title (no path separators)."""
    text = clean(geo_dataset)
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
    return DATASETS_DIR / f"{clean(collection_id)}_{_safe_layer_filename_stem(geo_dataset)}.geojson"


def _find_zip_entry_for_geo_layer(zip_names: list[str], geo_dataset: str) -> str | None:
    """Match a GeoJSON member inside the STAC ``latest/geojson`` ZIP to a Geometa layer label."""
    if not zip_names or not clean(geo_dataset):
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
        s_norm = normalize_name(stem)
        for key in normalize_geo_dataset_match_keys(geo_dataset):
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
    cid = clean(collection_id)
    if not cid or not clean(geo_dataset):
        return None
    out_dir.mkdir(parents=True, exist_ok=True)
    output_file = out_dir / f"{cid}_{_safe_layer_filename_stem(geo_dataset)}.geojson"

    cache_key = cid.lower()
    cached_zip = zip_cache.get(cache_key) if zip_cache is not None else None
    if cached_zip is None:
        url = f"{STAC_V1_BASE_URL}/download/{cid}/latest/geojson"
        headers = _api_geo_bs_headers()
        try:
            cached_zip = http_get_bytes(url, headers=headers, timeout=HTTP_TIMEOUT_LONG)
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
                raise ValueError(f"No GeoJSON entry matches layer {geo_dataset!r}; archive contains: {names!r}")
            with zf.open(matched) as source:
                _atomic_write_bytes(output_file, source.read())
        logging.info("STAC GeoJSON saved: %s", output_file)
        return output_file
    except Exception as exc:
        logging.warning("STAC GeoJSON extract failed for %s / %r: %s", cid, geo_dataset, exc)
        return None


def _load_preserved_schema_payload(
    schema_basename: str, *, huwise_id: str, dataspot_dataset_id: str
) -> dict[str, Any] | None:
    """Load user export settings (``data/schema_files``) for ETL merge."""
    preserved_fields = override_fields_for_etl(
        schema_basename,
        huwise_id=huwise_id,
        dataspot_dataset_id=dataspot_dataset_id,
    )
    if not preserved_fields:
        return None
    return {"fields": preserved_fields}


def _write_schema_file(
    *,
    huwise_id: str,
    dataspot_dataset_id: str,
    stac_collection_id: str,
    schema_basename: str,
    fields: list[dict[str, Any]],
    dataspot_asset_url: str,
) -> str:
    """Write pipeline schema to ``data_orig/schema_files`` and sync ``data/schema_files``."""
    path = schema_orig_path(schema_basename)
    previous_orig = None
    if path.is_file():
        previous_orig = yaml.safe_load(path.read_text(encoding="utf-8"))
        if not isinstance(previous_orig, dict):
            previous_orig = None
    orig_payload = build_orig_schema_payload(
        huwise_id=huwise_id,
        dataspot_asset_url=dataspot_asset_url,
        stac_url=geometa_stac_url(stac_collection_id, dataspot_dataset_id),
        fields=fields,
    )
    path.write_text(dump_yaml(orig_payload), encoding="utf-8")
    sync_user_schema_file(schema_basename, orig_payload, previous_orig_payload=previous_orig)
    return str(path)


def _matches_huwise_filter(huwise_id: str, huwise_id_filter: str) -> bool:
    filt = clean(huwise_id_filter)
    if not filt:
        return True
    return clean(huwise_id) == filt


def _prepare_one_dataset_assets(
    *,
    auth: DataspotAuth,
    collection_id: str,
    geo_dataset: str,
    huwise_id: str,
    dataspot_uuid: str,
    mapbs_url: str,
    old: dict[str, Any],
    stac_zip_cache: dict[str, bytes],
) -> None:
    """Download GeoJSON, transform, sync schema YAML, and enrich map_links for one bound dataset."""
    logging.info("STEP prepare_assets huwise_id=%s geo_dataset=%s", huwise_id, geo_dataset)
    geojson_file_hint = _resolve_geojson_file_for_dataset(geo_dataset)
    if geojson_file_hint:
        schema_basename = geojson_file_hint.stem
    else:
        schema_basename = f"{collection_id}_{schema_file_slug(geo_dataset)}"
    preserved_payload = _load_preserved_schema_payload(
        schema_basename,
        huwise_id=huwise_id,
        dataspot_dataset_id=dataspot_uuid,
    )
    create_map_links_flag = _coerce_create_map_links_flag(preserved_payload, old, mapbs_url)

    raw_path = _download_stac_layer_geojson(collection_id, geo_dataset, DATASETS_DIR, zip_cache=stac_zip_cache)
    if raw_path is not None:
        stem = schema_basename or raw_path.stem
        run_transform(
            input_path=raw_path,
            output_path=publish_geojson_path(stem),
            stem=stem,
        )

    if create_map_links_flag and mapbs_url:
        layer_path = publish_geojson_path(schema_basename)
        if not layer_path.is_file():
            alt = _resolve_geojson_file_for_dataset(geo_dataset)
            if alt is not None:
                layer_path = alt
        if layer_path.is_file():
            _add_map_links_to_dataset(layer_path, mapbs_url)

    preserved_fields = None
    if isinstance(preserved_payload, dict):
        candidate = preserved_payload.get("fields")
        if isinstance(candidate, list):
            preserved_fields = candidate
    geojson_file = _resolve_geojson_file_for_dataset(geo_dataset)
    geojson_properties = read_geojson_properties(geojson_file) if geojson_file else []
    fields = _dataspot_schema(auth, dataspot_uuid, preserved_fields)
    fields = _append_preserved_fields_missing_from_dataspot(fields, preserved_fields, geojson_properties)
    fields = _reconcile_schema_fields_with_geojson(fields, geojson_properties)
    fields = _apply_map_links_schema_field(fields, mapbs_url, create_map_links=create_map_links_flag)
    _write_schema_file(
        huwise_id=huwise_id,
        dataspot_dataset_id=dataspot_uuid,
        stac_collection_id=collection_id,
        schema_basename=schema_basename,
        fields=fields,
        dataspot_asset_url=f"https://bs.dataspot.io/web/prod/assets/{dataspot_uuid}",
    )


def sync_catalog() -> dict[str, Any]:
    """Refresh STAC/Dataspot metadata, bindings workbook, and flat publish catalog."""
    logging.info("STEP sync_catalog start")
    ensure_layout_dirs()
    catalog_path = CATALOG_FILE if CATALOG_FILE.exists() else LEGACY_CATALOG_FILE
    bindings = load_bindings_with_fallback({"version": 1, "datasets": []})
    by_uuid = existing_geo_by_dataspot_uuid(catalog_path)

    auth = DataspotAuth()
    dataspot_meta_cache: dict[str, dict[str, Any]] = {}
    output_collections: list[dict[str, Any]] = []
    for collection in _fetch_stac_collections():
        collection_id = clean(collection.get("id"))
        collection_title = clean(collection.get("title"))
        if not collection_id:
            continue
        links = _extract_links(collection)
        mapbs_url = clean(links.get("related", ""))
        collection_keywords = extract_string_list(collection.get("keywords"))
        producer_organization, _ = _extract_orgs(collection.get("providers"))
        instances = _discover_instances_for_collection(collection_id, collection_title)
        geo_rows: list[dict[str, Any]] = []
        for instance in instances:
            dataspot_uuid = clean(instance.get("dataspot_uuid")).lower()
            geo_dataset = clean(instance.get("geo_dataset")) or collection_title
            if not dataspot_uuid:
                continue
            dataspot_meta = dataspot_meta_cache.get(dataspot_uuid)
            if dataspot_meta is None:
                dataspot_meta = dataspot_metadata(auth, dataspot_uuid)
                dataspot_meta_cache[dataspot_uuid] = dataspot_meta
            if clean(dataspot_meta.get("object_type")).lower() != "dataset":
                logging.info(
                    "Skipping non-dataset Dataspot object for %s: %s (%s, _type=%s)",
                    collection_id,
                    geo_dataset,
                    dataspot_uuid,
                    clean(dataspot_meta.get("object_type")) or "unknown",
                )
                continue
            old = by_uuid.get(dataspot_uuid, {})
            huwise_id = bindings.get((collection_id, geo_dataset), "") or clean(old.get("huwise_id"))
            geo_rows.append(
                {
                    "huwise_id": huwise_id,
                    "dataspot_dataset_id": dataspot_uuid,
                    "dataspot_asset_url": f"https://bs.dataspot.io/web/prod/assets/{dataspot_uuid}",
                    "geo_dataset": geo_dataset,
                    "metadata": build_metadata_block(
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
            )

        if not geo_rows:
            continue
        geo_rows.sort(key=lambda item: clean(item.get("geo_dataset")).lower())
        output_collections.append(
            {
                "stac_collection_id": collection_id,
                "stac_url": GEOMETA_HTML_URL.format(collection_id=collection_id),
                "stac_browser_url": f"https://radiantearth.github.io/stac-browser/#/external/api.geo.bs.ch/stac/v1/collections/{collection_id}",
                "mapbs_url": mapbs_url,
                "geo_datasets": geo_rows,
            }
        )

    output_collections.sort(key=lambda item: clean(item.get("stac_collection_id")))
    payload = {"version": 1, "datasets": output_collections}
    write_bindings_workbook(payload, BINDINGS_FILE, bindings=bindings)
    write_flat_publish_catalog(payload, bindings, CATALOG_FILE)
    active_ids = active_huwise_ids_from_bindings(bindings)
    prune_all_publish_artifacts(active_ids)
    logging.info("STEP sync_catalog done (%s active huwise_id)", len(active_ids))
    return payload


def prepare_assets(*, huwise_id_filter: str = "") -> None:
    """Download GeoJSON, run transforms, sync schema YAML, and add map_links for bound datasets."""
    logging.info("STEP prepare_assets start")
    ensure_layout_dirs()
    if not CATALOG_FILE.is_file() and not LEGACY_CATALOG_FILE.is_file():
        raise FileNotFoundError(f"Missing publish catalog at {CATALOG_FILE}. Run `uv run sync_catalog.py` first.")
    if not BINDINGS_FILE.is_file():
        raise FileNotFoundError(f"Missing bindings workbook at {BINDINGS_FILE}. Run `uv run sync_catalog.py` first.")

    by_uuid = existing_geo_by_dataspot_uuid(CATALOG_FILE if CATALOG_FILE.exists() else LEGACY_CATALOG_FILE)
    auth = DataspotAuth()
    stac_zip_cache: dict[str, bytes] = {}
    rows = load_active_dataset_rows()
    if not rows:
        logging.warning("No active huwise_id rows in %s; nothing to prepare", BINDINGS_FILE)
        return

    prepared = 0
    for row in rows:
        huwise_id = clean(row.get("huwise_id"))
        if not _matches_huwise_filter(huwise_id, huwise_id_filter):
            continue
        collection_id = clean(row.get("stac_collection_id"))
        geo_dataset = clean(row.get("geo_dataset"))
        dataspot_uuid = clean(row.get("dataspot_dataset_id")).lower()
        mapbs_url = clean(row.get("mapbs_url"))
        if not collection_id or not geo_dataset or not dataspot_uuid:
            logging.warning("Skipping incomplete binding row for huwise_id=%s", huwise_id)
            continue
        old = by_uuid.get(dataspot_uuid, {})
        _prepare_one_dataset_assets(
            auth=auth,
            collection_id=collection_id,
            geo_dataset=geo_dataset,
            huwise_id=huwise_id,
            dataspot_uuid=dataspot_uuid,
            mapbs_url=mapbs_url,
            old=old,
            stac_zip_cache=stac_zip_cache,
        )
        prepared += 1

    logging.info("STEP prepare_assets done (%s dataset(s))", prepared)
