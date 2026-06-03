"""Publish STAC GeoJSON datasets to FTP and HUWISE.

This script processes datasets from ``data/publish_catalog.yaml`` and performs:

1. Dataspot schema extraction for each source dataset.
2. Schema reconciliation against local GeoJSON properties.
3. Optional per-field YAML override merge.
4. GeoJSON upload to FTP (remote folder ``fgi/stac``).
5. HUWISE dataset create/reuse and metadata updates.
6. HUWISE schema upsert based on the generated schema CSV.
"""

from __future__ import annotations

import logging
import re
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import common
import geopandas as gpd
import httpx
import pandas as pd
import yaml
from catalog import (
    load_active_dataset_rows,
    load_flat_publish_catalog,
    merge_snapshot_entries,
    order_snapshot_entry,
    prune_all_publish_artifacts,
    write_metadata_snapshot_file,
)
from common import change_tracking
from dataspot_auth import DataspotAuth
from http_client import HTTP_LIMITS, HTTP_TIMEOUT, with_http_retry
from huwise_utils_py import (
    HuwiseDataset,
    create_dataset,
    get_uid_by_id,
    list_dataset_field_configurations,
)
from huwise_utils_py.config import HuwiseConfig
from huwise_utils_py.http import HttpClient
from metadata import (
    DEFAULT_CONTACT_EMAIL,
    DEFAULT_CONTACT_NAME,
    DEFAULT_ATTRIBUTIONS,
    DEFAULT_GEOGRAPHIC_REFERENCE,
    DEFAULT_LICENSE,
    DEFAULT_RIGHTS,
    DEFAULT_TAG,
    GEOMETA_PREVIEW_URL,
    dataspot_uuid_from_snapshot,
)
from paths import (
    LEGACY_CATALOG_FILE,
    LEGACY_METADATA_LAST_PUSH_FILE,
    ORIG_CATALOG_FILE,
    ORIG_METADATA_LAST_PUSH_FILE,
    PUBLISH_DATASETS_DIR,
    ensure_layout_dirs,
)
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator
from schema_merge import (
    load_merged_schema_payload,
    resolve_schema_basename_for,
    schema_orig_path,
    schema_user_path,
)

from util import (
    clean_text,
    description_to_html,
    extract_stac_code,
    is_uuid,
    normalize_name,
    read_geojson_properties,
    split_keywords,
    split_semicolon_list,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

DATASETS_DIR = PUBLISH_DATASETS_DIR
PUBLISH_CATALOG_FILE = ORIG_CATALOG_FILE
PUBLISH_METADATA_LAST_PUSH_FILE = ORIG_METADATA_LAST_PUSH_FILE
FTP_REMOTE_FOLDER = "fgi/stac"
SOURCE_URL_PREFIX = "https://data-bs.ch/stata/fgi/stac"
SCHEMA_COLUMNS = [
    "technical_name_dataspot",
    "technical_name_huwise",
    "column_name",
    "description",
    "datatype",
    "multivalued_separator",
    "source",
]
DEFAULT_FIELD_TYPE_BY_DATATYPE = {
    "date": "date",
    "datetime": "datetime",
    "int": "int",
    "number": "double",
    "boolean": "boolean",
    "geo_point_2d": "geo_point_2d",
    "geometry": "geo_shape",
    "file": "file",
    "text": "text",
}
SCHEMA_PROCESSOR_LABEL_PREFIX = "FGI schema sync"
_FIELD_CONFIG_PAGE_SIZE = 100
THEME_MAP_DATA_BS_CH = {
    "arbeit, erwerb": "20bb143",
    "bau- und wohnungswesen": "c813f26",
    "bevolkerung": "3606293",
    "bildung, wissenschaft": "c9a169b",
    "energie": "06af88d",
    "finanzen": "b8b874a",
    "gebaude": "cc7ea4s",
    "geographie": "7542721",
    "gesetzgebung": "6173474",
    "gesundheit": "e2e248a",
    "handel": "d847e7c",
    "industrie, dienstleistungen": "da0ff7d",
    "kriminalitat, strafrecht": "ae41f5e",
    "kultur, medien, informationsgesellschaft, sport": "e9dc0c8",
    "land- und forstwirtschaft": "59506c3",
    "mobilitat und verkehr": "3d7f80f",
    "politik": "9b815ca",
    "preise": "338b3e5",
    "raum und umwelt": "186e3a8",
    "soziale sicherheit": "6e0eacc",
    "statistische grundlagen": "ca365da",
    "tourismus": "0a7844c",
    "verwaltung": "7b5b405",
    "volkswirtschaft": "0774467",
    "offentliche ordnung und sicherheit": "60c7454",
}


def ensure_output_dirs() -> None:
    ensure_layout_dirs()


@dataclass(frozen=True, slots=True)
class DatasetContext:
    """Minimal context for one dataset publishing run."""

    ods_id: str
    dataspot_dataset_id: str
    geo_dataset: str


def _dataspot_uuid_from_catalog(dataset: dict[str, Any]) -> str:
    """Return Dataspot REST dataset UUID (not STAC collection code like AFBA)."""
    raw_id = clean_text(dataset.get("dataspot_dataset_id"))
    if is_uuid(raw_id):
        return raw_id
    preview = clean_text(dataset.get("html_preview"))
    if "#" in preview:
        frag = preview.split("#")[-1].strip()
        if is_uuid(frag):
            return frag
    return raw_id


def _resolve_dataspot_dataset_id(dataset: dict[str, Any]) -> str:
    """Resolve id for Dataspot compositions/schema: UUID when present, else legacy short code."""
    resolved = _dataspot_uuid_from_catalog(dataset)
    if resolved and is_uuid(resolved):
        return resolved
    raw_id = clean_text(dataset.get("dataspot_dataset_id"))
    short_from_id = extract_stac_code(raw_id)
    if short_from_id:
        return short_from_id
    short_from_preview = extract_stac_code(dataset.get("html_preview"))
    if short_from_preview:
        return short_from_preview
    return raw_id


def _normalize_theme_key(value: str) -> str:
    """Normalize theme labels for deterministic mapping."""
    replacements = str.maketrans(
        {
            "ä": "a",
            "ö": "o",
            "ü": "u",
            "Ä": "A",
            "Ö": "O",
            "Ü": "U",
            "ß": "ss",
        }
    )
    cleaned = value.translate(replacements).lower().strip()
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned


def _resolve_theme_id(theme_cell: str) -> str:
    """Resolve the first mapped theme id from a semicolon-separated theme cell."""
    for theme_name in [part.strip() for part in theme_cell.split(";") if part.strip()]:
        mapped = THEME_MAP_DATA_BS_CH.get(_normalize_theme_key(theme_name))
        if mapped:
            return mapped
    return ""


class _SchemaCustomBlock(BaseModel):
    """User-editable override block on each schema field."""

    model_config = ConfigDict(extra="allow")

    technical_name: str = ""
    name: str = ""
    description: str = ""
    datentyp: str = ""
    mehrwertigkeit: str = ""


class _SchemaField(BaseModel):
    """One field entry inside a schema YAML's ``fields:`` list."""

    model_config = ConfigDict(extra="allow")

    technical_name: str = ""
    dataspot_attribute: str = ""
    name: str = ""
    description: str = ""
    datentyp: str = "text"
    mehrwertigkeit: str = ""
    export: bool = True
    custom: _SchemaCustomBlock = Field(default_factory=_SchemaCustomBlock)

    @model_validator(mode="after")
    def _resolve_technical_name(self) -> _SchemaField:
        if not clean_text(self.technical_name):
            ds = clean_text(self.dataspot_attribute)
            if ds:
                object.__setattr__(self, "technical_name", ds)
        if not clean_text(self.technical_name):
            raise ValueError("schema field requires technical_name or dataspot_attribute")
        return self


class _SchemaFileModel(BaseModel):
    """Top-level shape of ``data/schema_files/<basename>.yaml``.

    ``create_map_links`` is not a separate field on the schema file; the
    ``export`` flag on the synthetic ``map_links`` field row is the source
    of truth (see ``etl._coerce_create_map_links_flag``).
    """

    model_config = ConfigDict(extra="allow")

    huwise_id: str = ""
    dataspot_dataset_id: str = ""
    dataspot_asset_url: str = ""
    stac_url: str = ""
    fields: list[_SchemaField]


def snapshot_fields_only(entry: dict[str, Any]) -> dict[str, Any]:
    """Keep only ``template.field`` keys in canonical field order."""
    return order_snapshot_entry(entry)


def _metadata_row_field(metadata_row: pd.Series, snapshot_key: str, legacy_key: str = "") -> Any:
    value = metadata_row.get(snapshot_key)
    if value is None or (isinstance(value, str) and not clean_text(value)):
        if legacy_key:
            return metadata_row.get(legacy_key)
        return value
    return value


def _stac_collection_from_metadata_row(metadata_row: pd.Series) -> str:
    explicit = clean_text(metadata_row.get("stac_collection_id"))
    if explicit:
        return explicit
    tags = metadata_row.get("custom.tags")
    tag_list = (
        tags if isinstance(tags, list) else split_semicolon_list(tags) or split_semicolon_list(metadata_row.get("tags"))
    )
    for tag in tag_list:
        cleaned = clean_text(tag)
        if cleaned and cleaned.lower() not in {DEFAULT_TAG.lower(), "opendata.swiss"}:
            return cleaned
    return ""


def _load_catalog_dataframes() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load snapshot catalog + Excel bindings (``template.field`` keys only in YAML)."""

    catalog_path = PUBLISH_CATALOG_FILE if PUBLISH_CATALOG_FILE.exists() else LEGACY_CATALOG_FILE
    flat = load_flat_publish_catalog(catalog_path)
    if not flat:
        raise FileNotFoundError(f"No active datasets in publish catalog: {catalog_path}")

    binding_rows = {row["huwise_id"]: row for row in load_active_dataset_rows()}
    pub_rows: list[dict[str, Any]] = []
    metadata_rows: list[dict[str, Any]] = []
    for ods_id in sorted(flat.keys(), key=lambda value: clean_text(value)):
        entry = flat[ods_id]
        if not isinstance(entry, dict):
            continue
        snapshot = snapshot_fields_only(entry)
        binding = binding_rows.get(ods_id, {})
        dataspot_id = clean_text(binding.get("dataspot_dataset_id")) or dataspot_uuid_from_snapshot(snapshot)
        geo_dataset = clean_text(binding.get("geo_dataset"))
        stac_collection_id = clean_text(binding.get("stac_collection_id")) or _stac_collection_from_metadata_row(
            pd.Series(snapshot)
        )
        pub_rows.append(
            {
                "ods_id": ods_id,
                "id": dataspot_id,
                "geo_dataset": geo_dataset,
                "paket": stac_collection_id,
            }
        )
        metadata_row = dict(snapshot)
        metadata_row["ods_id"] = ods_id
        metadata_rows.append(metadata_row)

    return pd.DataFrame(pub_rows), pd.DataFrame(metadata_rows)


def _build_dataspot_client() -> DataspotAuth:
    """Construct a Dataspot authentication client."""
    return DataspotAuth()


def _load_publish_inputs() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load publish inputs from orig catalog + HUWISE bindings."""
    catalog_path = PUBLISH_CATALOG_FILE if PUBLISH_CATALOG_FILE.exists() else LEGACY_CATALOG_FILE
    if not catalog_path.exists():
        raise FileNotFoundError(f"Missing required catalog: {ORIG_CATALOG_FILE} or {LEGACY_CATALOG_FILE}")
    pub_df, metadata_df = _load_catalog_dataframes()
    logging.info("Loaded %s datasets with huwise_id from %s (+ bindings)", len(pub_df), catalog_path)
    return pub_df, metadata_df


def _extract_metadata_value(value: Any) -> Any:
    """Extract raw metadata values from HUWISE field payload shapes."""
    if isinstance(value, dict):
        if "value" in value:
            return _extract_metadata_value(value["value"])
        if "values" in value:
            return _extract_metadata_value(value["values"])
    return value


def _normalize_metadata_compare_value(value: Any) -> Any:
    """Normalize metadata for equality checks (Huwise payloads, snapshot file, or new values)."""
    extracted = _extract_metadata_value(value)
    if isinstance(extracted, list):
        return [item for item in [clean_text(v) for v in extracted] if item]
    if isinstance(extracted, bool):
        return extracted
    return clean_text(extracted)


def _metadata_snapshot_path() -> Path:
    if PUBLISH_METADATA_LAST_PUSH_FILE.exists():
        return PUBLISH_METADATA_LAST_PUSH_FILE
    if LEGACY_METADATA_LAST_PUSH_FILE.exists():
        return LEGACY_METADATA_LAST_PUSH_FILE
    return PUBLISH_METADATA_LAST_PUSH_FILE


def _load_last_push_snapshot(path: Path | None = None) -> dict[str, dict[str, Any]]:
    """Load last successful metadata push per ods_id and logical field key ``template.field``."""
    path = path or _metadata_snapshot_path()
    if not path.exists():
        return {}
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        return {}
    out: dict[str, dict[str, Any]] = {}
    for ods_raw, fields in raw.items():
        ods_id = clean_text(str(ods_raw))
        if not ods_id or not isinstance(fields, dict):
            continue
        out[ods_id] = {str(k): v for k, v in fields.items()}
    return out


def _save_last_push_snapshot(snapshot: dict[str, dict[str, Any]], path: Path = PUBLISH_METADATA_LAST_PUSH_FILE) -> None:
    """Persist metadata last-push snapshot (YAML, stable key order)."""
    write_metadata_snapshot_file(path, snapshot)
    # Reuse shared hash-file tracking from common for cheap change diagnostics in scheduled runs.
    change_tracking.update_check_file(str(path), method="hash")


def _coerce_string_list(value: Any) -> list[str]:
    """Normalize a metadata value to a list of non-empty strings."""
    extracted = _extract_metadata_value(value)
    if isinstance(extracted, list):
        return [clean_text(item) for item in extracted if clean_text(item)]
    text = clean_text(extracted)
    if not text:
        return []
    if ";" in text:
        return [part.strip() for part in text.split(";") if part.strip()]
    return [text]


def _fetch_metadata_templates(client: HttpClient, dataset_uid: str) -> dict[str, dict[str, Any]]:
    """Fetch HUWISE metadata templates for one dataset."""
    templates: dict[str, dict[str, Any]] = {}
    for template in ("default", "internal", "dcat", "dcat_ap_ch"):
        try:
            payload = client.get(f"/datasets/{dataset_uid}/metadata/{template}/").json()
        except Exception:
            continue
        if isinstance(payload, dict):
            templates[template] = payload
    return templates


def _get_template_field(templates: dict[str, dict[str, Any]], template: str, field: str) -> Any:
    """Return one metadata field value from fetched template payloads."""
    return _extract_metadata_value(templates.get(template, {}).get(field))


def _build_geojson_index() -> list[Path]:
    """Return all local GeoJSON files in the datasets folder."""
    if not DATASETS_DIR.exists():
        raise FileNotFoundError(f"Missing datasets folder: {DATASETS_DIR}")
    return sorted(DATASETS_DIR.glob("*.geojson"))


def _resolve_geojson_file(context: DatasetContext, candidates: list[Path]) -> Path | None:
    """Resolve the local GeoJSON for the given dataset context."""
    normalized_geo_dataset = normalize_name(context.geo_dataset)
    for candidate in candidates:
        stem_normalized = normalize_name(candidate.stem)
        if stem_normalized.endswith(normalized_geo_dataset):
            return candidate

    for candidate in candidates:
        stem_normalized = normalize_name(candidate.stem)
        if normalized_geo_dataset in stem_normalized:
            return candidate

    return None


def _geometa_collection_code_from_metadata(metadata_row: pd.Series) -> str:
    relation_urls = split_semicolon_list(metadata_row.get("relation_urls"))
    for url in relation_urls:
        marker = "/metadata_details/dataset/preview/html/"
        if marker in url:
            tail = url.split(marker, 1)[-1]
            code = tail.split("#", 1)[0].strip().strip("/")
            if code:
                return code
    return ""


@with_http_retry
def _fetch_geometa_preview_html(collection_id: str) -> str:
    with httpx.Client(timeout=HTTP_TIMEOUT, limits=HTTP_LIMITS) as client:
        response = client.get(GEOMETA_PREVIEW_URL.format(collection_id=collection_id))
    response.raise_for_status()
    return response.text


def _fetch_geometa_attribute_technical_names(collection_id: str, dataspot_uuid: str) -> set[str]:
    if not collection_id or not dataspot_uuid:
        return set()
    html = _fetch_geometa_preview_html(collection_id)
    start_marker = f'id="{dataspot_uuid}"'
    start_idx = html.find(start_marker)
    if start_idx < 0:
        return set()
    section = html[start_idx:]
    next_h3 = section.find("<h3", len(start_marker))
    if next_h3 > 0:
        section = section[:next_h3]
    extracted: set[str] = set()
    for row_html in re.findall(r"<tr>(.*?)</tr>", section, flags=re.DOTALL | re.IGNORECASE):
        cells = re.findall(r"<td[^>]*>\\s*(.*?)\\s*</td>", row_html, flags=re.DOTALL | re.IGNORECASE)
        if len(cells) < 2:
            continue
        # Attribute table: cell[1] is "Technische Bezeichnung"
        value = clean_text(re.sub(r"<[^>]+>", "", cells[1]))
        if value:
            extracted.add(value)
    return extracted


def _validate_dataspot_schema_against_geometa(
    *,
    schema_rows: list[dict[str, str]],
    metadata_row: pd.Series,
    dataspot_uuid: str,
    ods_id: str,
) -> None:
    collection_id = _geometa_collection_code_from_metadata(metadata_row)
    if not collection_id:
        return
    try:
        geometa_names = _fetch_geometa_attribute_technical_names(collection_id, dataspot_uuid)
    except Exception as exc:
        logging.warning(
            "Could not validate Geometa schema for ods_id=%s collection=%s uuid=%s: %s",
            ods_id,
            collection_id,
            dataspot_uuid,
            exc,
        )
        return
    if not geometa_names:
        return
    dataspot_names = {
        clean_text(row.get("technical_name_dataspot"))
        for row in schema_rows
        if clean_text(row.get("technical_name_dataspot"))
    }
    missing_in_dataspot = sorted(name for name in geometa_names if name not in dataspot_names)
    if missing_in_dataspot:
        logging.warning(
            "Geometa/Dataspot schema mismatch for ods_id=%s (%s#%s). Missing in Dataspot fetch: %s",
            ods_id,
            collection_id,
            dataspot_uuid,
            ", ".join(missing_in_dataspot[:10]),
        )


def _resolve_merged_schema_payload(huwise_id: str, dataspot_dataset_id: str) -> dict[str, Any] | None:
    """Load merged schema (data_orig + user overrides) for one dataset."""
    basename = resolve_schema_basename_for(huwise_id, dataspot_dataset_id)
    if not basename:
        return None
    return load_merged_schema_payload(
        basename,
        huwise_id=huwise_id,
        dataspot_dataset_id=dataspot_dataset_id,
    )


def _huwise_field_type_for_yaml_datentyp(datentyp: str) -> str:
    """Direct mapping from etl.py-normalized ``datentyp`` to HUWISE field type.

    ``etl.py`` already collapses every Dataspot label to a canonical small set
    (``text``, ``date``, ``datetime``, ``int``, ``double``, ``geo_point_2d``,
    ``geo_shape``). Going via the older ``_normalize_datatype_family`` was
    causing ``double`` / ``geo_shape`` overrides to silently fall back to
    ``text`` because those exact strings did not match any of its substring
    rules. This map is the single source of truth for the schema upsert.
    """
    normalized = clean_text(datentyp).lower()
    direct = {
        # etl.py-normalized canonical forms (the common case)
        "text": "text",
        "date": "date",
        "datetime": "datetime",
        "int": "int",
        "double": "double",
        "number": "double",
        "geo_point_2d": "geo_point_2d",
        "geo_shape": "geo_shape",
        "geometry": "geo_shape",
        "file": "file",
        "boolean": "boolean",
        # Raw Dataspot labels (defensive; may appear if a user pastes a
        # Dataspot value into the schema YAML manually)
        "datum": "date",
        "zeitpunkt": "datetime",
        "uhrzeit": "text",
        "ganzzahl": "int",
        "dezimalzahl": "double",
        "code": "text",
        "url": "text",
        "ja/nein": "boolean",
        "formatierter text": "text",
        "geometrie": "geo_shape",
        "geometrie (punkt)": "geo_point_2d",
        "geometrie (linie)": "geo_shape",
        "geometrie (fläche)": "geo_shape",
        "geometrie (flaeche)": "geo_shape",
    }
    if normalized in direct:
        return direct[normalized]
    family = _normalize_datatype_family(datentyp)
    return DEFAULT_FIELD_TYPE_BY_DATATYPE.get(family, "text")


def _load_schema_rows_from_yaml(huwise_id: str, dataspot_dataset_id: str) -> list[dict[str, str]]:
    """Load schema rows from merged ``data_orig/schema_files`` + ``data/schema_files``.

    ``etl.py`` produces the orig schema; users edit ``data/schema_files`` with
    ``dataspot_attribute``, ``technical_name`` (HUWISE), and ``export``. This
    function emits the row shape ``publish_dataset.py`` expects.

    Rules:

    - ``technical_name_dataspot`` = top-level ``technical_name`` (matches the
      column name in the locally downloaded GeoJSON).
    - ``technical_name_huwise`` = ``custom.technical_name`` when set, else
      the top-level ``technical_name`` (so ``custom.technical_name`` overrides
      are taken end-to-end and the rename actually reaches the data portal).
    - ``datatype`` carries the etl.py-normalized form. ``custom.datentyp``
      wins when set.
    - ``column_name`` / ``description`` / ``multivalued_separator`` follow
      the same "``custom.*`` wins when non-empty" rule.
    - Rows with ``export: false`` (e.g. ``gdh_fid``, ``map_links``) are
      dropped here so the rest of the publish pipeline can stay simple.
    """
    raw_payload = _resolve_merged_schema_payload(huwise_id, dataspot_dataset_id)
    if raw_payload is None:
        logging.warning(
            "No schema YAML found for huwise_id=%s dataspot_dataset_id=%s; "
            "publish will proceed with GeoJSON-derived columns only.",
            huwise_id,
            dataspot_dataset_id,
        )
        return []
    try:
        schema_model = _SchemaFileModel.model_validate(raw_payload)
    except ValidationError as exc:
        raise RuntimeError(
            f"Merged schema for huwise_id={huwise_id} dataspot_dataset_id={dataspot_dataset_id} "
            f"failed validation:\n{exc}"
        ) from exc

    rows: list[dict[str, str]] = []
    for item in schema_model.fields:
        technical_name = clean_text(item.technical_name)
        if not technical_name or not item.export:
            continue
        custom = item.custom
        custom_technical = clean_text(custom.technical_name)
        technical_name_huwise = custom_technical or technical_name
        datentyp_value = clean_text(custom.datentyp) or clean_text(item.datentyp) or "text"
        display_name = clean_text(custom.name) or clean_text(item.name)
        rows.append(
            {
                "technical_name_dataspot": technical_name,
                "technical_name_huwise": technical_name_huwise,
                "column_name": display_name or technical_name_huwise,
                "description": clean_text(custom.description) or clean_text(item.description),
                "datatype": datentyp_value,
                "multivalued_separator": clean_text(custom.mehrwertigkeit) or clean_text(item.mehrwertigkeit),
                "source": "schema_yaml",
            }
        )
    return rows


def _reconcile_schema_with_geojson(
    schema_rows: list[dict[str, str]],
    geojson_properties: list[str],
) -> list[dict[str, str]]:
    """Ensure schema contains all GeoJSON properties and preserve Dataspot labels."""
    by_technical_name: dict[str, dict[str, str]] = {}
    for row in schema_rows:
        for alias in (
            clean_text(row.get("technical_name_dataspot")),
            clean_text(row.get("technical_name_huwise")),
        ):
            if alias:
                by_technical_name[alias] = row
    for property_name in geojson_properties:
        if property_name in by_technical_name:
            continue
        by_technical_name[property_name] = {
            "technical_name_dataspot": property_name,
            "technical_name_huwise": property_name,
            "column_name": property_name,
            "description": "",
            "datatype": "Text",
            "multivalued_separator": "",
            "source": "geojson_fallback",
        }
    return [by_technical_name[name] for name in sorted(by_technical_name.keys(), key=str.lower)]


def _schema_rows_to_records(schema_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    """Normalize schema rows for HUWISE upsert without writing CSV files."""
    frame = pd.DataFrame(schema_rows, columns=SCHEMA_COLUMNS).fillna("")
    return frame.to_dict("records")


def _metadata_value_for_create(value: Any) -> dict[str, Any] | None:
    """Wrap a catalog value for the HUWISE create-dataset metadata payload."""
    if value is None:
        return None
    if isinstance(value, bool):
        return {"value": value}
    if isinstance(value, list):
        cleaned = [clean_text(item) for item in value if clean_text(item)]
        if not cleaned:
            return None
        return {"value": cleaned}
    text = clean_text(value)
    if not text:
        return None
    return {"value": text}


def _build_create_metadata_payload(ods_id: str, metadata_row: pd.Series) -> dict[str, Any]:
    """Seed critical metadata at create time so HUWISE template defaults do not win."""
    title = clean_text(_metadata_row_field(metadata_row, "default.title", "title")) or ods_id
    payload: dict[str, Any] = {
        "default": {
            "title": {"value": title},
            "language": {"value": "de"},
        },
        "internal": {
            "metadata_source_language": {"value": "de"},
        },
    }
    default_fields: dict[str, Any] = {
        "publisher": clean_text(_metadata_row_field(metadata_row, "default.publisher", "publisher")),
        "attributions": _coerce_string_list(metadata_row.get("default.attributions"))
        or list(DEFAULT_ATTRIBUTIONS),
    }
    for field_name, raw_value in default_fields.items():
        wrapped = _metadata_value_for_create(raw_value)
        if wrapped:
            payload["default"][field_name] = wrapped

    creator = clean_text(metadata_row.get("dcat.creator"))
    wrapped_creator = _metadata_value_for_create(creator)
    if wrapped_creator:
        payload["dcat"] = {"creator": wrapped_creator}

    publizierende = clean_text(
        _metadata_row_field(
            metadata_row,
            "custom.publizierende_organisation",
            "publizierende_organisation",
        )
    )
    wrapped_publizierende = _metadata_value_for_create(publizierende)
    if wrapped_publizierende:
        payload["custom"] = {"publizierende-organisation": wrapped_publizierende}
    return payload


def _ensure_huwise_dataset(ods_id: str, metadata_row: pd.Series) -> tuple[str | None, bool]:
    """Create dataset by ods_id if missing and return dataset UID + created flag."""
    try:
        return get_uid_by_id(dataset_id=ods_id), False
    except Exception:
        metadata_payload = _build_create_metadata_payload(ods_id, metadata_row)
        created = create_dataset(metadata=metadata_payload, dataset_id=ods_id, is_restricted=True)
        return created.uid, True


_TEMPLATE_FIELD_DEFINITIONS_CACHE: dict[str, set[str]] = {}


def _template_field_definitions(client: HttpClient, template_name: str) -> set[str]:
    """Portal-wide field definitions for one metadata template.

    Cached per process: the available field names on a template do not
    change per dataset, so hitting ``/metadata/templates/{template}/fields/``
    once per template (instead of once per (dataset, template)) saves a
    meaningful number of HUWISE roundtrips on a full run.
    """
    if template_name in _TEMPLATE_FIELD_DEFINITIONS_CACHE:
        return _TEMPLATE_FIELD_DEFINITIONS_CACHE[template_name]
    names: set[str] = set()
    try:
        payload = client.get(f"/metadata/templates/{template_name}/fields/").json()
    except Exception as exc:
        logging.warning("Could not fetch template definitions for %s: %s", template_name, exc)
        _TEMPLATE_FIELD_DEFINITIONS_CACHE[template_name] = names
        return names
    if isinstance(payload, dict):
        for item in payload.get("results", []):
            if isinstance(item, dict):
                name = clean_text(item.get("name"))
                if name:
                    names.add(name)
    _TEMPLATE_FIELD_DEFINITIONS_CACHE[template_name] = names
    return names


_METADATA_FIELDS_ALWAYS_WRITABLE = {
    ("default", "attributions"),
    ("custom", "publizierende_organisation"),
    ("custom", "geodaten_modellbeschreibung"),
}


def _set_metadata_fields(
    ods_id: str,
    metadata_row: pd.Series,
    source_url: str,
    *,
    metadata_last_push: dict[str, dict[str, Any]] | None = None,
    dataset_created: bool = False,
) -> None:
    """Set HUWISE metadata fields from the metadata table."""
    last_push_by_ods = metadata_last_push if metadata_last_push is not None else {}

    def _safe_set(action: str, callback: Any) -> None:
        try:
            callback()
        except Exception as exc:
            logging.warning("Failed metadata update '%s' for ods_id=%s: %s", action, ods_id, exc)

    dataset_uid = get_uid_by_id(dataset_id=ods_id)
    client = HttpClient(HuwiseConfig.from_env())
    try:
        all_templates_payload = client.get(f"/datasets/{dataset_uid}/metadata/").json()
    except Exception as exc:
        all_templates_payload = {"_error": clean_text(exc)}
    template_fields: dict[str, set[str]] = {}
    template_payloads: dict[str, dict[str, Any]] = {}
    for template_name in ("default", "internal", "dcat", "dcat_ap_ch", "custom"):
        try:
            payload = client.get(f"/datasets/{dataset_uid}/metadata/{template_name}/").json()
        except Exception:
            payload = {}
        template_fields[template_name] = set(payload.keys()) if isinstance(payload, dict) else set()
        template_payloads[template_name] = payload if isinstance(payload, dict) else {}
    # Ensure all actually available templates are discoverable for custom-field routing.
    if isinstance(all_templates_payload, dict):
        for template_name, payload in all_templates_payload.items():
            if not isinstance(payload, dict):
                continue
            template_fields[template_name] = set(payload.keys())
            template_payloads[template_name] = payload
    # Merge in portal-wide template field definitions (cached) so that fields
    # which are valid but currently unset on this specific dataset still show
    # up in the writable set.
    for template_name in list(template_fields.keys()):
        template_fields[template_name].update(_template_field_definitions(client, template_name))

    def _is_empty_value(value: Any) -> bool:
        extracted = _extract_metadata_value(value)
        if extracted is None:
            return True
        if isinstance(extracted, list):
            return len([item for item in extracted if clean_text(item)]) == 0
        return clean_text(extracted) == ""

    def _set_template_field(template: str, field: str, value: Any) -> None:
        resolved_template = template
        api_field = field
        # Route custom metadata fields to the real template namespace on this portal.
        if template == "custom" and field not in template_fields.get(template, set()):
            for candidate_name, candidate_fields in template_fields.items():
                if field in candidate_fields:
                    resolved_template = candidate_name
                    break
        if resolved_template == "custom":
            api_field = field.replace("_", "-")

        available_fields = template_fields.get(resolved_template, set())
        missing_in_template = (field not in available_fields) and (api_field not in available_fields)
        allow_missing_field_write = (resolved_template, field) in _METADATA_FIELDS_ALWAYS_WRITABLE
        if missing_in_template and not allow_missing_field_write:
            logging.info(
                "Skipping metadata field '%s.%s' for ods_id=%s because field is not available in dataset template",
                resolved_template,
                field,
                ods_id,
            )
            return
        existing = template_payloads.get(resolved_template, {}).get(field)
        if existing is None and api_field != field:
            existing = template_payloads.get(resolved_template, {}).get(api_field)
        if _is_empty_value(value):
            return
        snapshot_key = f"{resolved_template}.{field}"
        last_push = last_push_by_ods.get(ods_id, {}).get(snapshot_key)
        normalized_existing = _normalize_metadata_compare_value(existing)
        normalized_new = _normalize_metadata_compare_value(value)
        matches_last_push = last_push is not None and normalized_existing == _normalize_metadata_compare_value(
            last_push
        )
        publisher_existing = _normalize_metadata_compare_value(
            template_payloads.get("default", {}).get("publisher")
        )
        prefilled_as_publisher = (
            (resolved_template, field) == ("custom", "publizierende_organisation")
            and normalized_existing
            and publisher_existing
            and normalized_existing == publisher_existing
            and normalized_existing != normalized_new
        )
        can_write = (
            dataset_created
            or prefilled_as_publisher
            or _is_empty_value(existing)
            or (normalized_existing == normalized_new)
            or matches_last_push
        )
        if not can_write:
            return
        payload = {"value": value}
        client.put(f"/datasets/{dataset_uid}/metadata/{resolved_template}/{api_field}/", json=payload)
        entry = last_push_by_ods.setdefault(ods_id, {})
        entry[snapshot_key] = normalized_new
        last_push_by_ods[ods_id] = order_snapshot_entry(entry)

    stac_collection_id = _stac_collection_from_metadata_row(metadata_row)
    keyword_source = _metadata_row_field(metadata_row, "default.keyword", "keyword")
    keywords = [
        keyword
        for keyword in split_keywords(keyword_source)
        if clean_text(keyword).lower() != stac_collection_id.lower()
    ]
    tag_source = metadata_row.get("custom.tags")
    if isinstance(tag_source, list):
        extra_tags = [clean_text(tag) for tag in tag_source if clean_text(tag)]
    else:
        extra_tags = split_semicolon_list(tag_source) or split_semicolon_list(metadata_row.get("tags"))
    tags = [tag for tag in [DEFAULT_TAG, stac_collection_id, *extra_tags] if tag]
    deduped_tags = list(dict.fromkeys(tags))
    if keywords:
        _safe_set("keywords", lambda: _set_template_field("default", "keyword", keywords))
    _safe_set("custom_tags", lambda: _set_template_field("custom", "tags", deduped_tags))

    geo_ref = metadata_row.get("default.geographic_reference")
    if not isinstance(geo_ref, list) or not geo_ref:
        geo_ref = list(DEFAULT_GEOGRAPHIC_REFERENCE)

    static_fields: list[tuple[str, str, Any]] = [
        ("default", "title", clean_text(_metadata_row_field(metadata_row, "default.title", "title"))),
        (
            "default",
            "description",
            description_to_html(_metadata_row_field(metadata_row, "default.description", "description")),
        ),
        ("default", "language", clean_text(_metadata_row_field(metadata_row, "default.language")) or "de"),
        (
            "default",
            "attributions",
            _coerce_string_list(metadata_row.get("default.attributions")) or list(DEFAULT_ATTRIBUTIONS),
        ),
        ("default", "geographic_reference", geo_ref),
        (
            "default",
            "modified_updates_on_data_change",
            bool(metadata_row.get("default.modified_updates_on_data_change", False)),
        ),
        ("default", "modified", clean_text(metadata_row.get("default.modified"))),
        ("default", "modified_updates_on_metadata_change", False),
        (
            "default",
            "publisher",
            clean_text(_metadata_row_field(metadata_row, "default.publisher", "publisher")),
        ),
        (
            "custom",
            "publizierende_organisation",
            clean_text(
                _metadata_row_field(
                    metadata_row,
                    "custom.publizierende_organisation",
                    "publizierende_organisation",
                )
            ),
        ),
        (
            "custom",
            "geodaten_modellbeschreibung",
            clean_text(metadata_row.get("custom.geodaten_modellbeschreibung"))
            or clean_text(metadata_row.get("geodaten_modellbeschreibung")),
        ),
        ("dcat", "contact_name", DEFAULT_CONTACT_NAME),
        ("dcat", "contact_email", DEFAULT_CONTACT_EMAIL),
        ("dcat_ap_ch", "rights", DEFAULT_RIGHTS),
        ("dcat_ap_ch", "license", DEFAULT_LICENSE),
        ("dcat", "creator", clean_text(metadata_row.get("dcat.creator"))),
        ("dcat", "created", clean_text(metadata_row.get("dcat.created"))),
        ("dcat", "issued", clean_text(metadata_row.get("dcat.issued"))),
        ("dcat", "accrualperiodicity", clean_text(metadata_row.get("dcat.accrualperiodicity"))),
    ]
    for template, field, value in static_fields:
        _safe_set(
            f"{template}.{field}",
            lambda t=template, f=field, v=value: _set_template_field(t, f, v),
        )

    explicit_theme_ids = split_semicolon_list(_metadata_row_field(metadata_row, "internal.theme_id", "theme_ids"))
    if explicit_theme_ids:
        theme_ids = explicit_theme_ids
    else:
        theme_ids = []
        theme_text = clean_text(_metadata_row_field(metadata_row, "internal.theme", "theme"))
        if theme_text:
            resolved = _resolve_theme_id(theme_text)
            if resolved:
                theme_ids = [resolved]
            else:
                logging.warning("No known theme mapping for ods_id=%s theme=%s", ods_id, theme_text)
    if theme_ids:
        _safe_set("theme", lambda: _set_template_field("internal", "theme_id", theme_ids))
    license_id = clean_text(_metadata_row_field(metadata_row, "internal.license_id"))
    if license_id:
        _safe_set("license_id", lambda: _set_template_field("internal", "license_id", license_id))

    relation_source = metadata_row.get("dcat.relation")
    if isinstance(relation_source, list):
        relation_urls = [clean_text(url) for url in relation_source if clean_text(url)]
    else:
        relation_urls = split_semicolon_list(relation_source) or split_semicolon_list(metadata_row.get("relation_urls"))
    if relation_urls:
        _safe_set("relation", lambda: _set_template_field("dcat", "relation", relation_urls))


def _publish_huwise_dataset(huwise_id: str) -> None:
    """Publish the dataset on HUWISE so metadata and schema changes become visible."""
    logging.info("STEP publish_huwise huwise_id=%s", huwise_id)
    dataset_uid = get_uid_by_id(dataset_id=huwise_id)
    HuwiseDataset(uid=dataset_uid).publish()


def _normalize_datatype_family(datatype: str) -> str:
    """Map source datatype labels into logical datatype families."""
    normalized = normalize_name(datatype)
    if "geopoint2d" in normalized or "point" in normalized:
        return "geo_point_2d"
    if "datetime" in normalized:
        return "datetime"
    if "date" in normalized:
        return "date"
    if "ganzzahl" in normalized or re.fullmatch(r"int(eger)?", normalized):
        return "int"
    if "int" in normalized or "number" in normalized or "decimal" in normalized or "float" in normalized:
        return "number"
    if "bool" in normalized:
        return "boolean"
    if "file" in normalized or "document" in normalized:
        return "file"
    if "geometry" in normalized or "geometr" in normalized:
        return "geometry"
    return "text"


def _list_all_dataset_field_configurations(dataset_id: str) -> list[dict[str, Any]]:
    """Return all field configuration processors for a dataset (paginated API)."""
    all_results: list[dict[str, Any]] = []
    offset = 0
    while True:
        page = list_dataset_field_configurations(
            dataset_id=dataset_id,
            limit=_FIELD_CONFIG_PAGE_SIZE,
            offset=offset,
        )
        results = page.get("results", [])
        if isinstance(results, list):
            all_results.extend(item for item in results if isinstance(item, dict))
        if not results or not page.get("next"):
            break
        offset += _FIELD_CONFIG_PAGE_SIZE
    return all_results


def _is_managed_schema_processor(field: dict[str, Any]) -> bool:
    label = clean_text(field.get("label"))
    return bool(label.startswith(SCHEMA_PROCESSOR_LABEL_PREFIX))


def _count_managed_processors(fields: list[dict[str, Any]]) -> int:
    return sum(1 for field in fields if _is_managed_schema_processor(field))


def _expected_managed_processor_count(rows: list[dict[str, str]], accepted_types: set[str]) -> int:
    """Count processors this sync would create (mirrors the upsert create loop)."""
    total = 0
    for row in rows:
        technical_name = clean_text(row.get("technical_name_huwise"))
        if not technical_name:
            continue
        type_value = _build_field_type_value(row, accepted_types)
        if type_value is None:
            continue
        if clean_text(row.get("column_name")):
            total += 1
        total += 1
        if clean_text(row.get("description")):
            total += 1
        multivalued_separator = clean_text(row.get("multivalued_separator"))
        if multivalued_separator and type_value == "text":
            total += 1
    return total


def _schema_publish_tracking_paths(
    ods_id: str,
    dataspot_dataset_id: str,
    geojson_file: Path | None,
) -> list[Path]:
    paths: list[Path] = []
    if geojson_file is not None:
        paths.append(geojson_file)
    basename = resolve_schema_basename_for(ods_id, dataspot_dataset_id)
    if basename:
        orig = schema_orig_path(basename)
        user = schema_user_path(basename)
        if orig.exists():
            paths.append(orig)
        if user.exists():
            paths.append(user)
    return paths


def _schema_publish_inputs_changed(
    ods_id: str,
    dataspot_dataset_id: str,
    geojson_file: Path | None,
) -> bool:
    for path in _schema_publish_tracking_paths(ods_id, dataspot_dataset_id, geojson_file):
        if change_tracking.has_changed(str(path)):
            return True
    return False


def _update_schema_publish_tracking(
    ods_id: str,
    dataspot_dataset_id: str,
    geojson_file: Path | None,
) -> None:
    for path in _schema_publish_tracking_paths(ods_id, dataspot_dataset_id, geojson_file):
        change_tracking.update_hash_file(str(path))


def _discover_portal_field_types(dataset_ids: list[str]) -> set[str]:
    """Discover accepted field type values from existing field configurations."""
    discovered: set[str] = set()
    for dataset_id in dataset_ids:
        if not dataset_id:
            continue
        try:
            existing_fields = _list_all_dataset_field_configurations(dataset_id)
        except Exception:
            continue
        for field in existing_fields:
            field_type = clean_text(field.get("type"))
            if field_type:
                discovered.add(field_type)
        if discovered:
            break

    if discovered:
        return discovered

    # Fallback: scan first page of datasets and inspect any existing field configs.
    try:
        client = HttpClient(HuwiseConfig.from_env())
        response = client.get("/datasets/", params={"limit": 50})
        results = response.json().get("results", [])
        for item in results:
            uid = clean_text(item.get("uid"))
            if not uid:
                continue
            try:
                fields_response = client.get(f"/datasets/{uid}/fields/")
            except Exception:
                continue
            for field in fields_response.json().get("results", []):
                field_type = clean_text(field.get("type"))
                if field_type:
                    discovered.add(field_type)
            if discovered:
                break
    except Exception:
        return discovered
    return discovered


def _build_field_type_value(row: dict[str, str], accepted_types: set[str]) -> str | None:
    """Resolve HUWISE field type processor value from schema datatype.

    Uses :func:`_huwise_field_type_for_yaml_datentyp` so that the
    etl.py-normalized values (``double``, ``geo_shape``, ...) map directly
    to HUWISE types instead of being silently downgraded to ``text`` by
    the legacy substring rules in ``_normalize_datatype_family``.
    """
    raw = clean_text(row.get("datatype"))
    if not raw:
        return None
    # ``accepted_types`` is reserved for future field-type whitelisting; today
    # the helper returns ``None`` only when the datatype string itself is
    # empty, which still gates the upsert in ``_upsert_huwise_schema``.
    return _huwise_field_type_for_yaml_datentyp(raw)


def _extract_http_error_detail(exc: Exception) -> str:
    """Best-effort extraction of a HUWISE error body from an httpx exception."""
    response = getattr(exc, "response", None)
    if response is not None:
        try:
            body = response.text
        except Exception:
            body = ""
        if body:
            return f"{exc} | body={body[:600]}"
    return str(exc)


def _existing_field_type(existing_fields: list[dict[str, Any]], technical_name: str) -> str:
    """Return the HUWISE ``type`` value currently set on a data field, if any."""
    name_lower = technical_name.lower()
    for field in existing_fields:
        if not isinstance(field, dict):
            continue
        field_name = clean_text(field.get("name") or field.get("field"))
        if field_name.lower() != name_lower:
            continue
        ftype = clean_text(field.get("type"))
        if ftype:
            return ftype
    return ""


def _upsert_huwise_schema(
    ods_id: str,
    rows: list[dict[str, str]],
    accepted_types: set[str],
    *,
    dataspot_dataset_id: str = "",
    geojson_file: Path | None = None,
) -> None:
    """Upsert HUWISE field configurations from normalized schema rows."""
    if not accepted_types:
        logging.warning(
            "Skipping schema upsert for ods_id=%s because no accepted HUWISE field types were discovered",
            ods_id,
        )
        return

    try:
        existing_fields = _list_all_dataset_field_configurations(ods_id)
    except Exception as exc:
        logging.warning("Could not list field configurations for ods_id=%s: %s", ods_id, exc)
        return

    managed_count = _count_managed_processors(existing_fields)
    expected_count = _expected_managed_processor_count(rows, accepted_types)
    logging.info(
        "Listed %s field configurations for ods_id=%s (%s managed, %s expected)",
        len(existing_fields),
        ods_id,
        managed_count,
        expected_count,
    )

    inputs_changed = _schema_publish_inputs_changed(ods_id, dataspot_dataset_id, geojson_file)
    orphan_processors = managed_count > expected_count
    if not inputs_changed and not orphan_processors:
        logging.info("Skipping schema upsert for ods_id=%s (unchanged)", ods_id)
        return
    if orphan_processors and not inputs_changed:
        logging.info(
            "Force schema resync for ods_id=%s: managed processors (%s) exceed expected (%s)",
            ods_id,
            managed_count,
            expected_count,
        )

    dataset_uid = get_uid_by_id(dataset_id=ods_id)
    client = HttpClient(HuwiseConfig.from_env())

    # Remove previous processors created by this sync to keep run idempotent.
    for field in existing_fields:
        uid = clean_text(field.get("uid"))
        label = clean_text(field.get("label"))
        if uid and label.startswith(SCHEMA_PROCESSOR_LABEL_PREFIX):
            try:
                client.delete(f"/datasets/{dataset_uid}/fields/{uid}/")
            except Exception as exc:
                logging.warning("Could not delete managed processor ods_id=%s uid=%s: %s", ods_id, uid, exc)

    for row in rows:
        row["technical_name_huwise"] = clean_text(row.get("technical_name_huwise"))
        if not row["technical_name_huwise"]:
            logging.warning("Skipping field with empty HUWISE technical name for ods_id=%s", ods_id)
            continue
        technical_name = clean_text(row.get("technical_name_huwise"))
        # GeoJSON on FTP already uses ``technical_name_huwise`` column ids
        # (:func:`_prepare_geojson_wgs84`). Identity rename (from_name == to_name)
        # only sets the portal display label via ``field_label`` — safe unlike the
        # old Dataspot→HUWISE rename that broke resource processing.
        type_value = _build_field_type_value(row, accepted_types)
        if type_value is None:
            logging.warning(
                "Skipping field '%s' for ods_id=%s because no accepted HUWISE field type matched",
                technical_name,
                ods_id,
            )
            continue

        current_type = _existing_field_type(existing_fields, technical_name)
        if current_type and current_type != type_value:
            logging.error(
                "datentyp mismatch for ods_id=%s field=%s (current=%s, requested=%s); "
                "change the type manually in HUWISE or delete the field before re-publish",
                ods_id,
                technical_name,
                current_type,
                type_value,
            )

        processors: list[dict[str, Any]] = []
        field_label = clean_text(row.get("column_name"))
        if field_label:
            processors.append(
                {
                    "type": "rename",
                    "label": f"{SCHEMA_PROCESSOR_LABEL_PREFIX}: label {technical_name}",
                    "from_name": technical_name,
                    "to_name": technical_name,
                    "field_label": field_label,
                }
            )
        processors.append(
            {
                "type": "type",
                "label": f"{SCHEMA_PROCESSOR_LABEL_PREFIX}: type {technical_name}",
                "field": technical_name,
                "type_param": type_value,
            }
        )
        description = clean_text(row.get("description"))
        if description:
            processors.append(
                {
                    "type": "description",
                    "label": f"{SCHEMA_PROCESSOR_LABEL_PREFIX}: description {technical_name}",
                    "field": technical_name,
                    "description": description,
                }
            )
        # HUWISE ``annotate`` / ``multivalued`` (text fields only): separator from
        # schema ``mehrwertigkeit`` (Dataspot cardinality → ETL → YAML).
        multivalued_separator = clean_text(row.get("multivalued_separator"))
        if multivalued_separator and type_value == "text":
            processors.append(
                {
                    "type": "annotate",
                    "label": f"{SCHEMA_PROCESSOR_LABEL_PREFIX}: multivalued {technical_name}",
                    "field": technical_name,
                    "annotation": "multivalued",
                    "args": [multivalued_separator],
                }
            )

        for processor in processors:
            processor_type = clean_text(processor.get("type"))
            try:
                response = client.post(f"/datasets/{dataset_uid}/fields/", json=processor).json()
                logging.info(
                    "Created schema processor for ods_id=%s field=%s uid=%s type=%s",
                    ods_id,
                    technical_name,
                    clean_text(response.get("uid")),
                    processor_type,
                )
            except Exception as exc:
                # ``type`` failures are the silent killer of ``datentyp`` changes;
                # log them at ERROR level with the HUWISE response body. Other
                # processors (rename/description/annotate) get a WARNING.
                detail = _extract_http_error_detail(exc)
                if processor_type == "type":
                    logging.error(
                        "HUWISE rejected '%s' processor for ods_id=%s field=%s type=%s: %s",
                        processor_type,
                        ods_id,
                        technical_name,
                        processor.get("type_param"),
                        detail,
                    )
                else:
                    logging.warning(
                        "HUWISE rejected '%s' processor for ods_id=%s field=%s: %s",
                        processor_type,
                        ods_id,
                        technical_name,
                        detail,
                    )

    _update_schema_publish_tracking(ods_id, dataspot_dataset_id, geojson_file)


def _upload_geojson(local_file: Path) -> None:
    """Upload GeoJSON file to FTP destination folder."""
    logging.info("STEP upload_geojson file=%s", local_file.name)
    common.upload_ftp(str(local_file), remote_path=FTP_REMOTE_FOLDER)


def _schema_name_mapping(schema_rows: list[dict[str, str]]) -> dict[str, str]:
    """Build mapping from Dataspot technical names to HUWISE technical names."""
    mapping: dict[str, str] = {}
    for row in schema_rows:
        src = clean_text(row.get("technical_name_dataspot"))
        dst = clean_text(row.get("technical_name_huwise"))
        if not src or not dst or src == "geometry":
            continue
        mapping[src] = dst
    return mapping


def _sanitize_geojson_for_huwise(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Fix geometries and null-like property values before FTP upload."""
    if gdf.empty:
        return gdf
    out = gdf.copy()
    geometry_column = str(out.geometry.name) if hasattr(out, "geometry") else "geometry"
    if geometry_column in out.columns:
        valid_mask = out.geometry.notna()
        if valid_mask.any():
            invalid = valid_mask & (~out.geometry.is_valid)
            if invalid.any():
                try:
                    from shapely import make_valid

                    out.loc[invalid, geometry_column] = make_valid(out.loc[invalid, geometry_column])
                except Exception:
                    out.loc[invalid, geometry_column] = out.loc[invalid, geometry_column].buffer(0)
        out = out[valid_mask]
    for column in out.columns:
        if column == geometry_column:
            continue
        out[column] = out[column].where(out[column].notna(), None)
    return out


def _prepare_geojson_wgs84(local_file: Path, column_mapping: dict[str, str], allowed_fields: set[str]) -> Path:
    """Ensure uploaded GeoJSON uses EPSG:4326 and applies the schema rename."""
    gdf = _sanitize_geojson_for_huwise(gpd.read_file(local_file))
    # Resolve column matching case-insensitively so a Dataspot name like
    # ``ID_Block`` still finds a GeoJSON column named ``id_block`` (or vice
    # versa). The mapping value (HUWISE name) is preserved exactly.
    columns_by_lower = {str(column).lower(): str(column) for column in gdf.columns}
    applicable: dict[str, str] = {}
    skipped: list[str] = []
    for src, dst in column_mapping.items():
        if not dst:
            continue
        actual = columns_by_lower.get(str(src).lower())
        if actual is None:
            skipped.append(src)
            continue
        if actual != dst:
            applicable[actual] = dst
    if skipped:
        logging.info(
            "Skipped rename for %s columns absent from %s: %s",
            len(skipped),
            local_file.name,
            ", ".join(skipped),
        )
    inverse: dict[str, list[str]] = {}
    for src, dst in applicable.items():
        inverse.setdefault(dst, []).append(src)
    collisions = {dst: names for dst, names in inverse.items() if len(names) > 1}
    if collisions:
        raise ValueError(f"HUWISE field name collisions detected: {collisions}")
    if applicable:
        gdf = gdf.rename(columns=applicable)
        logging.info(
            "Renamed %s columns in %s: %s",
            len(applicable),
            local_file.name,
            ", ".join(f"{src} -> {dst}" for src, dst in sorted(applicable.items())),
        )
    before_drop_columns = [str(column) for column in gdf.columns]
    geometry_column = str(gdf.geometry.name) if hasattr(gdf, "geometry") else "geometry"
    keep_columns = [column for column in before_drop_columns if column in allowed_fields or column == geometry_column]
    if keep_columns:
        gdf = gdf[keep_columns]

    target = gdf
    if gdf.crs:
        if str(gdf.crs).upper() != "EPSG:4326":
            target = gdf.to_crs("EPSG:4326")
    else:
        bounds = gdf.total_bounds.tolist() if not gdf.empty else [0, 0, 0, 0]
        if bounds and abs(bounds[0]) > 180:
            gdf = gdf.set_crs("EPSG:2056", allow_override=True)
            target = gdf.to_crs("EPSG:4326")
        else:
            target = gdf.set_crs("EPSG:4326", allow_override=True)

    temp_dir = Path(tempfile.mkdtemp(prefix="fgi_wgs84_"))
    output = temp_dir / local_file.name
    target.to_file(output, driver="GeoJSON")
    return output


def _resource_payload(source_url: str, title: str, extractor_type: str = "geojson") -> dict[str, Any]:
    """Build Automation API payload for an HTTP resource."""
    parsed = urlparse(source_url)
    base_url = f"{parsed.scheme}://{parsed.netloc}"
    relative_url = parsed.path or "/"
    if parsed.query:
        relative_url = f"{relative_url}?{parsed.query}"
    return {
        "type": extractor_type,
        "title": title,
        "datasource": {
            "type": "http",
            "connection": {
                "type": "http",
                "url": base_url,
                "auth": None,
            },
            "headers": [],
            "relative_url": relative_url,
        },
    }


def _guess_resource_extractor(client: HttpClient, dataset_uid: str, source_url: str) -> str:
    """Guess extractor type for a URL; fallback to geojson."""
    payload = _resource_payload(source_url=source_url, title="tmp", extractor_type="geojson")
    try:
        response = client.post(
            f"/datasets/{dataset_uid}/resources/guess_extractors/",
            json={"datasource": payload["datasource"]},
        )
        results = response.json()
        if isinstance(results, list) and results:
            first = results[0]
            guessed = clean_text(first.get("type") if isinstance(first, dict) else "")
            if guessed:
                return guessed
    except Exception:
        return "geojson"
    return "geojson"


def _upsert_dataset_resource(ods_id: str, source_url: str) -> None:
    """Create/update HUWISE resource that points to the published GeoJSON URL."""
    logging.info("STEP upsert_resource huwise_id=%s", ods_id)
    dataset_uid = get_uid_by_id(dataset_id=ods_id)
    client = HttpClient(HuwiseConfig.from_env())
    try:
        existing = client.get(f"/datasets/{dataset_uid}/resources/").json().get("results", [])
    except Exception as exc:
        logging.warning("Could not list resources for ods_id=%s: %s", ods_id, exc)
        existing = []

    resource_title = f"{ods_id}.geojson"
    extractor_type = _guess_resource_extractor(client, dataset_uid=dataset_uid, source_url=source_url)
    payload = _resource_payload(source_url=source_url, title=resource_title, extractor_type=extractor_type)
    payload["title"] = resource_title

    matched_uid = ""
    for item in existing:
        title = clean_text(item.get("title"))
        datasource = item.get("datasource", {})
        relative_url = clean_text(datasource.get("relative_url"))
        connection_url = clean_text(datasource.get("connection", {}).get("url"))
        full_url = f"{connection_url}{relative_url}" if connection_url and relative_url else ""
        if title == resource_title or full_url == source_url or relative_url == urlparse(source_url).path:
            matched_uid = clean_text(item.get("uid"))
            break

    if matched_uid:
        try:
            client.put(f"/datasets/{dataset_uid}/resources/{matched_uid}/", json=payload)
            logging.info("Updated resource for ods_id=%s uid=%s", ods_id, matched_uid)
        except Exception as exc:
            logging.warning(
                "Failed to update resource for ods_id=%s uid=%s: %s. Trying create fallback.",
                ods_id,
                matched_uid,
                exc,
            )
            try:
                response = client.post(f"/datasets/{dataset_uid}/resources/", json=payload).json()
                logging.info("Created fallback resource for ods_id=%s uid=%s", ods_id, clean_text(response.get("uid")))
            except Exception as create_exc:
                logging.warning(
                    "Failed fallback resource create for ods_id=%s source=%s: %s",
                    ods_id,
                    source_url,
                    create_exc,
                )
    else:
        try:
            response = client.post(f"/datasets/{dataset_uid}/resources/", json=payload).json()
            logging.info("Created resource for ods_id=%s uid=%s", ods_id, clean_text(response.get("uid")))
        except Exception as exc:
            logging.warning("Failed to create resource for ods_id=%s source=%s: %s", ods_id, source_url, exc)


def _process_dataset(
    auth: DataspotAuth,
    pub_row: pd.Series,
    metadata_row: pd.Series,
    geojson_files: list[Path],
    accepted_types: set[str],
    *,
    metadata_last_push: dict[str, dict[str, Any]] | None = None,
) -> None:
    """Process one dataset from source extraction to HUWISE schema update."""
    logging.info("STEP publish_dataset huwise_id=%s", clean_text(pub_row.get("ods_id")))
    context = DatasetContext(
        ods_id=clean_text(pub_row.get("ods_id")),
        dataspot_dataset_id=clean_text(pub_row.get("id")),
        geo_dataset=clean_text(pub_row.get("geo_dataset")),
    )
    if not context.ods_id or not context.dataspot_dataset_id:
        logging.warning("Skipping row with missing ods_id or dataspot id")
        return

    _, dataset_created = _ensure_huwise_dataset(context.ods_id, metadata_row)

    geojson_file = _resolve_geojson_file(context, geojson_files)
    if geojson_file is None:
        logging.warning(
            "No local GeoJSON found for ods_id=%s (%s): resource/schema skipped, metadata still updated",
            context.ods_id,
            context.geo_dataset,
        )
        _set_metadata_fields(
            context.ods_id,
            metadata_row,
            source_url="",
            metadata_last_push=metadata_last_push,
            dataset_created=dataset_created,
        )
        _publish_huwise_dataset(context.ods_id)
        logging.info("Finished ods_id=%s (metadata only, no local GeoJSON)", context.ods_id)
        return

    schema_rows = _load_schema_rows_from_yaml(context.ods_id, context.dataspot_dataset_id)
    geojson_properties = read_geojson_properties(geojson_file)
    reconciled_schema = _reconcile_schema_with_geojson(schema_rows, geojson_properties)
    _validate_dataspot_schema_against_geometa(
        schema_rows=reconciled_schema,
        metadata_row=metadata_row,
        dataspot_uuid=context.dataspot_dataset_id,
        ods_id=context.ods_id,
    )
    rename_map = _schema_name_mapping(reconciled_schema)
    if rename_map:
        logging.info(
            "ods_id=%s renaming GeoJSON columns: %s",
            context.ods_id,
            ", ".join(f"{src} -> {dst}" for src, dst in sorted(rename_map.items()) if src != dst),
        )
    schema_records = _schema_rows_to_records(reconciled_schema)
    allowed_fields = {
        clean_text(row.get("technical_name_huwise"))
        for row in reconciled_schema
        if clean_text(row.get("technical_name_huwise"))
    }
    publish_geojson = _prepare_geojson_wgs84(
        geojson_file,
        _schema_name_mapping(reconciled_schema),
        allowed_fields=allowed_fields,
    )
    source_url = f"{SOURCE_URL_PREFIX}/{publish_geojson.name}"

    geojson_changed = change_tracking.has_changed(str(geojson_file))
    schema_inputs_changed = _schema_publish_inputs_changed(
        context.ods_id,
        context.dataspot_dataset_id,
        geojson_file,
    )
    if geojson_changed or schema_inputs_changed:
        _upload_geojson(publish_geojson)
        change_tracking.update_hash_file(str(geojson_file))
    else:
        logging.info("Skipping FTP upload for ods_id=%s (geojson and schema unchanged)", context.ods_id)
    if dataset_created:
        _upsert_dataset_resource(context.ods_id, source_url)
    else:
        logging.info("Skipping resource upsert for existing ods_id=%s", context.ods_id)
    _set_metadata_fields(
        context.ods_id,
        metadata_row,
        source_url,
        metadata_last_push=metadata_last_push,
        dataset_created=dataset_created,
    )
    logging.info("STEP publish_schema huwise_id=%s", context.ods_id)
    _upsert_huwise_schema(
        context.ods_id,
        schema_records,
        accepted_types=accepted_types,
        dataspot_dataset_id=context.dataspot_dataset_id,
        geojson_file=geojson_file,
    )
    _publish_huwise_dataset(context.ods_id)
    logging.info("Finished ods_id=%s", context.ods_id)


def _build_metadata_lookup(metadata_df: pd.DataFrame) -> dict[str, pd.Series]:
    """Build a map of metadata rows by ods_id."""
    lookup: dict[str, pd.Series] = {}
    for _, row in metadata_df.iterrows():
        ods_id = clean_text(row.get("ods_id"))
        if ods_id:
            lookup[ods_id] = row
    return lookup


def run(*, huwise_id_filter: str = "") -> None:
    """Publish active datasets to FTP and HUWISE."""
    logging.info("STEP publish start")
    auth = _build_dataspot_client()
    pub_df, metadata_df = _load_publish_inputs()
    active_ids = {clean_text(ods) for ods in pub_df.get("ods_id", []) if clean_text(ods)}
    metadata_lookup = _build_metadata_lookup(metadata_df)
    geojson_files = _build_geojson_index()
    metadata_last_push = _load_last_push_snapshot()
    metadata_last_push = {ods_id: fields for ods_id, fields in metadata_last_push.items() if ods_id in active_ids}
    accepted_field_types = _discover_portal_field_types([clean_text(value) for value in pub_df.get("ods_id", [])])
    if accepted_field_types:
        logging.info("Discovered HUWISE field types: %s", ", ".join(sorted(accepted_field_types)))
    else:
        logging.warning("Could not discover HUWISE field types; schema field upserts may be skipped")

    huwise_filter = clean_text(huwise_id_filter)
    for _, pub_row in pub_df.iterrows():
        ods_id = clean_text(pub_row.get("ods_id"))
        if huwise_filter and ods_id != huwise_filter:
            continue
        metadata_row = metadata_lookup.get(ods_id)
        if metadata_row is None:
            logging.warning("No metadata row found for ods_id=%s", ods_id)
            continue
        try:
            _process_dataset(
                auth=auth,
                pub_row=pub_row,
                metadata_row=metadata_row,
                geojson_files=geojson_files,
                accepted_types=accepted_field_types,
                metadata_last_push=metadata_last_push,
            )
        except Exception as exc:
            logging.error("Failed ods_id=%s: %s", ods_id, exc)

    catalog_snapshots = load_flat_publish_catalog()
    for ods_id in active_ids:
        catalog_entry = catalog_snapshots.get(ods_id, {})
        pushed_entry = metadata_last_push.get(ods_id, {})
        metadata_last_push[ods_id] = merge_snapshot_entries(catalog_entry, pushed_entry)
    prune_all_publish_artifacts(active_ids)
    _save_last_push_snapshot(metadata_last_push)
    logging.info("STEP publish done")
