"""Publish STAC GeoJSON datasets to FTP and HUWISE.

This script processes all datasets listed in ``data/pub_datasets.xlsx`` and performs:

1. Dataspot schema extraction for each source dataset.
2. Schema reconciliation against local GeoJSON properties.
3. Export of editable schema CSV files at ``data/datasets/{ods_id}.csv``.
4. GeoJSON upload to FTP (remote folder ``fgi/stac``).
5. HUWISE dataset create/reuse and metadata updates.
6. HUWISE schema upsert based on the generated schema CSV.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import argparse
import html
import json
import logging
import re
import tempfile
from urllib.parse import urlparse
from typing import Any

import geopandas as gpd
import pandas as pd
import requests

import common
from dataspot_auth import DataspotAuth
from huwise_utils_py import (
    HuwiseDataset,
    create_dataset,
    get_uid_by_id,
    list_dataset_field_configurations,
)
from huwise_utils_py.config import HuwiseConfig
from huwise_utils_py.http import HttpClient
from paths import DATASETS_DIR, DATA_DIR, SCHEMAS_DIR, ensure_output_dirs, resolve_input_file


logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

PUB_DATASETS_FILE = resolve_input_file("pub_datasets.xlsx")
METADATA_FILE = resolve_input_file("Metadata.csv")
PUBLISH_CATALOG_FILE = DATA_DIR / "publish_catalog.json"
STAC_INDEX_FILE = DATA_DIR / "stac_index.json"
DATASPOT_DATASET_URL = "https://bs.dataspot.io/rest/prod/datasets/{dataset_id}"
DATASPOT_COMPOSITIONS_URL = "https://bs.dataspot.io/rest/prod/datasets/{dataset_id}/compositions"
DATASPOT_ATTRIBUTE_URL = "https://bs.dataspot.io/rest/prod/attributes/{attribute_id}"
DATASPOT_DATATYPE_URL = "https://bs.dataspot.io/rest/prod/datatypes/{datatype_id}"
DATASPOT_CLASSIFIER_ATTRIBUTES_URL = "https://bs.dataspot.io/rest/prod/classifiers/{classifier_id}/attributes"
GEOMETA_PREVIEW_URL = "https://api.geo.bs.ch/geometa/v1/metadata_details/dataset/preview/html/{collection_id}"
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
    "number": "double",
    "boolean": "boolean",
    "geometry": "geo_shape",
    "text": "text",
}
SCHEMA_PROCESSOR_LABEL_PREFIX = "FGI schema sync"
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
DEFAULT_RIGHTS = "NonCommercialAllowed-CommercialAllowed-ReferenceRequired"
DEFAULT_LICENSE = "terms_by"
DEFAULT_CONTACT_NAME = "Open Data Basel-Stadt"
DEFAULT_CONTACT_EMAIL = "opendata@bs.ch"
DEFAULT_TAG = "opendata.swiss"
DEFAULT_GEOGRAPHIC_REFERENCE = ["ch_40_12"]
DEFAULT_LICENSE_ID = "cc-by"
DEFAULT_LICENSE_NAME = "CC BY 4.0"
DEFAULT_OVERRIDE_REMOTE_VALUE = True


@dataclass(frozen=True, slots=True)
class DatasetContext:
    """Minimal context for one dataset publishing run."""

    ods_id: str
    dataspot_dataset_id: str
    geo_dataset: str


def _clean_text(value: Any) -> str:
    """Return a stripped string representation."""
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    return str(value).strip()


def _normalize_name(value: str) -> str:
    """Normalize identifiers for fuzzy filename/field matching."""
    return re.sub(r"[^a-z0-9]", "", value.lower())


def _normalize_huwise_field_name(value: str) -> str:
    """Normalize a field name to HUWISE-compatible technical naming."""
    text = _clean_text(value).lower()
    text = re.sub(r"\s+", "_", text)
    text = re.sub(r"[^a-z0-9_]", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text


def _split_keywords(value: str) -> list[str]:
    """Parse metadata keyword cell into a list of non-empty keywords."""
    normalized = value.replace(";", ",")
    return [part.strip() for part in normalized.split(",") if part.strip()]


def _split_semicolon_list(value: Any) -> list[str]:
    """Convert a semicolon-delimited string (or list) to clean list values."""
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    text = _clean_text(value)
    if not text:
        return []
    return [part.strip() for part in text.split(";") if part.strip()]


def _extract_stac_code(value: Any) -> str:
    """Extract short STAC/Dataspot code (e.g. AFBA) from id/url-like values."""
    text = _clean_text(value)
    if not text:
        return ""
    tail = text.rstrip("/").split("/")[-1]
    if re.fullmatch(r"[A-Za-z0-9_]{3,16}", tail):
        return tail.upper()
    if re.fullmatch(r"[A-Za-z0-9_]{3,16}", text):
        return text.upper()
    return ""


_UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)


def _dataspot_uuid_from_catalog(dataset: dict[str, Any]) -> str:
    """Return Dataspot REST dataset UUID (not STAC collection code like AFBA)."""
    raw_id = _clean_text(dataset.get("dataspot_dataset_id"))
    if _UUID_RE.match(raw_id):
        return raw_id
    preview = _clean_text(dataset.get("html_preview"))
    if "#" in preview:
        frag = preview.split("#")[-1].strip()
        if _UUID_RE.match(frag):
            return frag
    return raw_id


def _resolve_dataspot_dataset_id(dataset: dict[str, Any]) -> str:
    """Resolve id for Dataspot compositions/schema: UUID when present, else legacy short code."""
    resolved = _dataspot_uuid_from_catalog(dataset)
    if resolved and _UUID_RE.match(resolved):
        return resolved
    raw_id = _clean_text(dataset.get("dataspot_dataset_id"))
    short_from_id = _extract_stac_code(raw_id)
    if short_from_id:
        return short_from_id
    short_from_preview = _extract_stac_code(dataset.get("html_preview"))
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


def _validate_publish_catalog(payload: dict[str, Any]) -> None:
    """Validate minimal publish catalog shape."""
    if not isinstance(payload, dict):
        raise ValueError("publish_catalog.json must contain a JSON object")
    datasets = payload.get("datasets")
    if not isinstance(datasets, list):
        raise ValueError("publish_catalog.json must contain a top-level 'datasets' list")
    for index, dataset in enumerate(datasets):
        if not isinstance(dataset, dict):
            raise ValueError(f"datasets[{index}] must be an object")
        required = ("ods_id", "dataspot_dataset_id", "geo_dataset", "title")
        missing = [key for key in required if not _clean_text(dataset.get(key))]
        if missing:
            raise ValueError(f"datasets[{index}] missing required keys: {', '.join(missing)}")


def _load_catalog_dataframes() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load JSON publish catalog and convert to pub/metadata dataframes."""
    payload = json.loads(PUBLISH_CATALOG_FILE.read_text(encoding="utf-8"))
    _validate_publish_catalog(payload)
    datasets = payload.get("datasets", [])

    pub_rows: list[dict[str, Any]] = []
    metadata_rows: list[dict[str, Any]] = []
    for dataset in datasets:
        ods_id = _clean_text(dataset.get("ods_id"))
        dataspot_id = _resolve_dataspot_dataset_id(dataset)
        geo_dataset = _clean_text(dataset.get("geo_dataset"))
        pub_rows.append(
            {
                "ods_id": ods_id,
                "id": dataspot_id,
                "geo_dataset": geo_dataset,
                "paket": _clean_text(dataset.get("paket")),
            }
        )

        relation_urls = _split_semicolon_list(dataset.get("relation_urls"))
        metadata_rows.append(
            {
                "ods_id": ods_id,
                "title": _clean_text(dataset.get("title")),
                "description": _clean_text(dataset.get("description")),
                "theme": ";".join(_split_semicolon_list(dataset.get("themes"))),
                "theme_ids": _split_semicolon_list(dataset.get("theme_ids")),
                "keyword": ";".join(_split_semicolon_list(dataset.get("keywords"))),
                "dcat_ap_ch.rights": _clean_text(dataset.get("dcat_ap_ch_rights")),
                "dcat_ap_ch.license": _clean_text(dataset.get("dcat_ap_ch_license")),
                "publizierende_organisation": _clean_text(dataset.get("publizierende_organisation")),
                "dcat.contact_name": _clean_text(dataset.get("dcat_contact_name")),
                "dcat.contact_email": _clean_text(dataset.get("dcat_contact_email")),
                "dcat.created": _clean_text(dataset.get("dcat_created")),
                "dcat.creator": _clean_text(dataset.get("dcat_creator")),
                "dcat.accrualperiodicity": _clean_text(dataset.get("dcat_accrualperiodicity")),
                "publisher": _clean_text(dataset.get("publisher")),
                "dcat.issued": _clean_text(dataset.get("dcat_issued")),
                "language": _clean_text(dataset.get("language")),
                "relation_urls": relation_urls,
                "tags": ";".join(_split_semicolon_list(dataset.get("tags"))),
                "custom.publizierende_organisation": _clean_text(dataset.get("publizierende_organisation")),
                "custom.geodaten_modellbeschreibung": _clean_text(dataset.get("geodaten_modellbeschreibung")),
            }
        )

    return pd.DataFrame(pub_rows), pd.DataFrame(metadata_rows)


def _build_dataspot_client() -> DataspotAuth:
    """Construct a Dataspot authentication client."""
    return DataspotAuth()


def _dataspot_get(auth: DataspotAuth, url: str, *, allow_404: bool = False) -> dict[str, Any] | None:
    """Execute an authenticated Dataspot GET call.

    Args:
        auth: Dataspot auth helper.
        url: Dataspot endpoint URL.
        allow_404: Whether missing resources should return ``None``.

    Returns:
        Parsed JSON object or ``None`` when ``allow_404=True`` and the resource
        does not exist.
    """
    response = requests.get(url=url, headers=auth.get_headers(), timeout=60)
    if response.status_code == 404 and allow_404:
        return None
    response.raise_for_status()
    return response.json()


def _load_pub_datasets() -> pd.DataFrame:
    """Load and normalize ``pub_datasets.xlsx``."""
    if not PUB_DATASETS_FILE.exists():
        raise FileNotFoundError(f"Missing file: {PUB_DATASETS_FILE}")

    frame = pd.read_excel(PUB_DATASETS_FILE).fillna("")
    for column in frame.columns:
        if pd.api.types.is_object_dtype(frame[column]):
            frame[column] = frame[column].astype(str).str.strip()
    return frame


def _load_metadata() -> pd.DataFrame:
    """Load and normalize ``Metadata.csv``."""
    if not METADATA_FILE.exists():
        raise FileNotFoundError(f"Missing file: {METADATA_FILE}")

    frame = pd.read_csv(METADATA_FILE, sep=";").fillna("")
    for column in frame.columns:
        if pd.api.types.is_object_dtype(frame[column]):
            frame[column] = frame[column].astype(str).str.strip()
    frame["ods_id"] = frame["ods_id"].astype(str).str.strip()
    frame["theme_ids"] = [[] for _ in range(len(frame))]
    relation_series = frame["dcat.relation"] if "dcat.relation" in frame.columns else pd.Series([""] * len(frame))
    frame["relation_urls"] = [
        [item.strip() for item in str(value).split(";") if item.strip()]
        for value in relation_series
    ]
    return frame


def _load_publish_inputs() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load publish inputs from JSON first, fallback to legacy files."""
    if PUBLISH_CATALOG_FILE.exists():
        pub_df, metadata_df = _load_catalog_dataframes()
        if len(pub_df) > 0:
            logging.info("Loaded %s datasets from %s", len(pub_df), PUBLISH_CATALOG_FILE)
            return pub_df, metadata_df
        logging.warning("Catalog %s is empty; falling back to legacy files", PUBLISH_CATALOG_FILE)
    return _load_pub_datasets(), _load_metadata()


def _extract_metadata_value(value: Any) -> Any:
    """Extract raw metadata values from HUWISE field payload shapes."""
    if isinstance(value, dict):
        if "value" in value:
            return _extract_metadata_value(value["value"])
        if "values" in value:
            return _extract_metadata_value(value["values"])
    return value


def _coerce_string_list(value: Any) -> list[str]:
    """Normalize a metadata value to a list of non-empty strings."""
    extracted = _extract_metadata_value(value)
    if isinstance(extracted, list):
        return [_clean_text(item) for item in extracted if _clean_text(item)]
    text = _clean_text(extracted)
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


def _sync_publish_catalog_from_huwise() -> None:
    """Refresh catalog metadata fields from HUWISE for all existing ods_id rows."""
    if not PUBLISH_CATALOG_FILE.exists():
        return
    payload = json.loads(PUBLISH_CATALOG_FILE.read_text(encoding="utf-8"))
    _validate_publish_catalog(payload)
    datasets = payload.get("datasets", [])
    if not datasets:
        return

    client = HttpClient(HuwiseConfig.from_env())
    theme_name_by_id = {theme_id: label for label, theme_id in THEME_MAP_DATA_BS_CH.items()}
    updated = 0

    for dataset in datasets:
        ods_id = _clean_text(dataset.get("ods_id"))
        if not ods_id:
            continue
        try:
            dataset_uid = get_uid_by_id(dataset_id=ods_id)
            templates = _fetch_metadata_templates(client, dataset_uid)
        except Exception as exc:
            logging.warning("Skipping HUWISE pull for ods_id=%s: %s", ods_id, exc)
            continue
        if not templates:
            continue

        title = _clean_text(_get_template_field(templates, "default", "title"))
        if title:
            dataset["title"] = title
        description = _clean_text(_get_template_field(templates, "default", "description"))
        if description:
            dataset["description"] = description
        publisher = _clean_text(_get_template_field(templates, "default", "publisher"))
        if publisher:
            dataset["publisher"] = publisher
        language = _clean_text(_get_template_field(templates, "default", "language"))
        if language:
            dataset["language"] = language
        keywords = _coerce_string_list(_get_template_field(templates, "default", "keyword"))
        if keywords:
            dataset["keywords"] = keywords

        theme_ids = _coerce_string_list(_get_template_field(templates, "internal", "theme_id"))
        if theme_ids:
            dataset["theme_ids"] = theme_ids
            dataset["themes"] = [theme_name_by_id.get(theme_id, theme_id) for theme_id in theme_ids]

        relation_urls = _coerce_string_list(_get_template_field(templates, "dcat", "relation"))
        if relation_urls:
            dataset["relation_urls"] = relation_urls
            if not _clean_text(dataset.get("html_preview")):
                dataset["html_preview"] = relation_urls[0]
        created = _clean_text(_get_template_field(templates, "dcat", "created"))
        if created:
            dataset["dcat_created"] = created
        creator = _clean_text(_get_template_field(templates, "dcat", "creator"))
        if creator:
            dataset["dcat_creator"] = creator
        issued = _clean_text(_get_template_field(templates, "dcat", "issued"))
        if issued:
            dataset["dcat_issued"] = issued
        contact_name = _clean_text(_get_template_field(templates, "dcat", "contact_name"))
        if contact_name:
            dataset["dcat_contact_name"] = contact_name
        contact_email = _clean_text(_get_template_field(templates, "dcat", "contact_email"))
        if contact_email:
            dataset["dcat_contact_email"] = contact_email
        accrual = _clean_text(_get_template_field(templates, "dcat", "accrualperiodicity"))
        if accrual:
            dataset["dcat_accrualperiodicity"] = accrual

        rights = _clean_text(_get_template_field(templates, "dcat_ap_ch", "rights"))
        if rights:
            dataset["dcat_ap_ch_rights"] = rights
        license_name = _clean_text(_get_template_field(templates, "dcat_ap_ch", "license"))
        if license_name:
            dataset["dcat_ap_ch_license"] = license_name
        updated += 1

    temp_path = PUBLISH_CATALOG_FILE.with_suffix(".json.tmp")
    temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temp_path.replace(PUBLISH_CATALOG_FILE)
    logging.info("Refreshed publish catalog metadata from HUWISE for %s datasets", updated)


def _build_geojson_index() -> list[Path]:
    """Return all local GeoJSON files in the datasets folder."""
    if not DATASETS_DIR.exists():
        raise FileNotFoundError(f"Missing datasets folder: {DATASETS_DIR}")
    return sorted(DATASETS_DIR.glob("*.geojson"))


def _schema_json_path(ods_id: str) -> Path:
    """Return the canonical schema JSON file path for one dataset."""
    return SCHEMAS_DIR / f"{ods_id}.schema.json"


def _resolve_geojson_file(context: DatasetContext, candidates: list[Path]) -> Path | None:
    """Resolve the local GeoJSON for the given dataset context."""
    normalized_geo_dataset = _normalize_name(context.geo_dataset)
    for candidate in candidates:
        stem_normalized = _normalize_name(candidate.stem)
        if stem_normalized.endswith(normalized_geo_dataset):
            return candidate

    for candidate in candidates:
        stem_normalized = _normalize_name(candidate.stem)
        if normalized_geo_dataset in stem_normalized:
            return candidate

    return None


def _read_geojson_properties(geojson_file: Path) -> list[str]:
    """Read top-level property keys from the first feature."""
    payload = json.loads(geojson_file.read_text(encoding="utf-8"))
    features = payload.get("features", [])
    if not features:
        return []
    properties = features[0].get("properties", {})
    return [str(key) for key in properties.keys()]


def _geometa_collection_code_from_metadata(metadata_row: pd.Series) -> str:
    relation_urls = _split_semicolon_list(metadata_row.get("relation_urls"))
    for url in relation_urls:
        marker = "/metadata_details/dataset/preview/html/"
        if marker in url:
            tail = url.split(marker, 1)[-1]
            code = tail.split("#", 1)[0].strip().strip("/")
            if code:
                return code
    return ""


def _fetch_geometa_attribute_technical_names(collection_id: str, dataspot_uuid: str) -> set[str]:
    if not collection_id or not dataspot_uuid:
        return set()
    response = requests.get(GEOMETA_PREVIEW_URL.format(collection_id=collection_id), timeout=60)
    response.raise_for_status()
    html = response.text
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
        value = _clean_text(re.sub(r"<[^>]+>", "", cells[1]))
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
    dataspot_names = {_clean_text(row.get("technical_name_dataspot")) for row in schema_rows if _clean_text(row.get("technical_name_dataspot"))}
    missing_in_dataspot = sorted(name for name in geometa_names if name not in dataspot_names)
    if missing_in_dataspot:
        logging.warning(
            "Geometa/Dataspot schema mismatch for ods_id=%s (%s#%s). Missing in Dataspot fetch: %s",
            ods_id,
            collection_id,
            dataspot_uuid,
            ", ".join(missing_in_dataspot[:10]),
        )


def _fetch_dataspot_schema_rows(auth: DataspotAuth, dataspot_dataset_id: str) -> list[dict[str, str]]:
    def _attribute_technical_name(attribute: dict[str, Any]) -> str:
        technical = _clean_text(attribute.get("title"))
        if technical:
            return technical
        composed_by_href = _clean_text(attribute.get("_links", {}).get("composedBy", {}).get("href"))
        if composed_by_href:
            if composed_by_href.startswith("/"):
                composed_by_href = f"https://bs.dataspot.io{composed_by_href}"
            composition_payload = _dataspot_get(auth, composed_by_href, allow_404=True)
            if composition_payload is not None:
                technical = _clean_text(composition_payload.get("title")) or _clean_text(composition_payload.get("label"))
                if technical:
                    return technical
        return _clean_text(attribute.get("label"))

    """Fetch schema rows from Dataspot, preferring classifier attributes via dataset domain."""
    compositions_data = _dataspot_get(auth, DATASPOT_COMPOSITIONS_URL.format(dataset_id=dataspot_dataset_id))
    compositions = compositions_data.get("_embedded", {}).get("compositions", [])

    classifier_id = ""
    for composition in compositions:
        attribute_id = _clean_text(composition.get("composedOf"))
        if not attribute_id:
            continue
        attribute_payload = _dataspot_get(auth, DATASPOT_ATTRIBUTE_URL.format(attribute_id=attribute_id), allow_404=True)
        if not attribute_payload:
            continue
        classifier_id = _clean_text(attribute_payload.get("hasDomain"))
        if classifier_id:
            break

    if classifier_id:
        classifier_attributes_payload = _dataspot_get(
            auth,
            DATASPOT_CLASSIFIER_ATTRIBUTES_URL.format(classifier_id=classifier_id),
            allow_404=True,
        )
        classifier_attributes = classifier_attributes_payload.get("_embedded", {}).get("attributes", []) if classifier_attributes_payload else []
        rows: list[dict[str, str]] = []
        for attribute in classifier_attributes:
            datatype_label = ""
            datatype_id = _clean_text(attribute.get("hasRange"))
            if datatype_id:
                datatype = _dataspot_get(auth, DATASPOT_DATATYPE_URL.format(datatype_id=datatype_id), allow_404=True)
                if datatype is not None:
                    datatype_label = _clean_text(datatype.get("label")) or _clean_text(datatype.get("title"))

            technical_name = _attribute_technical_name(attribute)
            if "geometr" in _normalize_name(datatype_label) and _normalize_name(technical_name) in {"geometrie", "geometry"}:
                technical_name = "geometry"
            if not technical_name:
                continue
            rows.append(
                {
                    "technical_name_dataspot": technical_name,
                    "technical_name_huwise": _normalize_huwise_field_name(technical_name) or technical_name,
                    "column_name": _clean_text(attribute.get("label")) or technical_name,
                    "description": _clean_text(attribute.get("description")),
                    "datatype": datatype_label or "Text",
                    "multivalued_separator": "",
                    "source": "dataspot_classifier",
                }
            )
        if rows:
            return rows

    rows: list[dict[str, str]] = []
    for composition in compositions:
        attribute_id = _clean_text(composition.get("composedOf"))
        if not attribute_id:
            continue
        attribute_payload = _dataspot_get(auth, DATASPOT_ATTRIBUTE_URL.format(attribute_id=attribute_id))
        if attribute_payload is None:
            continue
        attribute = attribute_payload
        datatype_label = ""
        datatype_id = _clean_text(attribute.get("hasRange"))
        if datatype_id:
            datatype = _dataspot_get(auth, DATASPOT_DATATYPE_URL.format(datatype_id=datatype_id), allow_404=True)
            if datatype is not None:
                datatype_label = _clean_text(datatype.get("label")) or _clean_text(datatype.get("title"))

        technical_name = _clean_text(composition.get("title"))
        if not technical_name:
            technical_name = _clean_text(composition.get("label"))
        if "geometr" in _normalize_name(datatype_label) and _normalize_name(technical_name) in {"geometrie", "geometry"}:
            technical_name = "geometry"
        if not technical_name:
            continue

        rows.append(
            {
                "technical_name_dataspot": technical_name,
                "technical_name_huwise": _normalize_huwise_field_name(technical_name) or technical_name,
                "column_name": _clean_text(composition.get("label")) or technical_name,
                "description": _clean_text(composition.get("description"))
                or _clean_text(attribute.get("description")),
                "datatype": datatype_label or "Text",
                "multivalued_separator": "",
                "source": "dataspot",
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
        source_name = _clean_text(row.get("technical_name_dataspot"))
        if not source_name:
            source_name = _clean_text(row.get("technical_name_huwise"))
        if not source_name:
            continue
        by_technical_name[source_name] = row
    for property_name in geojson_properties:
        if property_name in by_technical_name:
            continue
        by_technical_name[property_name] = {
            "technical_name_dataspot": property_name,
            "technical_name_huwise": _normalize_huwise_field_name(property_name) or property_name,
            "column_name": property_name,
            "description": "",
            "datatype": "Text",
            "multivalued_separator": "",
            "source": "geojson_fallback",
        }
    return [by_technical_name[name] for name in sorted(by_technical_name.keys(), key=str.lower)]


def _write_schema_file(ods_id: str, schema_rows: list[dict[str, str]]) -> Path:
    """Write per-dataset editable schema CSV."""
    output_path = DATASETS_DIR / f"{ods_id}.csv"
    frame = pd.DataFrame(schema_rows, columns=SCHEMA_COLUMNS)
    frame.to_csv(output_path, index=False, encoding="utf-8")
    return output_path


def _write_schema_json(ods_id: str, schema_rows: list[dict[str, str]]) -> Path:
    """Write editable per-dataset schema JSON."""
    SCHEMAS_DIR.mkdir(parents=True, exist_ok=True)
    output_path = _schema_json_path(ods_id)
    payload = {"ods_id": ods_id, "fields": schema_rows}
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return output_path


def _load_schema_rows(ods_id: str, schema_json: Path) -> list[dict[str, str]]:
    """Load schema rows from JSON; fallback to empty list on malformed payload."""
    payload = json.loads(schema_json.read_text(encoding="utf-8"))
    rows = payload.get("fields", [])
    if not isinstance(rows, list):
        return []
    normalized: list[dict[str, str]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        normalized.append(
            {
                "technical_name_dataspot": _clean_text(row.get("technical_name_dataspot"))
                or _clean_text(row.get("technical_name")),
                "technical_name_huwise": _normalize_huwise_field_name(
                    _clean_text(row.get("technical_name_huwise")) or _clean_text(row.get("technical_name"))
                ),
                "column_name": _clean_text(row.get("column_name")),
                "description": _clean_text(row.get("description")),
                "datatype": _clean_text(row.get("datatype")) or "Text",
                "multivalued_separator": _clean_text(row.get("multivalued_separator")),
                "source": _clean_text(row.get("source")) or "manual",
            }
        )
    if not normalized:
        logging.warning("Schema JSON for ods_id=%s is empty or invalid: %s", ods_id, schema_json)
    return normalized


def _ensure_huwise_dataset(ods_id: str, metadata_row: pd.Series, dry_run: bool) -> str | None:
    """Create dataset by ods_id if missing and return dataset UID."""
    if dry_run:
        logging.info("[dry-run] Would ensure HUWISE dataset for ods_id=%s", ods_id)
        return None

    try:
        return get_uid_by_id(dataset_id=ods_id)
    except Exception:
        # Keep create payload minimal as recommended by huwise-utils-py docs.
        # Additional metadata is set afterwards via dedicated setter calls.
        title = _clean_text(metadata_row.get("title")) or ods_id
        metadata_payload = {
            "default": {
                "title": {"value": title},
            },
        }
        created = create_dataset(metadata=metadata_payload, dataset_id=ods_id, is_restricted=True)
        return created.uid


def _ensure_dataset_restricted(ods_id: str, dry_run: bool) -> None:
    """Force dataset visibility to restricted for existing datasets."""
    if dry_run:
        logging.info("[dry-run] Would enforce restricted visibility for ods_id=%s", ods_id)
        return
    dataset_uid = get_uid_by_id(dataset_id=ods_id)
    client = HttpClient(HuwiseConfig.from_env())
    try:
        client.patch(f"/datasets/{dataset_uid}/", json={"is_restricted": True})
        return
    except Exception:
        pass
    try:
        current = client.get(f"/datasets/{dataset_uid}/").json()
        current["is_restricted"] = True
        client.put(f"/datasets/{dataset_uid}/", json=current)
    except Exception as exc:
        logging.warning("Could not enforce restricted visibility for ods_id=%s: %s", ods_id, exc)


def _description_to_huwise_html(value: Any) -> str:
    """Return HTML for HUWISE description while preserving existing HTML input."""
    text = _clean_text(value)
    if not text:
        return ""
    if re.search(r"<[a-zA-Z][^>]*>", text):
        return text
    paragraphs = [part.strip() for part in re.split(r"\n\s*\n", text) if part.strip()]
    if not paragraphs:
        return html.escape(text).replace("\n", "<br>")
    return "\n".join(f"<p>{html.escape(part).replace('\n', '<br>')}</p>" for part in paragraphs)


def _set_metadata_fields(ods_id: str, metadata_row: pd.Series, source_url: str, dry_run: bool) -> None:
    """Set HUWISE metadata fields from the metadata table."""
    if dry_run:
        logging.info("[dry-run] Would update metadata for ods_id=%s", ods_id)
        return

    def _safe_set(action: str, callback: Any) -> None:
        try:
            callback()
        except Exception as exc:
            logging.warning("Failed metadata update '%s' for ods_id=%s: %s", action, ods_id, exc)

    dataset_uid = get_uid_by_id(dataset_id=ods_id)
    dataset = HuwiseDataset(uid=dataset_uid)
    client = HttpClient(HuwiseConfig.from_env())
    template_fields: dict[str, set[str]] = {}
    for template_name in ("default", "internal", "dcat", "dcat_ap_ch", "custom"):
        try:
            payload = client.get(f"/datasets/{dataset_uid}/metadata/{template_name}/").json()
        except Exception:
            payload = {}
        template_fields[template_name] = set(payload.keys()) if isinstance(payload, dict) else set()

    def _set_template_field(template: str, field: str, value: Any, *, publish: bool = False) -> None:
        if field not in template_fields.get(template, set()):
            logging.info(
                "Skipping metadata field '%s.%s' for ods_id=%s because field is not available in dataset template",
                template,
                field,
                ods_id,
            )
            return
        try:
            dataset._set_metadata_value(
                template,
                field,
                value,
                publish=publish,
                override_remote_value=DEFAULT_OVERRIDE_REMOTE_VALUE,
            )
            return
        except TypeError:
            payload = {"value": value, "override_remote_value": DEFAULT_OVERRIDE_REMOTE_VALUE}
            client.put(f"/datasets/{dataset_uid}/metadata/{template}/{field}/", json=payload)
            if publish:
                dataset.publish()

    _safe_set("title", lambda: _set_template_field("default", "title", _clean_text(metadata_row.get("title")), publish=False))
    _safe_set(
        "description",
        lambda: _set_template_field(
            "default",
            "description",
            _description_to_huwise_html(metadata_row.get("description")),
            publish=False,
        ),
    )
    keywords = _split_keywords(_clean_text(metadata_row.get("keyword")))
    extra_tags = _split_semicolon_list(metadata_row.get("tags"))
    for tag in extra_tags:
        if tag not in keywords:
            keywords.append(tag)
    if DEFAULT_TAG not in keywords:
        keywords.append(DEFAULT_TAG)
    if keywords:
        _safe_set("keywords", lambda: _set_template_field("default", "keyword", keywords, publish=False))
    tags = [tag for tag in [*extra_tags, DEFAULT_TAG] if tag]
    if tags:
        deduped_tags = list(dict.fromkeys(tags))
        _safe_set("tags", lambda: _set_template_field("default", "tags", deduped_tags, publish=False))

    explicit_theme_ids = _split_semicolon_list(metadata_row.get("theme_ids"))
    if explicit_theme_ids:
        theme_ids = explicit_theme_ids
    else:
        theme_ids = []
        theme_text = _clean_text(metadata_row.get("theme"))
        if theme_text:
            resolved = _resolve_theme_id(theme_text)
            if resolved:
                theme_ids = [resolved]
            else:
                logging.warning("No known theme mapping for ods_id=%s theme=%s", ods_id, theme_text)
    if theme_ids:
        _safe_set("theme", lambda: _set_template_field("internal", "theme_id", theme_ids, publish=False))

    _safe_set("language", lambda: _set_template_field("default", "language", "de", publish=False))
    _safe_set("geographic_reference", lambda: _set_template_field("default", "geographic_reference", DEFAULT_GEOGRAPHIC_REFERENCE, publish=False))
    _safe_set(
        "modified_auto",
        lambda: _set_template_field("default", "modified_updates_on_data_change", True, publish=False),
    )
    _safe_set(
        "modified_manual",
        lambda: _set_template_field("default", "modified_updates_on_metadata_change", False, publish=False),
    )
    _safe_set(
        "publisher",
        lambda: _set_template_field("default", "publisher", _clean_text(metadata_row.get("publisher")), publish=False),
    )
    _safe_set(
        "custom_publizierende_organisation",
        lambda: _set_template_field(
            "custom",
            "publizierende_organisation",
            _clean_text(metadata_row.get("custom.publizierende_organisation"))
            or _clean_text(metadata_row.get("publizierende_organisation")),
            publish=False,
        ),
    )
    _safe_set(
        "custom_geodaten_modellbeschreibung",
        lambda: _set_template_field(
            "custom",
            "geodaten_modellbeschreibung",
            _clean_text(metadata_row.get("custom.geodaten_modellbeschreibung"))
            or _clean_text(metadata_row.get("geodaten_modellbeschreibung")),
            publish=False,
        ),
    )
    _safe_set(
        "contact_name",
        lambda: _set_template_field("dcat", "contact_name", DEFAULT_CONTACT_NAME, publish=False),
    )
    _safe_set(
        "contact_email",
        lambda: _set_template_field("dcat", "contact_email", DEFAULT_CONTACT_EMAIL, publish=False),
    )
    _safe_set(
        "dcat_rights",
        lambda: _set_template_field("dcat_ap_ch", "rights", DEFAULT_RIGHTS, publish=False),
    )
    _safe_set(
        "dcat_license",
        lambda: _set_template_field("dcat_ap_ch", "license", DEFAULT_LICENSE, publish=False),
    )
    _safe_set("creator", lambda: _set_template_field("dcat", "creator", _clean_text(metadata_row.get("dcat.creator")), publish=False))
    _safe_set("created", lambda: _set_template_field("dcat", "created", _clean_text(metadata_row.get("dcat.created")), publish=False))
    _safe_set("issued", lambda: _set_template_field("dcat", "issued", _clean_text(metadata_row.get("dcat.issued")), publish=False))
    _safe_set(
        "accrualperiodicity",
        lambda: _set_template_field("dcat", "accrualperiodicity", _clean_text(metadata_row.get("dcat.accrualperiodicity")), publish=False),
    )
    relation_urls = _split_semicolon_list(metadata_row.get("relation_urls"))
    if source_url and source_url not in relation_urls:
        relation_urls.insert(0, source_url)
    if relation_urls:
        _safe_set("relation", lambda: _set_template_field("dcat", "relation", relation_urls, publish=True))


def _normalize_datatype_family(datatype: str) -> str:
    """Map source datatype labels into logical datatype families."""
    normalized = _normalize_name(datatype)
    if "date" in normalized:
        return "date"
    if "int" in normalized or "number" in normalized or "decimal" in normalized or "float" in normalized:
        return "number"
    if "bool" in normalized:
        return "boolean"
    if "geometry" in normalized or "geometr" in normalized:
        return "geometry"
    return "text"


def _discover_portal_field_types(dataset_ids: list[str]) -> set[str]:
    """Discover accepted field type values from existing field configurations."""
    discovered: set[str] = set()
    for dataset_id in dataset_ids:
        if not dataset_id:
            continue
        try:
            payload = list_dataset_field_configurations(dataset_id=dataset_id)
        except Exception:
            continue
        for field in payload.get("results", []):
            field_type = _clean_text(field.get("type"))
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
            uid = _clean_text(item.get("uid"))
            if not uid:
                continue
            try:
                fields_response = client.get(f"/datasets/{uid}/fields/")
            except Exception:
                continue
            for field in fields_response.json().get("results", []):
                field_type = _clean_text(field.get("type"))
                if field_type:
                    discovered.add(field_type)
            if discovered:
                break
    except Exception:
        return discovered
    return discovered


def _build_field_type_value(row: dict[str, str], accepted_types: set[str]) -> str | None:
    """Resolve HUWISE field type processor value from schema datatype."""
    family = _normalize_datatype_family(row["datatype"])
    preferred_type = DEFAULT_FIELD_TYPE_BY_DATATYPE[family]
    # NOTE: accepted_types discovered from /datasets/{uid}/fields are processor
    # types (rename/type/annotate...), not the `type_param` enum; use stable map.
    return preferred_type


def _upsert_huwise_schema(ods_id: str, schema_csv: Path, dry_run: bool, accepted_types: set[str]) -> None:
    """Upsert HUWISE field configurations from the schema CSV."""
    rows = pd.read_csv(schema_csv).fillna("").to_dict("records")
    if dry_run:
        logging.info("[dry-run] Would upsert %s schema fields for ods_id=%s", len(rows), ods_id)
        return
    if not accepted_types:
        logging.warning(
            "Skipping schema upsert for ods_id=%s because no accepted HUWISE field types were discovered",
            ods_id,
        )
        return

    existing = list_dataset_field_configurations(dataset_id=ods_id)
    existing_fields = existing.get("results", [])
    dataset_uid = get_uid_by_id(dataset_id=ods_id)
    client = HttpClient(HuwiseConfig.from_env())

    # Remove previous processors created by this sync to keep run idempotent.
    for field in existing_fields:
        uid = _clean_text(field.get("uid"))
        label = _clean_text(field.get("label"))
        if uid and label.startswith(SCHEMA_PROCESSOR_LABEL_PREFIX):
            try:
                client.delete(f"/datasets/{dataset_uid}/fields/{uid}/")
            except Exception as exc:
                logging.warning("Could not delete managed processor ods_id=%s uid=%s: %s", ods_id, uid, exc)

    for row in rows:
        row["technical_name_huwise"] = _normalize_huwise_field_name(_clean_text(row.get("technical_name_huwise")))
        if not row["technical_name_huwise"]:
            logging.warning("Skipping field with empty HUWISE technical name for ods_id=%s", ods_id)
            continue
        technical_name = _clean_text(row.get("technical_name_huwise"))
        from_name = _clean_text(row.get("technical_name_dataspot")) or technical_name
        type_value = _build_field_type_value(row, accepted_types)
        if type_value is None:
            logging.warning(
                "Skipping field '%s' for ods_id=%s because no accepted HUWISE field type matched",
                technical_name,
                ods_id,
            )
            continue
        processors: list[dict[str, Any]] = [
            {
                "type": "rename",
                "label": f"{SCHEMA_PROCESSOR_LABEL_PREFIX}: rename {from_name}",
                "from_name": from_name,
                "to_name": technical_name,
                "field_label": _clean_text(row.get("column_name")) or technical_name,
            },
            {
                "type": "type",
                "label": f"{SCHEMA_PROCESSOR_LABEL_PREFIX}: type {technical_name}",
                "field": technical_name,
                "type_param": type_value,
            },
        ]
        description = _clean_text(row.get("description"))
        if description:
            processors.append(
                {
                    "type": "description",
                    "label": f"{SCHEMA_PROCESSOR_LABEL_PREFIX}: description {technical_name}",
                    "field": technical_name,
                    "description": description,
                }
            )
        multivalued_separator = _clean_text(row.get("multivalued_separator"))
        if multivalued_separator:
            processors.append(
                {
                    "type": "annotate",
                    "label": f"{SCHEMA_PROCESSOR_LABEL_PREFIX}: multivalued {technical_name}",
                    "field": technical_name,
                    "annotation": "multivalued",
                    "args": [multivalued_separator],
                }
            )

        try:
            for processor in processors:
                response = client.post(f"/datasets/{dataset_uid}/fields/", json=processor).json()
                logging.info(
                    "Created schema processor for ods_id=%s field=%s uid=%s type=%s",
                    ods_id,
                    technical_name,
                    _clean_text(response.get("uid")),
                    _clean_text(processor.get("type")),
                )
        except Exception as exc:
            logging.warning(
                "Skipping schema processor upsert for ods_id=%s field=%s: %s",
                ods_id,
                technical_name,
                exc,
            )
            continue


def _upload_geojson(local_file: Path, dry_run: bool) -> None:
    """Upload GeoJSON file to FTP destination folder."""
    if dry_run:
        logging.info("[dry-run] Would upload %s to FTP folder %s", local_file.name, FTP_REMOTE_FOLDER)
        return
    common.upload_ftp(str(local_file), remote_path=FTP_REMOTE_FOLDER)


def _schema_name_mapping(schema_rows: list[dict[str, str]]) -> dict[str, str]:
    """Build mapping from Dataspot technical names to HUWISE technical names."""
    mapping: dict[str, str] = {}
    for row in schema_rows:
        src = _clean_text(row.get("technical_name_dataspot"))
        dst = _normalize_huwise_field_name(_clean_text(row.get("technical_name_huwise")))
        if not src or not dst or src == "geometry":
            continue
        mapping[src] = dst
    return mapping


def _prepare_geojson_wgs84(local_file: Path, column_mapping: dict[str, str]) -> Path:
    """Ensure uploaded GeoJSON uses EPSG:4326."""
    gdf = gpd.read_file(local_file)
    applicable = {src: dst for src, dst in column_mapping.items() if src in gdf.columns and dst}
    inverse: dict[str, list[str]] = {}
    for src, dst in applicable.items():
        inverse.setdefault(dst, []).append(src)
    collisions = {dst: names for dst, names in inverse.items() if len(names) > 1}
    if collisions:
        raise ValueError(f"HUWISE field name collisions detected: {collisions}")
    if applicable:
        gdf = gdf.rename(columns=applicable)

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
            guessed = _clean_text(first.get("type") if isinstance(first, dict) else "")
            if guessed:
                return guessed
    except Exception:
        return "geojson"
    return "geojson"


def _upsert_dataset_resource(ods_id: str, source_url: str, *, dry_run: bool) -> None:
    """Create/update HUWISE resource that points to the published GeoJSON URL."""
    if dry_run:
        logging.info("[dry-run] Would upsert resource for ods_id=%s source=%s", ods_id, source_url)
        return
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
        title = _clean_text(item.get("title"))
        datasource = item.get("datasource", {})
        relative_url = _clean_text(datasource.get("relative_url"))
        connection_url = _clean_text(datasource.get("connection", {}).get("url"))
        full_url = f"{connection_url}{relative_url}" if connection_url and relative_url else ""
        if title == resource_title or full_url == source_url or relative_url == urlparse(source_url).path:
            matched_uid = _clean_text(item.get("uid"))
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
                logging.info("Created fallback resource for ods_id=%s uid=%s", ods_id, _clean_text(response.get("uid")))
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
            logging.info("Created resource for ods_id=%s uid=%s", ods_id, _clean_text(response.get("uid")))
        except Exception as exc:
            logging.warning("Failed to create resource for ods_id=%s source=%s: %s", ods_id, source_url, exc)


def _process_dataset(
    auth: DataspotAuth,
    pub_row: pd.Series,
    metadata_row: pd.Series,
    geojson_files: list[Path],
    accepted_types: set[str],
    refresh_schemas: bool,
    dry_run: bool,
) -> None:
    """Process one dataset from source extraction to HUWISE schema update."""
    context = DatasetContext(
        ods_id=_clean_text(pub_row.get("ods_id")),
        dataspot_dataset_id=_clean_text(pub_row.get("id")),
        geo_dataset=_clean_text(pub_row.get("geo_dataset")),
    )
    if not context.ods_id or not context.dataspot_dataset_id:
        logging.warning("Skipping row with missing ods_id or dataspot id")
        return

    _ensure_huwise_dataset(context.ods_id, metadata_row, dry_run=dry_run)
    _ensure_dataset_restricted(context.ods_id, dry_run=dry_run)

    geojson_file = _resolve_geojson_file(context, geojson_files)
    if geojson_file is None:
        logging.warning(
            "No local GeoJSON found for ods_id=%s (%s): resource/schema skipped, metadata still updated",
            context.ods_id,
            context.geo_dataset,
        )
        _set_metadata_fields(context.ods_id, metadata_row, source_url="", dry_run=dry_run)
        logging.info("Finished ods_id=%s (metadata only, no local GeoJSON)", context.ods_id)
        return

    schema_json = _schema_json_path(context.ods_id)
    if schema_json.exists() and not refresh_schemas:
        reconciled_schema = _load_schema_rows(context.ods_id, schema_json)
    else:
        schema_rows = _fetch_dataspot_schema_rows(auth, context.dataspot_dataset_id)
        geojson_properties = _read_geojson_properties(geojson_file)
        reconciled_schema = _reconcile_schema_with_geojson(schema_rows, geojson_properties)
        _write_schema_json(context.ods_id, reconciled_schema)
    _validate_dataspot_schema_against_geometa(
        schema_rows=reconciled_schema,
        metadata_row=metadata_row,
        dataspot_uuid=context.dataspot_dataset_id,
        ods_id=context.ods_id,
    )
    schema_file = _write_schema_file(context.ods_id, reconciled_schema)
    publish_geojson = _prepare_geojson_wgs84(geojson_file, _schema_name_mapping(reconciled_schema))
    source_url = f"{SOURCE_URL_PREFIX}/{publish_geojson.name}"

    _upload_geojson(publish_geojson, dry_run=dry_run)
    _upsert_dataset_resource(context.ods_id, source_url, dry_run=dry_run)
    _set_metadata_fields(context.ods_id, metadata_row, source_url, dry_run=dry_run)
    _upsert_huwise_schema(context.ods_id, schema_file, dry_run=dry_run, accepted_types=accepted_types)
    logging.info("Finished ods_id=%s", context.ods_id)


def _build_metadata_lookup(metadata_df: pd.DataFrame) -> dict[str, pd.Series]:
    """Build a map of metadata rows by ods_id."""
    lookup: dict[str, pd.Series] = {}
    for _, row in metadata_df.iterrows():
        ods_id = _clean_text(row.get("ods_id"))
        if ods_id:
            lookup[ods_id] = row
    return lookup


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Publish STAC datasets to FTP and HUWISE.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run without FTP/HUWISE write operations.",
    )
    parser.add_argument(
        "--refresh-schemas",
        action="store_true",
        help="Rebuild data/schemas/<ods_id>.schema.json from Dataspot even when schema JSON already exists.",
    )
    return parser.parse_args()


def main() -> None:
    ensure_output_dirs()
    """Run the publishing pipeline for all rows in pub_datasets."""
    args = parse_args()
    if PUBLISH_CATALOG_FILE.exists():
        try:
            _sync_publish_catalog_from_huwise()
        except Exception as exc:
            logging.warning("Automatic HUWISE pull into publish catalog failed: %s", exc)
    auth = _build_dataspot_client()
    pub_df, metadata_df = _load_publish_inputs()
    metadata_lookup = _build_metadata_lookup(metadata_df)
    geojson_files = _build_geojson_index()
    accepted_field_types = _discover_portal_field_types([_clean_text(value) for value in pub_df.get("ods_id", [])])
    if accepted_field_types:
        logging.info("Discovered HUWISE field types: %s", ", ".join(sorted(accepted_field_types)))
    else:
        logging.warning("Could not discover HUWISE field types; schema field upserts may be skipped")

    for _, pub_row in pub_df.iterrows():
        ods_id = _clean_text(pub_row.get("ods_id"))
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
                refresh_schemas=args.refresh_schemas,
                dry_run=args.dry_run,
            )
        except Exception as exc:
            logging.error("Failed ods_id=%s: %s", ods_id, exc)


if __name__ == "__main__":
    main()
