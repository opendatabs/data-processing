"""Streamlit editor for data/publish_catalog.json."""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any

_UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)

import pandas as pd
import streamlit as st

BASE_DIR = Path(__file__).parent
CATALOG_PATH = BASE_DIR / "publish_catalog.json"
STAC_INDEX_PATH = BASE_DIR / "stac_index.json"
STAC_COLLECTIONS_PATH = BASE_DIR / "bs_stac_collections.xlsx"
SCHEMAS_DIR = BASE_DIR / "schemas"
DATASETS_DIR = BASE_DIR / "datasets"

DEFAULT_RIGHTS = "NonCommercialAllowed-CommercialAllowed-ReferenceRequired"
DEFAULT_LICENSE = "terms_by"
DEFAULT_LANGUAGE = "de"
DEFAULT_CONTACT_NAME = "Open Data Basel-Stadt"
DEFAULT_CONTACT_EMAIL = "opendata@bs.ch"
DATASPOT_DATASET_PAGE_URL = "https://bs.dataspot.io/web/prod/assets/{dataset_id}"

THEME_OPTIONS = [
    ("Arbeit, Erwerb", "20bb143"),
    ("Bau- und Wohnungswesen", "c813f26"),
    ("Bevölkerung", "3606293"),
    ("Bildung, Wissenschaft", "c9a169b"),
    ("Energie", "06af88d"),
    ("Finanzen", "b8b874a"),
    ("Gebäude", "cc7ea4s"),
    ("Geographie", "7542721"),
    ("Gesetzgebung", "6173474"),
    ("Gesundheit", "e2e248a"),
    ("Handel", "d847e7c"),
    ("Industrie, Dienstleistungen", "da0ff7d"),
    ("Kriminalität, Strafrecht", "ae41f5e"),
    ("Kultur, Medien, Informationsgesellschaft, Sport", "e9dc0c8"),
    ("Land- und Forstwirtschaft", "59506c3"),
    ("Mobilität und Verkehr", "3d7f80f"),
    ("Politik", "9b815ca"),
    ("Preise", "338b3e5"),
    ("Raum und Umwelt", "186e3a8"),
    ("Soziale Sicherheit", "6e0eacc"),
    ("Statistische Grundlagen", "ca365da"),
    ("Tourismus", "0a7844c"),
    ("Verwaltung", "7b5b405"),
    ("Volkswirtschaft", "0774467"),
    ("Öffentliche Ordnung und Sicherheit", "60c7454"),
]

ACCRUAL_OPTIONS = [
    {"label": "jaehrlich", "uri": "http://publications.europa.eu/resource/authority/frequency/ANNUAL"},
    {"label": "halbjaehrlich", "uri": "http://publications.europa.eu/resource/authority/frequency/ANNUAL_2"},
    {"label": "dreimal im Jahr", "uri": "http://publications.europa.eu/resource/authority/frequency/ANNUAL_3"},
    {"label": "alle zwanzig Jahre", "uri": "http://publications.europa.eu/resource/authority/frequency/BIDECENNIAL"},
    {"label": "zweijaehrlich", "uri": "http://publications.europa.eu/resource/authority/frequency/BIENNIAL"},
    {"label": "alle zwei Stunden", "uri": "http://publications.europa.eu/resource/authority/frequency/BIHOURLY"},
    {"label": "zweimonatlich", "uri": "http://publications.europa.eu/resource/authority/frequency/BIMONTHLY"},
    {"label": "zweiwoechentlich", "uri": "http://publications.europa.eu/resource/authority/frequency/BIWEEKLY"},
    {"label": "kontinuierlich", "uri": "http://publications.europa.eu/resource/authority/frequency/CONT"},
    {"label": "taeglich", "uri": "http://publications.europa.eu/resource/authority/frequency/DAILY"},
    {"label": "zweimal taeglich", "uri": "http://publications.europa.eu/resource/authority/frequency/DAILY_2"},
    {"label": "alle zehn Jahre", "uri": "http://publications.europa.eu/resource/authority/frequency/DECENNIAL"},
    {"label": "stuendlich", "uri": "http://publications.europa.eu/resource/authority/frequency/HOURLY"},
    {"label": "unregelmaessig", "uri": "http://publications.europa.eu/resource/authority/frequency/IRREG"},
    {"label": "monatlich", "uri": "http://publications.europa.eu/resource/authority/frequency/MONTHLY"},
    {"label": "zweimal im Monat", "uri": "http://publications.europa.eu/resource/authority/frequency/MONTHLY_2"},
    {"label": "dreimal im Monat", "uri": "http://publications.europa.eu/resource/authority/frequency/MONTHLY_3"},
    {"label": "niemals", "uri": "http://publications.europa.eu/resource/authority/frequency/NEVER"},
    {"label": "vorlaeufige Daten", "uri": "http://publications.europa.eu/resource/authority/frequency/OP_DATPRO"},
    {"label": "anderer", "uri": "http://publications.europa.eu/resource/authority/frequency/OTHER"},
    {"label": "vierjaehrlich", "uri": "http://publications.europa.eu/resource/authority/frequency/QUADRENNIAL"},
    {"label": "vierteljaehrlich", "uri": "http://publications.europa.eu/resource/authority/frequency/QUARTERLY"},
    {"label": "fuenfjaehrlich", "uri": "http://publications.europa.eu/resource/authority/frequency/QUINQUENNIAL"},
    {"label": "alle dreissig Jahre", "uri": "http://publications.europa.eu/resource/authority/frequency/TRIDECENNIAL"},
    {"label": "dreijaehrlich", "uri": "http://publications.europa.eu/resource/authority/frequency/TRIENNIAL"},
    {"label": "alle drei Stunden", "uri": "http://publications.europa.eu/resource/authority/frequency/TRIHOURLY"},
    {"label": "unbekannt", "uri": "http://publications.europa.eu/resource/authority/frequency/UNKNOWN"},
    {"label": "staendige Aktualisierung", "uri": "http://publications.europa.eu/resource/authority/frequency/UPDATE_CONT"},
    {"label": "woechentlich", "uri": "http://publications.europa.eu/resource/authority/frequency/WEEKLY"},
    {"label": "zweimal pro Woche", "uri": "http://publications.europa.eu/resource/authority/frequency/WEEKLY_2"},
    {"label": "dreimal pro Woche", "uri": "http://publications.europa.eu/resource/authority/frequency/WEEKLY_3"},
]


def _clean(value: Any) -> str:
    return str(value).strip() if value is not None else ""


def _split_semicolon(value: Any) -> list[str]:
    text = _clean(value)
    if not text:
        return []
    return [part.strip() for part in text.split(";") if part.strip()]


def _split_keywords(value: Any) -> list[str]:
    text = _clean(value)
    if not text:
        return []
    text = text.replace(";", ",")
    return [part.strip() for part in text.split(",") if part.strip()]


def _load_catalog() -> dict[str, Any]:
    if not CATALOG_PATH.exists():
        return {"version": 1, "datasets": []}
    return json.loads(CATALOG_PATH.read_text(encoding="utf-8"))


def _save_catalog(catalog: dict[str, Any]) -> None:
    CATALOG_PATH.write_text(json.dumps(catalog, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _load_stac_index() -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    if STAC_INDEX_PATH.exists():
        payload = json.loads(STAC_INDEX_PATH.read_text(encoding="utf-8"))
        frames.append(pd.DataFrame(payload.get("datasets", [])))
    if STAC_COLLECTIONS_PATH.exists():
        df = pd.read_excel(STAC_COLLECTIONS_PATH).fillna("")
        frames.append(
            pd.DataFrame(
                {
                    "dataspot_dataset_id": df.get("id", ""),
                    "geo_dataset": df.get("id", ""),
                    "paket": df.get("datasets", ""),
                    "titel_nice": df.get("title", ""),
                    "publizierende_organisation": df.get("publishing_organization", ""),
                    "herausgeber": df.get("producer_organization", ""),
                    "theme": df.get("themes", ""),
                    "keyword": df.get("keywords", ""),
                    "stac_collection_id": df.get("id", ""),
                    "stac_title": df.get("title", ""),
                    "stac_description": df.get("description", ""),
                    "stac_metadata_html": df.get("Metadata", ""),
                }
            )
        )
    if not frames:
        return pd.DataFrame()
    merged = pd.concat(frames, ignore_index=True).fillna("")
    merged["dataspot_dataset_id"] = merged.get("dataspot_dataset_id", "").astype(str).str.strip()
    merged["geo_dataset"] = merged.get("geo_dataset", "").astype(str).str.strip()
    merged["paket"] = merged.get("paket", "").astype(str).str.strip()
    merged["_dedupe_key"] = (
        merged.get("stac_collection_id", "").astype(str).str.strip()
        + "||"
        + merged["dataspot_dataset_id"]
        + "||"
        + merged["geo_dataset"]
    )
    merged = merged.loc[merged["_dedupe_key"] != "||||||"].drop_duplicates("_dedupe_key")
    return merged.drop(columns=["_dedupe_key"])


def _normalize_name(value: str) -> str:
    return "".join(ch for ch in value.lower() if ch.isalnum())


def _normalize_huwise_field_name(value: Any) -> str:
    text = _clean(value).lower()
    text = re.sub(r"\s+", "_", text)
    text = re.sub(r"[^a-z0-9_]", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text


def _find_geojson_preview(geo_dataset: str) -> tuple[list[str], list[dict[str, Any]]]:
    if not DATASETS_DIR.exists():
        return [], []
    target = _normalize_name(geo_dataset)
    for candidate in sorted(DATASETS_DIR.glob("*.geojson")):
        stem = _normalize_name(candidate.stem)
        if target and (stem.endswith(target) or target in stem):
            payload = json.loads(candidate.read_text(encoding="utf-8"))
            features = payload.get("features", [])
            if not features:
                return [], []
            props = [feature.get("properties", {}) for feature in features[:5]]
            columns = list(props[0].keys()) if props else []
            return columns, props
    return [], []


def _schema_file(ods_id: str) -> Path:
    return SCHEMAS_DIR / f"{ods_id}.schema.json"


def _load_schema_rows(ods_id: str) -> list[dict[str, Any]]:
    path = _schema_file(ods_id)
    if not path.exists():
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows = payload.get("fields", [])
    if not isinstance(rows, list):
        return []
    normalized: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        dataspot_name = _clean(row.get("technical_name_dataspot")) or _clean(row.get("technical_name"))
        huwise_name = _normalize_huwise_field_name(_clean(row.get("technical_name_huwise")) or dataspot_name)
        normalized.append(
            {
                "technical_name_dataspot": dataspot_name,
                "technical_name_huwise": huwise_name,
                "column_name": _clean(row.get("column_name")),
                "description": _clean(row.get("description")),
                "datatype": _clean(row.get("datatype")) or "Text",
                "multivalued_separator": _clean(row.get("multivalued_separator")),
                "source": _clean(row.get("source")) or "manual",
            }
        )
    return normalized


def _save_schema_rows(ods_id: str, rows: list[dict[str, Any]]) -> None:
    SCHEMAS_DIR.mkdir(parents=True, exist_ok=True)
    path = _schema_file(ods_id)
    normalized_rows: list[dict[str, Any]] = []
    seen_huwise: set[str] = set()
    for row in rows:
        dataspot_name = _clean(row.get("technical_name_dataspot")) or _clean(row.get("technical_name"))
        huwise_name = _normalize_huwise_field_name(_clean(row.get("technical_name_huwise")) or dataspot_name)
        if not dataspot_name or not huwise_name:
            continue
        if huwise_name in seen_huwise:
            raise ValueError(f"Doppelter HUWISE-Feldname im Schema: {huwise_name}")
        seen_huwise.add(huwise_name)
        normalized_rows.append(
            {
                "technical_name_dataspot": dataspot_name,
                "technical_name_huwise": huwise_name,
                "column_name": _clean(row.get("column_name")),
                "description": _clean(row.get("description")),
                "datatype": _clean(row.get("datatype")) or "Text",
                "multivalued_separator": _clean(row.get("multivalued_separator")),
                "source": _clean(row.get("source")) or "manual",
            }
        )
    payload = {"ods_id": ods_id, "fields": normalized_rows}
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _validate(catalog: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    if catalog.get("version") != 1:
        errors.append("Top-level version muss 1 sein.")
    datasets = catalog.get("datasets")
    if not isinstance(datasets, list):
        errors.append("Top-level datasets muss eine Liste sein.")
        return errors
    required = ("ods_id", "dataspot_dataset_id", "geo_dataset", "paket", "title")
    for index, dataset in enumerate(datasets):
        for field in required:
            if not _clean(dataset.get(field)):
                errors.append(f"datasets[{index}] fehlt Pflichtfeld '{field}'")
        if not isinstance(dataset.get("theme_ids", []), list):
            errors.append(f"datasets[{index}].theme_ids muss eine Liste sein")
        if not isinstance(dataset.get("relation_urls", []), list):
            errors.append(f"datasets[{index}].relation_urls muss eine Liste sein")
    return errors


def _new_entry_from_stac(stac_row: dict[str, Any]) -> dict[str, Any]:
    preview = _clean(stac_row.get("stac_metadata_html"))
    theme_list = _split_semicolon(stac_row.get("theme"))
    ds_id = _clean(stac_row.get("dataspot_dataset_id"))
    if not ds_id:
        ds_id = _clean(stac_row.get("stac_collection_id"))
    return {
        "ods_id": "",
        "dataspot_dataset_id": ds_id,
        "geo_dataset": _clean(stac_row.get("geo_dataset")),
        "paket": _clean(stac_row.get("paket")),
        "title": _clean(stac_row.get("titel_nice")) or _clean(stac_row.get("stac_title")),
        "description": _clean(stac_row.get("stac_description")),
        "themes": theme_list,
        "theme_ids": [],
        "keywords": _split_keywords(stac_row.get("keyword")),
        "publizierende_organisation": _clean(stac_row.get("publizierende_organisation")),
        "publisher": _clean(stac_row.get("herausgeber")),
        "dcat_ap_ch_rights": DEFAULT_RIGHTS,
        "dcat_ap_ch_license": DEFAULT_LICENSE,
        "dcat_contact_name": DEFAULT_CONTACT_NAME,
        "dcat_contact_email": DEFAULT_CONTACT_EMAIL,
        "dcat_created": "",
        "dcat_creator": _clean(stac_row.get("herausgeber")),
        "dcat_issued": "",
        "dcat_accrualperiodicity": "",
        "language": DEFAULT_LANGUAGE,
        "relation_urls": [preview] if preview else [],
        "html_preview": preview,
        "tags": ["opendata.swiss"],
        "geodaten_modellbeschreibung": "",
    }


def _dataspot_link(dataset_id: str) -> str:
    clean_id = _clean(dataset_id)
    if not clean_id:
        return ""
    return DATASPOT_DATASET_PAGE_URL.format(dataset_id=clean_id)


def _stac_code_from_row(row: dict[str, Any]) -> str:
    code = _clean(row.get("stac_collection_id"))
    if code:
        return code
    return _clean(row.get("dataspot_dataset_id"))


def _norm_uuid(value: Any) -> str:
    text = _clean(value).lower()
    return text if _UUID_RE.match(text) else ""


def _catalog_dataspot_uuid(dataset: dict[str, Any]) -> str:
    direct = _norm_uuid(dataset.get("dataspot_dataset_id"))
    if direct:
        return direct
    hp = _clean(dataset.get("html_preview"))
    if "#" in hp:
        return _norm_uuid(hp.split("#")[-1])
    return ""


def _stac_code_from_catalog(dataset: dict[str, Any]) -> str:
    hp = _clean(dataset.get("html_preview"))
    if "/html/" in hp:
        return hp.split("/html/", 1)[-1].split("#")[0].strip().rstrip("/")
    ds = _clean(dataset.get("dataspot_dataset_id"))
    if ds and not _norm_uuid(ds):
        return ds
    return ""


def _variant_label(row: dict[str, Any]) -> str:
    geo = _clean(row.get("geo_dataset"))
    ds = _norm_uuid(row.get("dataspot_dataset_id"))
    return f"{geo or '—'} ({ds or '—'})"


def _stac_collection_label(row: dict[str, Any]) -> str:
    code = _stac_code_from_row(row)
    title = _clean(row.get("stac_title")) or _clean(row.get("paket")) or code
    return f"{title} ({code})" if code else title


def _huwise_link(ods_id: str) -> str:
    clean_id = _clean(ods_id)
    if not clean_id:
        return ""
    domain = _clean(os.environ.get("HUWISE_DOMAIN")) or "data.bs.ch"
    return f"https://{domain}/explore/dataset/{clean_id}/information/"


st.set_page_config(page_title="FGI Publish-Katalog Editor", layout="wide")
st.title("FGI Publish-Katalog Editor")
st.caption("Pflegeoberfläche für publish_catalog.json und datensatzspezifische Schema-JSON-Dateien.")

catalog = _load_catalog()
datasets = catalog.setdefault("datasets", [])
stac_df = _load_stac_index()
geo_dataset_options = sorted({value for value in stac_df.get("geo_dataset", pd.Series(dtype=str)).astype(str).tolist() if value.strip()})
paket_options = sorted({value for value in stac_df.get("paket", pd.Series(dtype=str)).astype(str).tolist() if value.strip()})
accrual_labels = [f"{opt['label']} ({opt['uri']})" for opt in ACCRUAL_OPTIONS]
accrual_by_label = {f"{opt['label']} ({opt['uri']})": opt["uri"] for opt in ACCRUAL_OPTIONS}
label_by_accrual = {opt["uri"]: f"{opt['label']} ({opt['uri']})" for opt in ACCRUAL_OPTIONS}

left, right = st.columns([1, 2])

with left:
    st.subheader("Datensatz-Auswahl")
    if stac_df.empty:
        st.info("Kein STAC-Index vorhanden. Bitte zuerst `uv run migrate_publish_catalog.py` ausführen.")
        selected_code = ""
        selected_geo = ""
        matched = None
    else:
        stac_df = stac_df.copy().fillna("")
        stac_df["stac_code"] = stac_df.apply(lambda row: _stac_code_from_row(row.to_dict()), axis=1)
        code_values = sorted({value for value in stac_df["stac_code"].astype(str).tolist() if value.strip()})
        code_labels: dict[str, str] = {}
        for code in code_values:
            sample = stac_df[stac_df["stac_code"].astype(str) == code].iloc[0].to_dict()
            code_labels[code] = _stac_collection_label(sample)
        code_options = sorted(code_values, key=lambda code: code_labels[code].lower())
        selected_code = st.selectbox(
            "STAC-Collection",
            [""] + code_options,
            format_func=lambda code: code_labels.get(code, code) if code else "",
        )
        filtered_stac = stac_df[stac_df["stac_code"].astype(str) == selected_code] if selected_code else stac_df.iloc[0:0]
        if selected_code:
            uuid_rows = filtered_stac[filtered_stac["dataspot_dataset_id"].apply(lambda value: bool(_norm_uuid(value)))]
            geo_rows = uuid_rows.assign(
                geo_label=uuid_rows.apply(lambda row: _variant_label(row.to_dict()), axis=1)
            ).sort_values("geo_label")
            geo_labels = geo_rows["geo_label"].astype(str).tolist()
            geo_by_label = {label: geo_rows.iloc[idx].to_dict() for idx, label in enumerate(geo_labels)}
        else:
            geo_labels = []
            geo_by_label = {}
        selected_geo_label = st.selectbox(
            "Geodatensatz",
            [""] + geo_labels,
        )
        selected_geo_row = geo_by_label.get(selected_geo_label)
        selected_geo = _clean(selected_geo_row.get("geo_dataset")) if selected_geo_row else ""
        matched = None
        if selected_geo_row:
            sel_uuid = _norm_uuid(selected_geo_row.get("dataspot_dataset_id"))
            sel_preview = _clean(selected_geo_row.get("stac_metadata_html"))
            sel_stac = _clean(selected_geo_row.get("stac_collection_id"))
            for item in datasets:
                if sel_uuid and _catalog_dataspot_uuid(item) == sel_uuid:
                    matched = item
                    break
                if sel_preview and _clean(item.get("html_preview")) == sel_preview:
                    matched = item
                    break
                if (
                    sel_stac
                    and _stac_code_from_catalog(item) == sel_stac
                    and _clean(item.get("geo_dataset")) == selected_geo
                ):
                    matched = item
                    break

        if selected_code and selected_geo_label and matched is None:
            st.warning("Zu dieser Kombination gibt es noch keinen Datensatz im Katalog.")
            if st.button(
                "Neuen STAC-Datensatz hinzufügen",
                use_container_width=True,
                disabled=selected_geo_row is None,
                key="add_stac_dataset_btn",
            ):
                if selected_geo_row is None:
                    st.error("Bitte zuerst einen Geodatensatz auswählen.")
                else:
                    datasets.append(_new_entry_from_stac(selected_geo_row))
                    _save_catalog(catalog)
                    st.success("Neuer Datensatz wurde hinzugefügt.")
                    st.rerun()
        elif matched is not None:
            st.success("Datensatz vorhanden. Werte werden rechts angezeigt.")

    validation_errors = _validate(catalog)
    if validation_errors:
        st.error("Validierungsfehler gefunden:")
        for item in validation_errors:
            st.write(f"- {item}")
    else:
        st.success("Katalog ist valide.")

    if st.button("JSON speichern", type="primary", use_container_width=True):
        _save_catalog(catalog)
        st.success(f"Gespeichert: {CATALOG_PATH}")

current: dict[str, Any] | None = matched if "matched" in locals() else None

with right:
    st.subheader("Editor")
    if current is None:
        st.info("Bitte links einen vorhandenen Datensatz auswählen.")
    else:
        theme_label_to_id = {label: theme_id for label, theme_id in THEME_OPTIONS}
        theme_id_to_label = {theme_id: label for label, theme_id in THEME_OPTIONS}

        current_uuid = _catalog_dataspot_uuid(current) or _clean(current.get("dataspot_dataset_id"))
        current["dataspot_dataset_id"] = current_uuid
        current.pop("metadata_source", None)
        current["dataspot_dataset_id"] = st.text_input(
            "Dataspot-UUID",
            value=current_uuid,
            disabled=True,
        )
        dataspot_link = _dataspot_link(current.get("dataspot_dataset_id", ""))
        if dataspot_link:
            st.markdown(f"[Dataspot-Link öffnen]({dataspot_link})")
        else:
            st.caption("Kein Link gesetzt")
        current_stac = _stac_code_from_catalog(current)
        current["stac_collection_id"] = st.text_input("STAC-Collection", value=current_stac, disabled=True)
        current_geo = _clean(current.get("geo_dataset"))
        geo_choices = [current_geo] + [item for item in geo_dataset_options if item != current_geo] if current_geo else geo_dataset_options
        current["geo_dataset"] = st.selectbox("Geodatensatz", geo_choices if geo_choices else [""], index=0, disabled=True)
        current["ods_id"] = st.text_input("HUWISE-ID", value=_clean(current.get("ods_id")))
        huwise_link = _huwise_link(current["ods_id"])
        if huwise_link:
            st.markdown(f"[HUWISE-Link öffnen]({huwise_link})")
        current_paket = _clean(current.get("paket"))
        paket_choices = [current_paket] + [item for item in paket_options if item != current_paket] if current_paket else paket_options
        current["paket"] = st.selectbox("Paket", paket_choices if paket_choices else [""], index=0, disabled=True)
        st.caption(
            "Quelle: Paket/Geodatensatz aus STAC. Streamlit-Werte werden beim Publish als lokale Overrides gesetzt."
        )
        current["title"] = st.text_input("Titel", value=_clean(current.get("title")))
        current["publisher"] = st.text_input("Herausgeber", value=_clean(current.get("publisher")))
        current["publizierende_organisation"] = st.text_input(
            "Publizierende Organisation",
            value=_clean(current.get("publizierende_organisation")),
        )

        current["description"] = st.text_area(
            "Beschreibung",
            value=_clean(current.get("description")),
            height=120,
        )
        selected_theme_labels = [theme_id_to_label[item] for item in current.get("theme_ids", []) if item in theme_id_to_label]
        if not selected_theme_labels:
            selected_theme_labels = [label for label in current.get("themes", []) if label in theme_label_to_id]
        selected_theme_labels = st.multiselect("Themen", options=[label for label, _ in THEME_OPTIONS], default=selected_theme_labels)
        current["theme_ids"] = [theme_label_to_id[label] for label in selected_theme_labels]
        current["themes"] = selected_theme_labels

        current["keywords"] = st.multiselect(
            "Schlüsselwörter",
            options=sorted(
                {
                    keyword.strip()
                    for item in current.get("keywords", [])
                    for keyword in _split_keywords(item)
                    if keyword.strip()
                }
            ),
            default=sorted(
                {
                    keyword.strip()
                    for item in current.get("keywords", [])
                    for keyword in _split_keywords(item)
                    if keyword.strip()
                }
            ),
            accept_new_options=True,
        )
        current["tags"] = st.multiselect(
            "Tags (inkl. opendata.swiss)",
            options=sorted(
                {
                    tag.strip()
                    for item in current.get("tags", [])
                    for tag in _split_semicolon(item)
                    if tag.strip()
                }
                | {"opendata.swiss"}
            ),
            default=sorted(
                {
                    tag.strip()
                    for item in current.get("tags", [])
                    for tag in _split_semicolon(item)
                    if tag.strip()
                }
                | {"opendata.swiss"}
            ),
            accept_new_options=True,
        )
        current["geodaten_modellbeschreibung"] = st.text_input(
            "Geodaten Modellbeschreibung (PDF-URL)",
            value=_clean(current.get("geodaten_modellbeschreibung")),
        )

        current["dcat_created"] = st.text_input("Erstellt", value=_clean(current.get("dcat_created")))
        current["dcat_creator"] = st.text_input("Ersteller", value=_clean(current.get("dcat_creator")))
        current["dcat_issued"] = st.text_input("Veröffentlicht", value=_clean(current.get("dcat_issued")))
        current["relation_urls"] = [
            line.strip()
            for line in st.text_area(
                "Relation (eine URL pro Zeile)",
                value="\n".join(current.get("relation_urls", [])),
                height=90,
            ).splitlines()
            if line.strip()
        ]

        selected_accrual = label_by_accrual.get(_clean(current.get("dcat_accrualperiodicity")), accrual_labels[0])
        selected_accrual = st.selectbox("Aktualisierungsintervall", accrual_labels, index=accrual_labels.index(selected_accrual))
        current["dcat_accrualperiodicity"] = accrual_by_label[selected_accrual]

        # Harte Defaults nach Vorgabe.
        current["language"] = DEFAULT_LANGUAGE
        current["dcat_ap_ch_rights"] = DEFAULT_RIGHTS
        current["dcat_ap_ch_license"] = DEFAULT_LICENSE
        current["dcat_contact_name"] = DEFAULT_CONTACT_NAME
        current["dcat_contact_email"] = DEFAULT_CONTACT_EMAIL

        st.markdown("### Vorschau-Link")
        current["html_preview"] = st.text_input("Klickbare Vorschau-URL", value=_clean(current.get("html_preview")))
        if current["html_preview"]:
            st.markdown(f"[Vorschau öffnen]({current['html_preview']})")

        st.markdown("### Datensatz-Ansicht (erste 5 Werte)")
        columns, preview_rows = _find_geojson_preview(_clean(current.get("geo_dataset")))
        if columns:
            st.caption(f"Technische Spaltennamen: {', '.join(columns)}")
            st.dataframe(pd.DataFrame(preview_rows), use_container_width=True)
        else:
            st.info("Keine lokale GeoJSON-Datei für diesen Geodatensatz gefunden.")

        st.markdown("### Schema (JSON)")
        schema_rows = _load_schema_rows(_clean(current.get("ods_id")))
        schema_df = pd.DataFrame(schema_rows if schema_rows else [])
        if schema_df.empty:
            schema_df = pd.DataFrame(
                columns=[
                    "technical_name_dataspot",
                    "technical_name_huwise",
                    "column_name",
                    "description",
                    "datatype",
                    "multivalued_separator",
                    "source",
                ]
            )
        edited_rows = st.data_editor(
            schema_df,
            use_container_width=True,
            num_rows="dynamic",
            disabled=["technical_name_dataspot"],
            key=f"schema_editor_{_clean(current.get('ods_id'))}",
        )
        if st.button("Schema speichern", use_container_width=True):
            try:
                _save_schema_rows(_clean(current.get("ods_id")), edited_rows.fillna("").to_dict("records"))
            except ValueError as exc:
                st.error(str(exc))
            else:
                st.success("Schema gespeichert.")
