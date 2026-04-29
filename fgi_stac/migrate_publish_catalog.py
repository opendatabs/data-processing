"""Migrate legacy raw inputs to `data/publish_catalog.json`."""

from __future__ import annotations

from pathlib import Path
import json
import re
from typing import Any

import pandas as pd

from geometa_stac_datasets import discover_instances_for_collection
from paths import DATA_DIR, ensure_output_dirs, resolve_input_file

_UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)

PUB_DATASETS_FILE = resolve_input_file("pub_datasets.xlsx")
METADATA_FILE = resolve_input_file("Metadata.csv")
STAC_COLLECTIONS_FILE = resolve_input_file("bs_stac_collections.xlsx")
OUTPUT_FILE = DATA_DIR / "publish_catalog.json"
STAC_INDEX_FILE = DATA_DIR / "stac_index.json"
DEFAULT_RIGHTS = "NonCommercialAllowed-CommercialAllowed-ReferenceRequired"
DEFAULT_LICENSE = "terms_by"
DEFAULT_LANGUAGE = "de"
DEFAULT_CONTACT_NAME = "Open Data Basel-Stadt"
DEFAULT_CONTACT_EMAIL = "opendata@bs.ch"


def _split_semicolon(value: Any) -> list[str]:
    text = str(value).strip() if value is not None else ""
    if not text or text.lower() == "nan":
        return []
    return [part.strip() for part in text.split(";") if part.strip()]


def _split_keywords_cell(value: Any) -> list[str]:
    """HUWISE/Dataspot keyword cells may use commas or semicolons."""
    text = str(value).strip() if value is not None else ""
    if not text or text.lower() == "nan":
        return []
    text = text.replace(";", ",")
    return [part.strip() for part in text.split(",") if part.strip()]


def _geometa_preview_url(stac_collection_id: str, dataspot_uuid: str, relation_urls: list[str]) -> str:
    """Canonical geometa HTML preview: /html/<STAC>#<Dataspot-UUID>."""
    last_rel = relation_urls[-1] if relation_urls else ""
    if last_rel and "#" in last_rel:
        frag = last_rel.split("#")[-1].strip()
        if _UUID_RE.match(frag) and frag.lower() == dataspot_uuid.lower():
            return last_rel
    base = f"https://api.geo.bs.ch/geometa/v1/metadata_details/dataset/preview/html/{stac_collection_id}"
    if last_rel:
        base = last_rel.split("#")[0].strip() or base
    if dataspot_uuid and _UUID_RE.match(dataspot_uuid):
        return f"{base}#{dataspot_uuid}"
    return last_rel or base


def _catalog_to_pub_df(catalog_datasets: list[dict[str, Any]]) -> pd.DataFrame:
    """Build a pub-like dataframe from publish_catalog datasets."""
    rows: list[dict[str, Any]] = []
    for dataset in catalog_datasets:
        rows.append(
            {
                "ods_id": str(dataset.get("ods_id", "")).strip(),
                "id": str(dataset.get("dataspot_dataset_id", "")).strip(),
                "geo_dataset": str(dataset.get("geo_dataset", "")).strip(),
                "Paket": str(dataset.get("paket", "")).strip(),
                "publizierende Organisation": str(dataset.get("publizierende_organisation", "")).strip(),
            }
        )
    return pd.DataFrame(rows).fillna("")


def main() -> None:
    ensure_output_dirs()
    existing_catalog = (
        json.loads(OUTPUT_FILE.read_text(encoding="utf-8")).get("datasets", []) if OUTPUT_FILE.exists() else []
    )
    if PUB_DATASETS_FILE.exists():
        pub_df = pd.read_excel(PUB_DATASETS_FILE).fillna("")
    elif existing_catalog:
        pub_df = _catalog_to_pub_df(existing_catalog)
    else:
        raise FileNotFoundError(
            f"Weder {PUB_DATASETS_FILE} noch ein bestehendes {OUTPUT_FILE} gefunden. "
            "Bitte zuerst Rohdaten extrahieren oder einen bestehenden Katalog bereitstellen."
        )

    metadata_df = pd.read_csv(METADATA_FILE, sep=";").fillna("") if METADATA_FILE.exists() else pd.DataFrame()
    stac_df = pd.read_excel(STAC_COLLECTIONS_FILE).fillna("") if STAC_COLLECTIONS_FILE.exists() else pd.DataFrame()
    metadata_lookup = {str(row.get("ods_id", "")).strip(): row for _, row in metadata_df.iterrows()}

    datasets: list[dict[str, Any]] = []
    warnings: list[str] = []
    for _, row in pub_df.iterrows():
        ods_id = str(row.get("ods_id", "")).strip()
        dataspot_id = str(row.get("id", "")).strip()
        geo_dataset = str(row.get("geo_dataset", "")).strip()
        metadata = metadata_lookup.get(ods_id)
        if metadata is None:
            metadata = next(
                (item for item in existing_catalog if str(item.get("ods_id", "")).strip() == ods_id),
                {},
            )
            warnings.append(f"Missing Metadata.csv row for ods_id={ods_id}; fallback to existing catalog values")

        relations = _split_semicolon(metadata.get("dcat.relation") or metadata.get("relation_urls"))
        datasets.append(
            {
                "ods_id": ods_id,
                "dataspot_dataset_id": dataspot_id,
                "geo_dataset": geo_dataset,
                "paket": str(row.get("Paket", "")).strip(),
                "publizierende_organisation": str(row.get("publizierende Organisation", "")).strip(),
                "title": str(metadata.get("title", "")).strip(),
                "description": str(metadata.get("description", "")).strip(),
                "themes": _split_semicolon(metadata.get("theme")),
                "theme_ids": [],
                "keywords": _split_keywords_cell(metadata.get("keyword")),
                "dcat_ap_ch_rights": DEFAULT_RIGHTS,
                "dcat_ap_ch_license": DEFAULT_LICENSE,
                "dcat_contact_name": DEFAULT_CONTACT_NAME,
                "dcat_contact_email": DEFAULT_CONTACT_EMAIL,
                "dcat_created": str(metadata.get("dcat.created") or metadata.get("dcat_created", "")).strip(),
                "dcat_creator": str(metadata.get("dcat.creator") or metadata.get("dcat_creator", "")).strip(),
                "dcat_accrualperiodicity": str(
                    metadata.get("dcat.accrualperiodicity") or metadata.get("dcat_accrualperiodicity", "")
                ).strip(),
                "publisher": str(metadata.get("publisher", "")).strip(),
                "dcat_issued": str(metadata.get("dcat.issued") or metadata.get("dcat_issued", "")).strip(),
                "language": DEFAULT_LANGUAGE,
                "relation_urls": relations,
                "html_preview": relations[-1] if relations else "",
                "metadata_source": "huwise",
                "tags": ["opendata.swiss"],
                "geodaten_modellbeschreibung": "",
            }
        )

    output = {"version": 1, "datasets": datasets}
    OUTPUT_FILE.write_text(json.dumps(output, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {len(datasets)} datasets to {OUTPUT_FILE}")

    # STAC index: every collection from bs_stac_collections × every Dataspot dataset UUID from Geometa HTML.
    pub_by_uuid: dict[str, Any] = {}
    for _, prow in pub_df.iterrows():
        uid = str(prow.get("id", "")).strip().lower()
        if uid and _UUID_RE.match(uid):
            pub_by_uuid[uid] = prow

    def _append_stac_index_row(
        *,
        stac_meta: pd.Series,
        stac_collection_id: str,
        dataspot_uuid: str,
        geo_dataset: str,
        pub_row: Any | None,
    ) -> None:
        ods_id = str(pub_row.get("ods_id", "")).strip() if pub_row is not None else ""
        meta_row = metadata_lookup.get(ods_id) if ods_id else None
        relations = _split_semicolon(meta_row.get("dcat.relation")) if meta_row is not None else []
        paket = str(pub_row.get("Paket", "")).strip() if pub_row is not None else ""
        if not paket:
            paket = str(stac_meta.get("title", "")).strip()
        titel_nice = str(meta_row.get("title", "")).strip() if meta_row is not None else ""
        if not titel_nice:
            titel_nice = geo_dataset or str(stac_meta.get("title", "")).strip()
        keyword_cell = str(meta_row.get("keyword", "")).strip() if meta_row is not None else ""
        theme_cell = str(meta_row.get("theme", "")).strip() if meta_row is not None else ""
        preview = _geometa_preview_url(stac_collection_id, dataspot_uuid, relations)
        stac_items.append(
            {
                "dataspot_dataset_id": dataspot_uuid,
                "geo_dataset": geo_dataset,
                "paket": paket,
                "titel_nice": titel_nice,
                "publizierende_organisation": str(
                    pub_row.get("publizierende Organisation", "")
                ).strip()
                if pub_row is not None
                else str(stac_meta.get("publishing_organization", "")).strip(),
                "herausgeber": str(stac_meta.get("producer_organization", "")).strip(),
                "theme": theme_cell or str(stac_meta.get("themes", "")).strip(),
                "keyword": keyword_cell or str(stac_meta.get("keywords", "")).strip(),
                "stac_collection_id": stac_collection_id,
                "stac_title": str(stac_meta.get("title", "")).strip(),
                "stac_description": str(stac_meta.get("description", "")).strip(),
                "stac_metadata_html": preview or str(stac_meta.get("Metadata", "")).strip(),
            }
        )

    stac_items: list[dict[str, Any]] = []
    if not stac_df.empty:
        for _, srow in stac_df.iterrows():
            scid = str(srow.get("id", "")).strip()
            st_title = str(srow.get("title", "")).strip()
            if not scid:
                continue
            instances = discover_instances_for_collection(scid, st_title)
            if instances:
                print(f"  Geometa {scid}: {len(instances)} dataset(s)")
                for inst in instances:
                    u = inst["dataspot_uuid"]
                    geo = inst["geo_dataset"]
                    prow = pub_by_uuid.get(u.lower())
                    _append_stac_index_row(
                        stac_meta=srow,
                        stac_collection_id=scid,
                        dataspot_uuid=u,
                        geo_dataset=geo,
                        pub_row=prow,
                    )
                continue
            # No Geometa parse: fall back to pub rows whose Paket matches STAC title
            matching = pub_df[pub_df["Paket"].astype(str).str.strip() == st_title]
            if not matching.empty:
                print(f"  Geometa {scid}: 0 datasets (using {len(matching)} pub_datasets row(s))")
                for _, pub_row in matching.iterrows():
                    uid = str(pub_row.get("id", "")).strip()
                    geo = str(pub_row.get("geo_dataset", "")).strip()
                    if not uid:
                        continue
                    _append_stac_index_row(
                        stac_meta=srow,
                        stac_collection_id=scid,
                        dataspot_uuid=uid,
                        geo_dataset=geo,
                        pub_row=pub_row,
                    )
                continue
            print(f"  Geometa {scid}: 0 datasets (STAC-only fallback)")
            stac_items.append(
                {
                    "dataspot_dataset_id": scid,
                    "geo_dataset": "",
                    "paket": st_title,
                    "titel_nice": st_title,
                    "publizierende_organisation": str(srow.get("publishing_organization", "")).strip(),
                    "herausgeber": str(srow.get("producer_organization", "")).strip(),
                    "theme": str(srow.get("themes", "")).strip(),
                    "keyword": str(srow.get("keywords", "")).strip(),
                    "stac_collection_id": scid,
                    "stac_title": st_title,
                    "stac_description": str(srow.get("description", "")).strip(),
                    "stac_metadata_html": str(srow.get("Metadata", "")).strip(),
                }
            )
    else:
        for _, row in pub_df.iterrows():
            stac_items.append(
                {
                    "dataspot_dataset_id": str(row.get("id", "")).strip(),
                    "geo_dataset": str(row.get("geo_dataset", "")).strip(),
                    "paket": str(row.get("Paket", "")).strip(),
                    "titel_nice": "",
                    "publizierende_organisation": str(row.get("publizierende Organisation", "")).strip(),
                    "herausgeber": "",
                    "theme": "",
                    "keyword": "",
                    "stac_collection_id": "",
                    "stac_title": "",
                    "stac_description": "",
                    "stac_metadata_html": "",
                }
            )
    STAC_INDEX_FILE.write_text(json.dumps({"datasets": stac_items}, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {len(stac_items)} STAC index entries to {STAC_INDEX_FILE}")
    if warnings:
        print("Warnings:")
        for warning in warnings:
            print(f"- {warning}")


if __name__ == "__main__":
    main()
