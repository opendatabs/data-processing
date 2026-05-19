"""Nested metadata blocks and flat HUWISE snapshot entries."""

from __future__ import annotations

from typing import Any

from util import (
    clean,
    description_to_html,
    split_keywords,
    split_semicolon_list,
    third_path_segment,
)

GEOMETA_PREVIEW_URL = "https://api.geo.bs.ch/geometa/v1/metadata_details/dataset/preview/html/{collection_id}"
DEFAULT_RIGHTS = "NonCommercialAllowed-CommercialAllowed-ReferenceRequired"
DEFAULT_LICENSE = "terms_by"
DEFAULT_CONTACT_NAME = "Open Data Basel-Stadt"
DEFAULT_CONTACT_EMAIL = "opendata@bs.ch"
DEFAULT_TAG = "opendata.swiss"
DEFAULT_GEOGRAPHIC_REFERENCE = ["ch_40_12"]
DEFAULT_LICENSE_ID = "cc-by"
DEFAULT_LICENSE_NAME = "CC BY 4.0"
LICENSE_ID_BY_NAME = {
    "CC BY 4.0": "5sylls5",
    "CC BY 3.0 CH": "cc_by",
    "CC0 1.0": "4bj8ceb",
}


def _nested_payloads(metadata: dict[str, Any]) -> tuple[dict, dict, dict, dict]:
    default = metadata.get("default", {})
    dcat = metadata.get("dcat", {})
    custom = metadata.get("custom", {})
    internal = metadata.get("internal", {})
    if not isinstance(default, dict):
        default = {}
    if not isinstance(dcat, dict):
        dcat = {}
    if not isinstance(custom, dict):
        custom = {}
    if not isinstance(internal, dict):
        internal = {}
    return default, dcat, custom, internal


def build_metadata_block(
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
    """Build nested ``default`` / ``dcat`` / ``custom`` / ``internal`` metadata for catalog geo rows."""
    metadata = dataset.get("metadata", {})
    if not isinstance(metadata, dict):
        metadata = {}
    default, dcat, custom, _internal = _nested_payloads(metadata)

    relation_values_raw = dcat.get("relation", [])
    if isinstance(relation_values_raw, list):
        relation_values = [value.strip() for value in relation_values_raw if clean(value)]
    else:
        relation_values = [value.strip() for value in clean(relation_values_raw).split(";") if value.strip()]
    relation_values_final: list[str] = []
    for url in [stac_browser_url, mapbs_url, *relation_values]:
        cleaned = clean(url)
        if cleaned and cleaned not in relation_values_final:
            relation_values_final.append(cleaned)

    default_publisher = clean(default.get("publisher"))
    publisher_from_path = (
        third_path_segment(default_publisher)
        or third_path_segment(producer_organization)
        or third_path_segment(dataspot_meta["publisher_path"])
    )
    if not publisher_from_path:
        publisher_from_path = default_publisher or clean(producer_organization) or dataspot_meta["publisher_path"]

    keyword_values = [item for item in collection_keywords if clean(item)]
    if not keyword_values:
        keyword_values_raw = default.get("keyword")
        if isinstance(keyword_values_raw, list):
            keyword_values = [item.strip() for item in keyword_values_raw if clean(item)]
        else:
            keyword_values = [item.strip() for item in clean(keyword_values_raw).split(";") if item.strip()]
    if not keyword_values:
        keyword_values = [item for item in dataspot_meta["keyword_values"] if clean(item)]
    keyword_values = [item for item in keyword_values if clean(item).lower() != clean(stac_collection_id).lower()]

    tags = [item for item in ["opendata.swiss", stac_collection_id] if clean(item)]
    expected_geodaten_modellbeschreibung = f"{stac_url}#{dataspot_dataset_id}"
    custom_geodaten = clean(custom.get("geodaten_modellbeschreibung"))
    geodaten_modellbeschreibung = (
        custom_geodaten if custom_geodaten.endswith(f"#{dataspot_dataset_id}") else expected_geodaten_modellbeschreibung
    )

    return {
        "default": {
            "title": clean(default.get("title")) or dataspot_meta["title"] or geo_dataset,
            "description": description_to_html(clean(default.get("description")) or dataspot_meta["description"]),
            "keyword": keyword_values,
            "language": "de",
            "publisher": publisher_from_path,
            "modified": clean(default.get("modified")) or dataspot_meta["modified"],
            "modified_updates_on_data_change": False,
        },
        "internal": {"license": "CC BY 4.0"},
        "dcat": {
            "creator": publisher_from_path,
            "created": clean(dcat.get("created")) or dataspot_meta["created"],
            "issued": clean(dcat.get("issued")) or dataspot_meta["issued"],
            "accrualperiodicity": clean(dcat.get("accrualperiodicity")) or dataspot_meta["accrualperiodicity"],
            "relation": relation_values_final,
        },
        "custom": {
            "publizierende_organisation": clean(custom.get("publizierende_organisation")) or publisher_from_path,
            "geodaten_modellbeschreibung": geodaten_modellbeschreibung,
            "tags": tags,
        },
    }


def flatten_to_snapshot(geo: dict[str, Any], collection: dict[str, Any]) -> dict[str, Any]:
    """Build one flat ``template.field`` metadata block from nested catalog geo metadata."""
    from catalog import order_snapshot_entry

    metadata = geo.get("metadata", {})
    if not isinstance(metadata, dict):
        metadata = {}
    default, dcat, custom, internal = _nested_payloads(metadata)

    stac_collection_id = clean(collection.get("stac_collection_id"))
    stac_browser = clean(collection.get("stac_browser_url"))
    mapbs_url = clean(collection.get("mapbs_url"))
    dataspot_dataset_id = clean(geo.get("dataspot_dataset_id")).lower()
    stac_url = clean(collection.get("stac_url"))
    if not stac_url and stac_collection_id:
        stac_url = GEOMETA_PREVIEW_URL.format(collection_id=stac_collection_id).replace("/preview/", "/published/")

    relation_values: list[str] = []
    relation_raw = dcat.get("relation", [])
    if isinstance(relation_raw, list):
        relation_values = [clean(value) for value in relation_raw if clean(value)]
    else:
        relation_values = [clean(value) for value in split_semicolon_list(relation_raw) if clean(value)]
    for url in (stac_browser, mapbs_url):
        if url and url not in relation_values:
            relation_values.append(url)

    title = clean(default.get("title")) or clean(geo.get("geo_dataset"))
    keyword_values = split_keywords(default.get("keyword"))
    tag_values = split_keywords(custom.get("tags"))
    if not tag_values:
        tag_values = [DEFAULT_TAG, stac_collection_id] if stac_collection_id else [DEFAULT_TAG]
    elif stac_collection_id and stac_collection_id not in tag_values:
        tag_values = [DEFAULT_TAG, stac_collection_id, *tag_values]
    else:
        tag_values = list(dict.fromkeys([DEFAULT_TAG, *tag_values]))
    tag_values = [tag for tag in tag_values if tag]

    license_name = clean(internal.get("license")) or DEFAULT_LICENSE_NAME
    license_id = LICENSE_ID_BY_NAME.get(license_name, clean(license_name)) or LICENSE_ID_BY_NAME[DEFAULT_LICENSE_NAME]
    raw_publisher = clean(default.get("publisher"))
    publisher_from_path = third_path_segment(raw_publisher)
    resolved_publisher = publisher_from_path or raw_publisher
    resolved_creator = publisher_from_path or clean(dcat.get("creator")) or raw_publisher
    publizierende = clean(custom.get("publizierende_organisation")) or resolved_publisher
    geodaten_modellbeschreibung = clean(custom.get("geodaten_modellbeschreibung"))
    if not geodaten_modellbeschreibung and stac_collection_id and dataspot_dataset_id:
        geodaten_modellbeschreibung = (
            f"{GEOMETA_PREVIEW_URL.format(collection_id=stac_collection_id)}#{dataspot_dataset_id}"
        )

    snapshot: dict[str, Any] = {
        "default.title": title,
        "default.description": clean(default.get("description")),
        "default.language": "de",
        "default.geographic_reference": list(DEFAULT_GEOGRAPHIC_REFERENCE),
        "default.publisher": resolved_publisher,
        "default.modified_updates_on_data_change": bool(default.get("modified_updates_on_data_change", False)),
        "default.modified_updates_on_metadata_change": False,
        "custom.publizierende_organisation": publizierende,
        "custom.tags": tag_values,
        "custom.geodaten_modellbeschreibung": geodaten_modellbeschreibung,
        "dcat.contact_name": DEFAULT_CONTACT_NAME,
        "dcat.contact_email": DEFAULT_CONTACT_EMAIL,
        "dcat_ap_ch.rights": DEFAULT_RIGHTS,
        "dcat_ap_ch.license": DEFAULT_LICENSE,
        "internal.license_id": license_id,
    }
    modified = clean(default.get("modified"))
    if modified:
        snapshot["default.modified"] = modified
    if keyword_values:
        snapshot["default.keyword"] = keyword_values
    if resolved_creator:
        snapshot["dcat.creator"] = resolved_creator
    created = clean(dcat.get("created"))
    if created:
        snapshot["dcat.created"] = created
    issued = clean(dcat.get("issued"))
    if issued:
        snapshot["dcat.issued"] = issued
    accrual = clean(dcat.get("accrualperiodicity"))
    if accrual:
        snapshot["dcat.accrualperiodicity"] = accrual
    if relation_values:
        snapshot["dcat.relation"] = relation_values
    return order_snapshot_entry(snapshot)


def dataspot_uuid_from_snapshot(entry: dict[str, Any]) -> str:
    geodaten = clean(entry.get("custom.geodaten_modellbeschreibung"))
    if "#" in geodaten:
        return geodaten.rsplit("#", 1)[-1].lower()
    return ""
