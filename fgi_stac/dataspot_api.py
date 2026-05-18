"""Dataspot REST helpers."""

from __future__ import annotations

from typing import Any

from dataspot_auth import DataspotAuth
from http_client import http_get_json
from util import clean, extract_string_list, normalize_optional_date

DATASPOT_COMPOSITIONS_URL = "https://bs.dataspot.io/rest/prod/datasets/{dataset_id}/compositions"
DATASPOT_DATASET_URL = "https://bs.dataspot.io/rest/prod/datasets/{dataset_id}"
DATASPOT_ATTRIBUTE_URL = "https://bs.dataspot.io/rest/prod/attributes/{attribute_id}"
DATASPOT_RANGE_ASSET_URL = "https://bs.dataspot.io/rest/prod/assets/{asset_id}"


def dataspot_get(auth: DataspotAuth, url: str, *, allow_404: bool = False) -> dict[str, Any] | None:
    return http_get_json(url, headers=auth.get_headers(), allow_404=allow_404)


def dataspot_metadata(auth: DataspotAuth, dataspot_dataset_id: str) -> dict[str, Any]:
    payload = dataspot_get(auth, DATASPOT_DATASET_URL.format(dataset_id=dataspot_dataset_id), allow_404=True) or {}
    if not isinstance(payload, dict):
        payload = {}
    custom = payload.get("customProperties", {})
    if not isinstance(custom, dict):
        custom = {}
    keywords = extract_string_list(payload.get("tags"))
    publisher_path = clean(
        payload.get("producerOrganization") or payload.get("publishingOrganization") or payload.get("publisher")
    )
    return {
        "object_type": clean(payload.get("_type")),
        "title": clean(payload.get("label") or payload.get("title")),
        "description": clean(payload.get("description")),
        "keyword_values": keywords,
        "publisher_path": publisher_path,
        "created": normalize_optional_date(custom.get("creationDate")),
        "modified": normalize_optional_date(payload.get("lastUpdate") or payload.get("modified")),
        "issued": normalize_optional_date(custom.get("publicationDate")),
        "accrualperiodicity": clean(payload.get("accrualPeriodicity")),
    }
