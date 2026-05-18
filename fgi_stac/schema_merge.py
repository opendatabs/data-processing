"""Merge pipeline schema (data_orig) with user settings (data/schema_files)."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import yaml

from paths import GEOMETA_DATASET_HTML_URL, ORIG_SCHEMA_FILES_DIR, USER_SCHEMA_FILES_DIR
from yaml_io import dump_yaml

EDITORIAL_KEYS = ("name", "description", "mehrwertigkeit", "datentyp")


from util import clean


def schema_file_slug(value: str) -> str:
    import re

    text = clean(value)
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


def schema_orig_path(schema_basename: str) -> Path:
    return ORIG_SCHEMA_FILES_DIR / f"{schema_file_slug(schema_basename)}.yaml"


def schema_user_path(schema_basename: str) -> Path:
    return USER_SCHEMA_FILES_DIR / f"{schema_file_slug(schema_basename)}.yaml"


def schema_export_value(value: Any, *, default: bool) -> bool:
    """Interpret YAML ``export``; preserved across ETL while Dataspot refreshes other fields."""
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = clean(value).lower()
    if text in {"", "none"}:
        return default
    return text not in {"false", "0", "no", "off"}


def _default_export(dataspot_attribute: str) -> bool:
    return clean(dataspot_attribute).lower() != "gdh_fid"


def _dataspot_attribute_from_field(field: dict[str, Any]) -> str:
    """Dataspot / GeoJSON column name (legacy key: ``technical_name`` on orig rows)."""
    return clean(field.get("dataspot_attribute")) or clean(field.get("technical_name"))


def _huwise_technical_name_from_field(field: dict[str, Any], *, dataspot_attribute: str) -> str:
    """HUWISE column name from user YAML (``technical_name``) or legacy keys."""
    hw = clean(field.get("technical_name"))
    if hw and hw != dataspot_attribute:
        return hw
    legacy = clean(field.get("huwise_technical_name"))
    if legacy:
        return legacy
    custom = field.get("custom")
    if isinstance(custom, dict):
        hw = clean(custom.get("technical_name"))
        if hw:
            return hw
    return dataspot_attribute


def _orig_field_row(field: dict[str, Any]) -> dict[str, Any]:
    """Orig schema: Dataspot metadata only — no ``export``."""
    return {
        "technical_name": _dataspot_attribute_from_field(field),
        "name": clean(field.get("name")),
        "description": clean(field.get("description")),
        "mehrwertigkeit": clean(field.get("mehrwertigkeit")),
        "datentyp": clean(field.get("datentyp")) or "text",
    }


def build_orig_schema_payload(
    *,
    huwise_id: str,
    dataspot_asset_url: str,
    stac_url: str,
    fields: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "huwise_id": clean(huwise_id),
        "dataspot_asset_url": clean(dataspot_asset_url),
        "stac_url": clean(stac_url),
        "fields": [_orig_field_row(f) for f in fields if isinstance(f, dict) and _dataspot_attribute_from_field(f)],
    }


def _fields_index_by_dataspot(payload: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    if not payload:
        return {}
    out: dict[str, dict[str, Any]] = {}
    for field in payload.get("fields", []):
        if isinstance(field, dict):
            ds = _dataspot_attribute_from_field(field)
            if ds:
                out[ds] = field
    return out


def _merge_editorial_value(
    key: str,
    *,
    saved: dict[str, Any],
    new_orig: dict[str, Any],
    previous_orig: dict[str, Any] | None,
) -> str:
    """Keep user edits; refresh from Dataspot when the value still matched the last orig."""
    saved_val = clean(saved.get(key))
    new_val = clean(new_orig.get(key))
    prev_val = clean(previous_orig.get(key)) if previous_orig else ""
    if not saved_val:
        return new_val
    if prev_val and saved_val == prev_val:
        return new_val
    if saved_val == new_val:
        return new_val
    return saved_val


def _user_settings_from_field(field: dict[str, Any]) -> dict[str, Any]:
    ds = _dataspot_attribute_from_field(field)
    settings: dict[str, Any] = {
        "export": schema_export_value(field.get("export"), default=_default_export(ds)),
        "technical_name": _huwise_technical_name_from_field(field, dataspot_attribute=ds),
    }
    for key in EDITORIAL_KEYS:
        settings[key] = clean(field.get(key))
    return settings


def _user_field_row(
    orig_field: dict[str, Any],
    saved: dict[str, Any],
    *,
    previous_orig_field: dict[str, Any] | None = None,
) -> dict[str, Any]:
    ds = _dataspot_attribute_from_field(orig_field)
    hw = clean(saved.get("technical_name")) or _huwise_technical_name_from_field(saved, dataspot_attribute=ds)
    if not hw:
        hw = ds
    row: dict[str, Any] = {
        "dataspot_attribute": ds,
        "technical_name": hw,
        "export": schema_export_value(saved.get("export"), default=_default_export(ds)),
    }
    for key in EDITORIAL_KEYS:
        row[key] = _merge_editorial_value(
            key, saved=saved, new_orig=orig_field, previous_orig=previous_orig_field
        )
        if key == "datentyp" and not row[key]:
            row[key] = "text"
    return row


def build_user_schema_payload(
    orig_payload: dict[str, Any],
    *,
    preserved_settings: dict[str, dict[str, Any]] | None = None,
    previous_orig_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """User file: Dataspot metadata with preserved HUWISE/export/editorial overrides."""
    preserved_settings = preserved_settings or {}
    previous_by_ds = _fields_index_by_dataspot(previous_orig_payload)
    fields_out: list[dict[str, Any]] = []
    for field in orig_payload.get("fields", []):
        if not isinstance(field, dict):
            continue
        ds = _dataspot_attribute_from_field(field)
        if not ds:
            continue
        saved = preserved_settings.get(ds, {})
        fields_out.append(
            _user_field_row(field, saved, previous_orig_field=previous_by_ds.get(ds))
        )
    return {
        "huwise_id": clean(orig_payload.get("huwise_id")),
        "dataspot_asset_url": clean(orig_payload.get("dataspot_asset_url")),
        "stac_url": clean(orig_payload.get("stac_url")),
        "fields": fields_out,
    }


def _load_preserved_settings(path: Path | None) -> dict[str, dict[str, Any]]:
    if path is None or not path.is_file():
        return {}
    payload = load_yaml_payload(path)
    if not payload:
        return {}
    out: dict[str, dict[str, Any]] = {}
    for field in payload.get("fields", []):
        if not isinstance(field, dict):
            continue
        ds = _dataspot_attribute_from_field(field)
        if ds:
            out[ds] = _user_settings_from_field(field)
    return out


def sync_user_schema_file(
    schema_basename: str,
    orig_payload: dict[str, Any],
    *,
    previous_orig_payload: dict[str, Any] | None = None,
) -> Path | None:
    if not clean(orig_payload.get("huwise_id")):
        return None
    user_path = schema_user_path(schema_basename)
    preserved = _load_preserved_settings(user_path)
    user_payload = build_user_schema_payload(
        orig_payload,
        preserved_settings=preserved,
        previous_orig_payload=previous_orig_payload,
    )
    user_path.parent.mkdir(parents=True, exist_ok=True)
    user_path.write_text(dump_yaml(user_payload), encoding="utf-8")
    return user_path


def _publish_field_row(orig_field: dict[str, Any], user_field: dict[str, Any] | None) -> dict[str, Any]:
    """Row shape for publish (internal ``technical_name`` = Dataspot column)."""
    row = _orig_field_row(orig_field)
    ds = row["technical_name"]
    default_export = _default_export(ds)
    export = default_export
    hw = ds
    if user_field:
        export = schema_export_value(user_field.get("export"), default=default_export)
        hw = _huwise_technical_name_from_field(user_field, dataspot_attribute=ds)
        for key in EDITORIAL_KEYS:
            value = clean(user_field.get(key))
            if value:
                row[key] = value
        datentyp = clean(user_field.get("datentyp"))
        if datentyp:
            row["datentyp"] = datentyp
    row["export"] = export
    if hw != ds:
        row["custom"] = {
            "technical_name": hw,
            "name": "",
            "description": "",
            "datentyp": "",
            "mehrwertigkeit": "",
        }
    return row


def apply_user_settings_to_fields(
    orig_fields: list[dict[str, Any]],
    user_fields: list[dict[str, Any]] | None,
) -> list[dict[str, Any]]:
    user_by_ds: dict[str, dict[str, Any]] = {}
    if user_fields:
        for item in user_fields:
            if isinstance(item, dict):
                key = _dataspot_attribute_from_field(item)
                if key:
                    user_by_ds[key] = item

    merged: list[dict[str, Any]] = []
    for field in orig_fields:
        if not isinstance(field, dict):
            continue
        ds = _dataspot_attribute_from_field(field)
        merged.append(_publish_field_row(field, user_by_ds.get(ds)))
    return merged


def load_yaml_payload(path: Path) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise RuntimeError(f"Cannot parse YAML file {path}: {exc}") from exc
    return payload if isinstance(payload, dict) else None


def load_user_schema(schema_basename: str) -> dict[str, Any] | None:
    return load_yaml_payload(schema_user_path(schema_basename))


def load_merged_schema_payload(
    schema_basename: str,
    *,
    huwise_id: str,
    dataspot_dataset_id: str,
) -> dict[str, Any] | None:
    _ = dataspot_dataset_id
    orig = load_yaml_payload(schema_orig_path(schema_basename))
    if orig is None:
        return None
    user = load_user_schema(schema_basename)
    fields = orig.get("fields")
    if not isinstance(fields, list):
        fields = []
    user_fields = user.get("fields") if isinstance(user, dict) else None
    if isinstance(user_fields, list):
        fields = apply_user_settings_to_fields(fields, user_fields)
    merged = dict(orig)
    merged["huwise_id"] = clean(huwise_id) or clean(orig.get("huwise_id"))
    merged["fields"] = fields
    return merged


def override_fields_for_etl(
    schema_basename: str,
    *,
    huwise_id: str,
    dataspot_dataset_id: str,
) -> list[dict[str, Any]] | None:
    """Preserved rows for ETL from ``data/schema_files`` (export + HUWISE rename)."""
    _ = huwise_id, dataspot_dataset_id
    user = load_user_schema(schema_basename)
    if not user:
        return None
    fields = user.get("fields")
    if not isinstance(fields, list):
        return None
    preserved: list[dict[str, Any]] = []
    for item in fields:
        if not isinstance(item, dict):
            continue
        ds = _dataspot_attribute_from_field(item)
        if not ds:
            continue
        settings = _user_settings_from_field(item)
        row: dict[str, Any] = {
            "technical_name": ds,
            "name": "",
            "description": "",
            "datentyp": "",
            "mehrwertigkeit": "",
            "export": settings["export"],
            "custom": {},
        }
        hw = clean(settings["technical_name"])
        if hw and hw != ds:
            row["custom"] = {
                "technical_name": hw,
                "name": "",
                "description": "",
                "datentyp": "",
                "mehrwertigkeit": "",
            }
        preserved.append(row)
    return preserved or None


def resolve_schema_basename_for(
    huwise_id: str,
    dataspot_dataset_id: str,
    *,
    search_dir: Path = ORIG_SCHEMA_FILES_DIR,
) -> str | None:
    _ = dataspot_dataset_id
    if not search_dir.exists():
        return None
    huwise_norm = clean(huwise_id)
    for path in sorted(search_dir.glob("*.yaml")):
        payload = load_yaml_payload(path)
        if not payload:
            continue
        if huwise_norm and clean(payload.get("huwise_id")) == huwise_norm:
            return path.stem
    if huwise_norm and USER_SCHEMA_FILES_DIR.exists():
        for path in sorted(USER_SCHEMA_FILES_DIR.glob("*.yaml")):
            payload = load_yaml_payload(path)
            if payload and clean(payload.get("huwise_id")) == huwise_norm:
                return path.stem
    return None


def geometa_stac_url(collection_id: str, dataspot_dataset_id: str) -> str:
    return GEOMETA_DATASET_HTML_URL.format(
        collection_id=clean(collection_id),
        dataspot_dataset_id=clean(dataspot_dataset_id).lower(),
    )
