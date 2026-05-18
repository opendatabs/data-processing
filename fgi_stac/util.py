"""Shared text, date, and GeoJSON helpers."""

from __future__ import annotations

import html
import json
import re
import time
from pathlib import Path
from typing import Any

_HTML_MARKER_RE = re.compile(
    r"</[a-zA-Z][\w-]*\s*>"
    r"|<[a-zA-Z][\w-]*[^<>]*?/\s*>"
    r"|<[a-zA-Z][\w-]*\s+[^<>]*?>"
)
_UUID_RE = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")


def clean(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def clean_text(value: Any) -> str:
    """Like :func:`clean` but treats pandas NaN as empty."""
    if value is None:
        return ""
    try:
        import pandas as pd

        if isinstance(value, float) and pd.isna(value):
            return ""
    except ImportError:
        pass
    return str(value).strip()


def normalize_name(value: str) -> str:
    text = clean(value).lower()
    for src, dst in (("ä", "ae"), ("ö", "oe"), ("ü", "ue"), ("ß", "ss")):
        text = text.replace(src, dst)
    return re.sub(r"[^a-z0-9]", "", text)


def normalize_huwise_field_name(value: str) -> str:
    text = clean(value).lower()
    text = re.sub(r"\s+", "_", text)
    text = re.sub(r"[^a-z0-9_]", "_", text)
    return re.sub(r"_+", "_", text).strip("_")


def normalize_geo_dataset_match_keys(geo_dataset: str) -> list[str]:
    keys: list[str] = []
    seen: set[str] = set()
    base = normalize_name(geo_dataset)
    if base and base not in seen:
        seen.add(base)
        keys.append(base)
    text = clean(geo_dataset)
    if text and re.search(r"\bund\b", text, re.IGNORECASE):
        parts = re.split(r"\s+und\s+", text, flags=re.IGNORECASE)
        merged = "".join(normalize_name(part) for part in parts if clean(part))
        if merged and merged not in seen:
            seen.add(merged)
            keys.append(merged)
    if text and "(" in text:
        stripped = re.sub(r"\s*\([^)]*\)\s*", " ", text)
        stripped_key = normalize_name(stripped)
        if stripped_key and stripped_key not in seen:
            seen.add(stripped_key)
            keys.append(stripped_key)
    return keys


def third_path_segment(path_value: Any) -> str:
    parts = [part.strip() for part in clean(path_value).split("/") if part.strip()]
    return parts[2] if len(parts) > 2 else ""


def description_to_html(value: Any) -> str:
    text = clean(value)
    if not text:
        return ""
    if _HTML_MARKER_RE.search(text):
        return text
    paragraphs = [part.strip() for part in re.split(r"\n\s*\n", text) if part.strip()]
    if not paragraphs:
        return html.escape(text).replace("\n", "<br>")
    return "\n".join(f"<p>{html.escape(part).replace(chr(10), '<br>')}</p>" for part in paragraphs)


def normalize_optional_date(value: Any) -> str:
    text = clean(value)
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


def split_keywords(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(part).strip() for part in value if str(part).strip()]
    normalized = clean_text(value).replace(";", ",")
    return [part.strip() for part in normalized.split(",") if part.strip()]


def split_semicolon_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    text = clean_text(value)
    if not text:
        return []
    return [part.strip() for part in text.split(";") if part.strip()]


def extract_string_list(value: Any) -> list[str]:
    def _is_placeholder(text: str) -> bool:
        return text.strip().lower() in {"[]", "[ ]", "null", "none", "nan"}

    if isinstance(value, list):
        items: list[str] = []
        for item in value:
            if isinstance(item, dict):
                label = clean(item.get("label") or item.get("title") or item.get("name"))
                if label and not _is_placeholder(label):
                    items.append(label)
            else:
                text = clean(item)
                if text and not _is_placeholder(text):
                    items.append(text)
        return items
    text = clean(value)
    if not text or _is_placeholder(text):
        return []
    return [text]


def extract_stac_code(value: Any) -> str:
    text = clean_text(value)
    if not text:
        return ""
    tail = text.rstrip("/").split("/")[-1]
    if re.fullmatch(r"[A-Za-z0-9_]{3,16}", tail):
        return tail.upper()
    if re.fullmatch(r"[A-Za-z0-9_]{3,16}", text):
        return text.upper()
    return ""


def is_uuid(value: str) -> bool:
    return bool(_UUID_RE.match(clean(value)))


def read_geojson_properties(path: Path) -> list[str]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    features = payload.get("features", [])
    if not features:
        return []
    properties = features[0].get("properties", {})
    if not isinstance(properties, dict):
        return []
    return [str(key) for key in properties.keys()]


def normalize_uuid(value: Any) -> str:
    return clean(value).lower()


def row_key(stac_collection_id: str, geo_dataset: str) -> tuple[str, str]:
    return (clean(stac_collection_id), clean(geo_dataset))
