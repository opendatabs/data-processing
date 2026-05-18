"""YAML helpers for fgi_stac (readable huwise_id without quotes)."""

from __future__ import annotations

import copy
import re
from typing import Any

import yaml

from util import clean

_HUIWISE_ID_PATTERN = re.compile(r"^[0-9][0-9a-zA-Z_-]*$")


def _normalize_huwise_id_value(value: Any) -> Any:
    """Use int for pure numeric ids so YAML renders without quotes."""
    text = clean(value)
    if not text:
        return ""
    if text.isdigit():
        return int(text)
    return text


def _normalize_huwise_ids_tree(obj: Any) -> Any:
    if isinstance(obj, dict):
        out: dict[Any, Any] = {}
        for key, value in obj.items():
            out_key: Any = key
            if isinstance(key, str) and key.isdigit():
                out_key = int(key)
            if key == "huwise_id" or out_key == "huwise_id":
                out[out_key] = _normalize_huwise_id_value(value)
            else:
                out[out_key] = _normalize_huwise_ids_tree(value)
        return out
    if isinstance(obj, list):
        return [_normalize_huwise_ids_tree(item) for item in obj]
    return obj


class FgiYamlDumper(yaml.SafeDumper):
    """Dumper with folded blocks for long text and plain scalars for huwise ids."""


def _fgi_str_representer(dumper: yaml.SafeDumper, value: str) -> yaml.ScalarNode:
    if _HUIWISE_ID_PATTERN.match(value):
        return dumper.represent_scalar("tag:yaml.org,2002:str", value, style="")
    if "\n" in value or len(value) > 120:
        return dumper.represent_scalar("tag:yaml.org,2002:str", value, style=">")
    return dumper.represent_scalar("tag:yaml.org,2002:str", value)


FgiYamlDumper.add_representer(str, _fgi_str_representer)


def dump_yaml(payload: dict[str, Any], *, width: int = 10_000) -> str:
    """Dump a document with unquoted numeric ``huwise_id`` values where possible."""
    normalized = _normalize_huwise_ids_tree(copy.deepcopy(payload))
    return yaml.dump(
        normalized,
        Dumper=FgiYamlDumper,
        allow_unicode=True,
        sort_keys=False,
        width=width,
    )
