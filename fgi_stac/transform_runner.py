"""Run optional per-dataset GeoJSON transforms before publish."""

from __future__ import annotations

import importlib.util
import logging
import os
from pathlib import Path

import geopandas as gpd
from paths import PUBLISH_DATASETS_DIR, TRANSFORMS_DIR


def _default_transform_path() -> Path:
    return TRANSFORMS_DIR / "_default.py"


def resolve_transform_module(stem: str) -> Path:
    """Return dataset-specific transform module or ``_default.py``."""
    specific = TRANSFORMS_DIR / f"{stem}.py"
    if specific.is_file():
        return specific
    default = _default_transform_path()
    if default.is_file():
        return default
    raise FileNotFoundError(
        f"No transform module for {stem!r} and missing {default}. "
        "Create transforms/_default.py with an identity transform()."
    )


def _load_transform_callable(module_path: Path):
    spec = importlib.util.spec_from_file_location(f"fgi_stac_transform_{module_path.stem}", module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load transform module: {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    transform = getattr(module, "transform", None)
    if not callable(transform):
        raise AttributeError(f"{module_path} must define transform(gdf) -> gdf")
    return transform


def run_transform(
    *,
    input_path: Path,
    output_path: Path,
    stem: str | None = None,
) -> Path:
    """Read GeoJSON from input_path, apply transform, write to output_path."""
    stem = stem or input_path.stem
    module_path = resolve_transform_module(stem)
    transform = _load_transform_callable(module_path)
    gdf = gpd.read_file(input_path)
    result = transform(gdf)
    if not isinstance(result, gpd.GeoDataFrame):
        raise TypeError(f"{module_path}: transform() must return a GeoDataFrame")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = output_path.with_suffix(output_path.suffix + ".tmp")
    result.to_file(tmp_path, driver="GeoJSON")
    os.replace(tmp_path, output_path)
    logging.info("Transform %s -> %s (module %s)", input_path.name, output_path.name, module_path.name)
    return output_path


def publish_geojson_path(stem: str) -> Path:
    return PUBLISH_DATASETS_DIR / f"{stem}.geojson"
