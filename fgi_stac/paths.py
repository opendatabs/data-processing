"""Shared file system paths for FGI STAC processing."""

from __future__ import annotations

from pathlib import Path

DATA_ORIG_DIR = Path("data_orig")
DATA_DIR = Path("data")
DATASETS_DIR = DATA_DIR / "datasets"
SCHEMAS_DIR = DATA_DIR / "schemas"


def ensure_output_dirs() -> None:
    """Create output directories that are managed by this project."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    DATASETS_DIR.mkdir(parents=True, exist_ok=True)
    SCHEMAS_DIR.mkdir(parents=True, exist_ok=True)


def resolve_input_file(filename: str) -> Path:
    """Return canonical input path from data_orig, with fallback to data."""
    primary = DATA_ORIG_DIR / filename
    if primary.exists():
        return primary
    return DATA_DIR / filename
