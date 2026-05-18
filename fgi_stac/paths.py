"""Filesystem layout for fgi_stac pipeline vs user-editable data."""

from __future__ import annotations

from pathlib import Path

DATA_ORIG_DIR = Path("data_orig")
DATA_DIR = Path("data")
TRANSFORMS_DIR = Path("transforms")

ORIG_CATALOG_FILE = DATA_ORIG_DIR / "publish_catalog.yaml"
ORIG_METADATA_LAST_PUSH_FILE = DATA_ORIG_DIR / "publish_metadata_last_push.yaml"
ORIG_DATASETS_DIR = DATA_ORIG_DIR / "datasets"
ORIG_SCHEMA_FILES_DIR = DATA_ORIG_DIR / "schema_files"

BINDINGS_FILE = DATA_DIR / "huwise_bindings.xlsx"
USER_SCHEMA_FILES_DIR = DATA_DIR / "schema_files"
PUBLISH_DATASETS_DIR = DATA_DIR / "datasets"

GEOMETA_DATASET_HTML_URL = (
    "https://api.geo.bs.ch/geometa/v1/metadata_details/dataset/published/html/{collection_id}#{dataspot_dataset_id}"
)

# Legacy paths (pre data_orig split)
# Fallback, falls noch alte Pfade im Exchange-Ordner liegen
LEGACY_CATALOG_FILE = DATA_DIR / "publish_catalog.yaml"
LEGACY_METADATA_LAST_PUSH_FILE = DATA_DIR / "publish_metadata_last_push.yaml"


def ensure_layout_dirs() -> None:
    """Create expected directories if missing."""
    for path in (
        DATA_ORIG_DIR,
        ORIG_DATASETS_DIR,
        ORIG_SCHEMA_FILES_DIR,
        DATA_DIR,
        USER_SCHEMA_FILES_DIR,
        PUBLISH_DATASETS_DIR,
        TRANSFORMS_DIR,
    ):
        path.mkdir(parents=True, exist_ok=True)
