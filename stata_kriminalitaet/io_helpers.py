from __future__ import annotations

import io
import json
import logging
import zipfile
from pathlib import Path
from typing import Any

import common
import geopandas as gpd
import pandas as pd

import config
from config import DATA_DIR


def read_pks() -> pd.DataFrame:
    """Raw PKS crime data (one row per offence)."""
    return pd.read_csv(config.PKS_CSV, encoding="windows-1252")


def download_shapefile(ods_id: str) -> gpd.GeoDataFrame:
    """
    Fetch a zipped shapefile from Basel ODS and return as GeoDataFrame.
    """
    dest = DATA_DIR / ods_id
    url = f"https://data.bs.ch/explore/dataset/{ods_id}/download/?format=shp"
    logging.info("Downloading %s …", ods_id)
    resp = common.requests_get(url)
    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        zf.extractall(dest)
    shp_path = next(dest.glob("*.shp"))
    return gpd.read_file(shp_path, encoding="utf-8")


def load_cache(path: Path) -> dict[str, Any]:
    if path.exists():
        return json.loads(path.read_text())
    return {}


def save_cache(path: Path, cache: dict[str, Any]) -> None:
    path.write_text(json.dumps(cache, indent=2, ensure_ascii=False))


def download_gwr():
    raw_data_file = DATA_DIR / "gebaeudeeingaenge.csv"
    logging.info(f"Downloading Gebäudeeingänge from ods to file {raw_data_file}...")
    r = common.requests_get("https://data.bs.ch/api/records/1.0/download?dataset=100231")
    with open(raw_data_file, "wb") as f:
        f.write(r.content)
    return pd.read_csv(raw_data_file, sep=";")
