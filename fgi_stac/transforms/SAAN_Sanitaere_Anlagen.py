"""Derived columns for Sanitäre Anlagen (SAAN) before publish."""

from __future__ import annotations

import geopandas as gpd
import numpy as np

COL_PLZ = "plz"


def transform(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    out = gdf.copy()

    plz = out[COL_PLZ].astype(str).str.strip().str.replace(r"\.0$", "", regex=True)

    out["ort"] = np.select(
        [
            plz.eq("4125"),
            plz.eq("4126"),
            plz.str.startswith("40"),
        ],
        [
            "Riehen",
            "Bettingen",
            "Basel",
        ],
        default=None,
    )

    return out
