"""Default identity transform applied when no dataset-specific module exists."""

from __future__ import annotations

import geopandas as gpd


def transform(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    return gdf
