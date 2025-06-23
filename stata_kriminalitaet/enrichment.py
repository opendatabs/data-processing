from __future__ import annotations

import logging
from typing import Any

import geopandas as gpd
import numpy as np
import pandas as pd
from config import COLUMNS_OF_INTEREST, CRS_CH_LV95, CRS_WGS84
from geocoding import Geocoder
from pyproj import Transformer
from shapely.geometry import Point
from spatial import (
    assign_plz,
    assign_wohnviertel,
    enrich_with_wgs,
    match_street_plz,
    match_street_shapes,
)

_LV95_to_WGS84 = Transformer.from_crs(CRS_CH_LV95, CRS_WGS84, always_xy=True)


def _street_centroid_wgs(shape) -> tuple[float, float] | None:
    """
    Convert centroid of a street *LineString/MultiLineString*
    (in LV95) to (lat, lon).  Returns None if shape is None.
    """
    if shape is None or shape.is_empty:
        return None
    cx, cy = shape.centroid.coords[0]
    lon, lat = _LV95_to_WGS84.transform(cx, cy)
    return (lat, lon)


def _dist_to_ortxy(row) -> float | None:
    """
    Euclidean distance (LV95 metres) between the street centroid
    and the original ort_x/ort_y reference point.
    """
    if row["street_shape"] is None or row["ort_x"] == -1 or row["ort_y"] == -1:
        return None
    return row["street_shape"].centroid.distance(Point(row["ort_x"], row["ort_y"]))


def _has_value(x: Any) -> bool:
    """True if x is not None / NA / NaN."""
    if x is None or x is pd.NA:
        return False
    if isinstance(x, float) and np.isnan(x):
        return False
    return True  # lists/tuples are truthy if non-empty


def add_availability_flags(
    df: pd.DataFrame,
    *,
    include_georef: bool = False,
    include_strasse_centroid: bool = False,
) -> pd.DataFrame:
    """
    Adds *_avail Boolean columns in-place and returns df.
    """

    def coords_ok(r) -> bool:
        # 1) raw LV95
        if (r["ort_x"] != -1) and (r["ort_y"] != -1):
            return True
        # 2) geocoder
        if include_georef and _has_value(r.get("Georef_coords")):
            return True
        # 3) street centroid
        if include_strasse_centroid and _has_value(r.get("Strasse_coords")):
            return True
        return False

    rules = {
        "Koordinaten_avail": coords_ok,
        "Strasse_avail": lambda r: r["Strasse"] != "unbekannt",
        "Hausnummer_avail": lambda r: r["Hausnummer"] != "unbekannt",
        "Ort_avail": lambda r: r["Ort"] != "unbekannt",
    }
    for col, rule in rules.items():
        df[col] = df.apply(rule, axis=1)

    return df


def plz_to_gemeinde(plz: str | int | None) -> str | None:
    if plz is None:
        return None
    plz = str(plz)
    if plz.startswith("40"):
        return "Basel"
    if plz == "4125":
        return "Riehen"
    if plz == "4126":
        return "Bettingen"
    return None


def annotate(
    df: pd.DataFrame,
    gdf_viertel: gpd.GeoDataFrame,
    gdf_streets: gpd.GeoDataFrame,
    gdf_plz: gpd.GeoDataFrame,
    gwr_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Full pipeline:
        1. direct LV95 → WGS points
        2. Wohnviertel via spatial join
        3. address geocoding (GWR → Nominatim → fuzzy)
        4. street-shape overlay
        5. distance diagnostics
    """
    # ------------------------------------------------------------------ #
    # 1. direct coordinate points
    # ------------------------------------------------------------------ #
    logging.info("Adding WGS84 coordinates …")
    gdf = enrich_with_wgs(df)
    logging.info("Adding Wohnviertel …")
    df["Wohnviertel"] = assign_wohnviertel(gdf, gdf_viertel)
    logging.info("Adding PLZ …")
    df["PLZ"] = assign_plz(gdf, gdf_plz)

    # ------------------------------------------------------------------ #
    # 2. geocoder
    # ------------------------------------------------------------------ #
    logging.info("Geocoding addresses …")
    geocoder = Geocoder(gwr_df, gdf_viertel)

    def make_address(row) -> str:
        return f"{row['Strasse']} {row['Hausnummer']}, {row['Ort']}"

    df["address"] = df.apply(make_address, axis=1)
    df["Georef_coords"] = df["address"].map(geocoder.coordinates)

    # Point geometry from geocoded (lat, lon)
    df["Georef_geom"] = df["Georef_coords"].map(lambda t: Point(t[1], t[0]) if t else None)
    gdf_georef = gpd.GeoDataFrame(df.dropna(subset=["Georef_geom"]), geometry="Georef_geom", crs=CRS_WGS84)
    df.loc[gdf_georef.index, "Georef_Wohnviertel"] = assign_wohnviertel(gdf_georef, gdf_viertel)
    df.loc[gdf_georef.index, "Georef_PLZ"] = assign_plz(gdf_georef, gdf_plz)
    df["Georef_Gemeinde"] = df["Georef_PLZ"].map(plz_to_gemeinde)

    # ------------------------------------------------------------------ #
    # 3. street shapes (for rows w/o house number)
    # ------------------------------------------------------------------ #
    logging.info("Matching street shapes …")
    df = match_street_shapes(df, gdf_streets, gdf_viertel)
    df = match_street_plz(df, gdf_streets, gdf_plz)
    df["Strasse_Gemeinde"] = df["Strasse_PLZ"].map(plz_to_gemeinde)
    df["Strasse_coords"] = df["street_shape"].apply(_street_centroid_wgs)
    df["Dist_Strasse_vs_ortxy"] = df.apply(_dist_to_ortxy, axis=1)

    # ------------------------------------------------------------------ #
    # 4. diagnostic distances
    # ------------------------------------------------------------------ #
    to_lv95 = Transformer.from_crs(CRS_WGS84, CRS_CH_LV95, always_xy=True)

    def dist(a: Point | None, b: Point | None) -> float | None:
        if not (a and b):
            return None
        ax, ay = to_lv95.transform(a.x, a.y)
        bx, by = to_lv95.transform(b.x, b.y)
        return Point(ax, ay).distance(Point(bx, by))

    df["Dist_Georef_vs_ortxy"] = [dist(p1, p2) for p1, p2 in zip(df["Georef_geom"], gdf["geometry"], strict=False)]

    return df


def reconcile_wohnviertel(df: pd.DataFrame) -> pd.DataFrame:
    """
    • If Georef_geom exists and Wohnviertel empty → copy Georef_Wohnviertel.
    • Else, if Wohnviertel empty AND
        ▸ Pct_Strasse_Wohnviertel == 1  AND
        ▸ every coord-bearing row of that street agrees on a single Wov
      → fill from Strasse_Wohnviertel.
    """

    # ------------------------------------------------------------------
    # Ensure *_avail flags exist
    # ------------------------------------------------------------------
    if "Koordinaten_avail" not in df.columns:
        df = add_availability_flags(
            df,
            include_georef=True,
            include_strasse_centroid=True,
        )

    # 1) fill from geocoder match
    mask = df["Wohnviertel"].isna() & df["Georef_geom"].notna()
    df.loc[mask, "Wohnviertel"] = df.loc[mask, "Georef_Wohnviertel"]

    # 2) street-consensus fallback
    coords_mask = df["Koordinaten_avail"]
    consensus = df[coords_mask].groupby("Strasse")["Strasse_Wohnviertel"].nunique().eq(1)

    for street in consensus[consensus].index:
        good = (df["Strasse"] == street) & (df["Pct_Strasse_Wohnviertel"] > 0.9) & df["Wohnviertel"].isna()
        if good.any():
            viertel = df.loc[df["Strasse"] == street, "Strasse_Wohnviertel"].dropna().iat[0]
            df.loc[good, "Wohnviertel"] = viertel

    return df[COLUMNS_OF_INTEREST]
