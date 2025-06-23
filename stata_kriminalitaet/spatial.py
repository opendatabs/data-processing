from __future__ import annotations

import logging

import geopandas as gpd
import pandas as pd
from pyproj import Transformer
from rapidfuzz import process
from shapely.geometry import Point

from config import CRS_CH_LV95, CRS_WGS84  # same as before


def to_wgs84(x_lv95: float, y_lv95: float) -> tuple[float, float]:
    """
    Convert Swiss LV95 (EPSG-2056) to WGS84 (lon, lat).
    """
    transformer = Transformer.from_crs(CRS_CH_LV95, CRS_WGS84, always_xy=True)
    return transformer.transform(x_lv95, y_lv95)


def enrich_with_wgs(df: pd.DataFrame) -> gpd.GeoDataFrame:
    """
    For rows containing LV95 columns 'ort_x', 'ort_y', add a geometry column
    (WGS84). Rows with (-1,-1) remain NaN.
    """
    mask = (df["ort_x"] != -1) & (df["ort_y"] != -1)
    df = df.copy()
    df.loc[mask, "geometry"] = [Point(*to_wgs84(x, y)) for x, y in df.loc[mask, ["ort_x", "ort_y"]].values]
    return gpd.GeoDataFrame(df, geometry="geometry", crs=CRS_WGS84)


def assign_plz(gdf: gpd.GeoDataFrame, gdf_plz: gpd.GeoDataFrame) -> pd.Series:
    """
    Spatial join that returns the PLZ (as string) for each point geometry.
    """
    joined = gpd.sjoin(gdf, gdf_plz.to_crs(CRS_WGS84), how="left", predicate="within")
    return joined["plz"].astype("string")


def assign_wohnviertel(gdf: gpd.GeoDataFrame, viertel: gpd.GeoDataFrame) -> pd.Series:
    """
    Spatial join → Wohnviertel name for every point (None if outside).
    """
    joined = gpd.sjoin(gdf, viertel.to_crs(CRS_WGS84), how="left", predicate="within")
    return joined["wov_name"]


def assign_plu(df_pts: pd.DataFrame, plz: gpd.GeoDataFrame) -> pd.Series:
    gdf_pts = gpd.GeoDataFrame(
        df_pts,
        geometry=gpd.points_from_xy(df_pts["ort_x"], df_pts["ort_y"]),
        crs=CRS_CH_LV95,
    )
    joined = gpd.sjoin(gdf_pts, plz.to_crs(CRS_CH_LV95), how="left", predicate="within")
    return joined["plz"]


def _closest_streetname(query: str, candidates: pd.Series, *, threshold: int = 85) -> str | None:
    """
    Best fuzzy match for *query* in *candidates* or None if below threshold.
    """
    result = process.extractOne(query, candidates, score_cutoff=threshold)
    return result[0] if result else None


def match_street_plz(
    df: pd.DataFrame,
    gdf_streets: gpd.GeoDataFrame,
    gdf_plz: gpd.GeoDataFrame,
    *,
    threshold: int = 85,
) -> pd.DataFrame:
    """
    Adds
        • Strasse_PLZ          – PLZ with longest overlap
        • Pct_Strasse_PLZ      – that overlap expressed as share of street length
    """
    crs_proj = CRS_CH_LV95
    streets = gdf_streets.to_crs(crs_proj)
    plz_polys = gdf_plz.to_crs(crs_proj)

    df[["Strasse_PLZ", "Pct_Strasse_PLZ"]] = None

    for name in df["Strasse"].dropna().unique():
        seg = streets[streets["strname"] == name]
        if seg.empty:
            fuzzy = _closest_streetname(name, streets["strname"], threshold=threshold)
            if fuzzy:
                seg = streets[streets["strname"] == fuzzy]

        if seg.empty:
            continue

        shape = seg.geometry.iloc[0]
        inter = gpd.overlay(
            gpd.GeoDataFrame(geometry=[shape], crs=crs_proj),
            plz_polys,
            how="intersection",
            keep_geom_type=False,
        )
        if inter.empty:
            continue

        inter["len"] = inter.geometry.length
        best = inter.loc[inter["len"].idxmax()]
        best_plz = best["plz"]
        pct_len = best["len"] / shape.length

        mask = df["Strasse"] == name
        df.loc[mask, ["Strasse_PLZ", "Pct_Strasse_PLZ"]] = (best_plz, pct_len)

    return df


def match_street_shapes(
    df: pd.DataFrame,
    gdf_streets: gpd.GeoDataFrame,
    gdf_viertel: gpd.GeoDataFrame,
    *,
    threshold: int = 85,
) -> pd.DataFrame:
    """
    • Finds the street geometry (exact or fuzzy).
    • Intersects it with Wohnviertel polygons (ODS 100042).
    • Picks the polygon covering the largest length fraction.

    Columns added / updated:
        street_shape           – shapely geometry (proj. CH LV95)
        matched_streetname     – the ODS street name actually used
        Strasse_Wohnviertel    – wov_name of the dominating polygon
        Pct_Strasse_Wohnviertel– share of the street inside that polygon
        Strasse_Gemeinde       – gemeinde_name of the same polygon
    """
    crs_proj = CRS_CH_LV95
    streets = gdf_streets.to_crs(crs_proj)
    viertel = gdf_viertel.to_crs(crs_proj)

    out = df.copy()
    out[
        [
            "street_shape",
            "matched_streetname",
            "Strasse_Wohnviertel",
            "Pct_Strasse_Wohnviertel",
            "Strasse_Gemeinde",
        ]
    ] = None

    for name in out["Strasse"].dropna().unique():
        seg = streets[streets["strname"] == name]

        if seg.empty:
            fuzzy = _closest_streetname(name, streets["strname"], threshold=threshold)
            if fuzzy:
                seg = streets[streets["strname"] == fuzzy]
                logging.info("Fuzzy-matched '%s' → '%s'", name, fuzzy)

        if seg.empty:
            logging.info("Street '%s' not found in ODS 100189", name)
            continue

        shape = seg.geometry.iloc[0]
        street_gdf = gpd.GeoDataFrame({"geom": [shape]}, geometry="geom", crs=crs_proj)

        inter = gpd.overlay(street_gdf, viertel, how="intersection", keep_geom_type=False)
        if inter.empty:
            best_viertel = best_pct = best_gemeinde = None
        else:
            inter["len"] = inter.geometry.length
            pct_series = inter["len"] / shape.length
            idx = pct_series.idxmax()
            best_viertel = inter.loc[idx, "wov_name"]
            best_pct = pct_series.loc[idx]
            best_gemeinde = inter.loc[idx, "gemeinde_na"]

        mask = out["Strasse"] == name
        out.loc[
            mask,
            [
                "street_shape",
                "matched_streetname",
                "Strasse_Wohnviertel",
                "Pct_Strasse_Wohnviertel",
                "Strasse_Gemeinde",
            ],
        ] = (shape, seg.iloc[0]["strname"], best_viertel, best_pct, best_gemeinde)

    return out
