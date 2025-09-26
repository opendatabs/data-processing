import json
import logging
from pathlib import Path

import common
import geopandas as gpd
import pandas as pd

CRS = "EPSG:4326"


def _make_hashable(v):
    """Recursively turn lists/dicts into hashables for nunique checks."""
    if isinstance(v, list):
        return tuple(_make_hashable(x) for x in v)
    if isinstance(v, dict):
        return tuple(sorted((k, _make_hashable(v[k])) for k in v))
    try:
        from shapely.geometry.base import BaseGeometry

        if isinstance(v, BaseGeometry):
            return v.wkt
    except Exception:
        pass
    return v


def log_intra_group_differences(df: pd.DataFrame, group_col: str, cols: list[str], max_values: int = 10):
    """
    For each group, log columns where values differ and show the set of distinct values.
    Writes a CSV report too (data/inconsistent_attributes.csv).
    """
    rows = []
    for gid, sub in df.groupby(group_col, dropna=False):
        for c in cols:
            vals = list(sub[c])  # keep originals for display
            hashed = {_make_hashable(v) for v in vals}
            if len(hashed) > 1:
                # build a compact, readable list of unique originals
                uniq = []
                seen = set()
                for v in vals:
                    hv = _make_hashable(v)
                    if hv not in seen:
                        seen.add(hv)
                        uniq.append(v)
                sample = uniq[:max_values]
                logging.warning(
                    "BelegungID %s: column '%s' has %d distinct values. Sample: %s",
                    gid,
                    c,
                    len(uniq),
                    [repr(x) for x in sample],
                )
                rows.append(
                    {
                        "BelegungID": gid,
                        "column": c,
                        "num_distinct": len(uniq),
                        "distinct_values_sample": "; ".join(repr(x) for x in sample),
                    }
                )
    if rows:
        Path("data").mkdir(exist_ok=True)
        pd.DataFrame(rows).to_csv("data/inconsistent_attributes.csv", index=False)
        logging.warning("Wrote inconsistency report to data/inconsistent_attributes.csv")


def get_allmendbewilligungen() -> gpd.GeoDataFrame:
    url = "https://data.bs.ch/explore/dataset/100018/download/"
    r = common.requests_get(
        url,
        params={
            "format": "geojson",
            "refine.belgartid": 7,
        },
    )
    r.raise_for_status()
    gj = r.json()
    gdf = gpd.GeoDataFrame.from_features(gj["features"], crs=CRS)
    gdf["geometry"] = gdf["geometry"].buffer(0)

    # rename technical → title
    with open("data_orig/allmend_fields.json", "r", encoding="utf-8") as f:
        colmap = json.load(f)
    gdf = gdf.rename(columns=colmap)

    if "BelegungID" not in gdf.columns:
        raise ValueError("Column 'BelegungID' not found after renaming.")

    # Validate sameness of attributes within BelegungID (ignore geometry, IDUnique, and BelegungID itself)
    meta_cols = [c for c in gdf.columns if c not in ("IDUnique", "Geo Point", "geometry", "BelegungID")]

    # Hashable view only for nunique()
    hashable_view = gdf[meta_cols].map(_make_hashable)  # applymap -> map (pandas >= 2.2)
    nunq = hashable_view.groupby(gdf["BelegungID"]).nunique(dropna=False)
    bad = nunq[(nunq > 1).any(axis=1)]

    if not bad.empty:
        logging.warning("Some attributes differ within BelegungID(s): %s", ", ".join(map(str, bad.index.tolist())))
        # Build report frame WITHOUT duplicate 'BelegungID'
        report_cols = ["BelegungID"] + meta_cols
        report_df = gdf.loc[:, report_cols]
        log_intra_group_differences(report_df, "BelegungID", meta_cols)

    # Aggregate: first for meta, unary_union for geometry
    agg_dict = {c: "first" for c in meta_cols}
    agg_dict["geometry"] = lambda s: s.unary_union

    grouped = gdf.groupby("BelegungID", as_index=False).agg(agg_dict)

    # Optional: keep list of IDUnique values per BelegungID
    if "IDUnique" in gdf.columns:
        idu = (
            gdf.groupby("BelegungID")["IDUnique"]
            .agg(lambda s: sorted({x for x in s if pd.notna(x)}))
            .rename("IDUnique_list")
        )
        grouped = grouped.merge(idu, on="BelegungID", how="left")

    grouped = gpd.GeoDataFrame(grouped, geometry="geometry", crs=CRS)
    return grouped


def get_perimeter_and_puffer(path: str = "data_orig/SBT_Perimeter_und_Puffer.json") -> gpd.GeoDataFrame:
    logging.info("Loading perimeter data from %s", path)
    perim = gpd.read_file(path)
    if perim.crs is None:
        logging.warning("Perimeter has no CRS; assuming EPSG:4326.")
        perim.set_crs(CRS, inplace=True)
    elif perim.crs.to_string() != CRS:
        perim = perim.to_crs(CRS)

    if "SBT_Perimeter" not in perim.columns:
        raise ValueError("Expected 'SBT_Perimeter' property in perimeter data.")
    perim = perim.reset_index(drop=True)
    perim["perimeter_name"] = perim["SBT_Perimeter"]
    perim["geometry"] = perim["geometry"].buffer(0)
    return perim[["perimeter_name", "geometry"]]


def build_intersections(perim: gpd.GeoDataFrame, alm: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    # Prepare perimeter for join
    perim_for_join = perim.rename(columns={"geometry": "perim_geom"}).set_geometry("perim_geom")

    # Spatial join (alm × perimeter/puffer)
    joined = (
        gpd.sjoin(alm, perim_for_join, how="inner", predicate="intersects")
        .rename_geometry("allmend_geom")
        .reset_index(drop=True)
    )

    # Parse area ("StJohann"/"Matthaeus") and kind ("Perimeter"/"Puffer")
    def _parse_area_kind(name: str):
        base = name.removeprefix("SBT_")
        if base.endswith("_Perimeter"):
            return base[:-10], "Perimeter"
        if base.endswith("_Puffer"):
            return base[:-7], "Puffer"
        return base, "Perimeter"

    parsed = joined["perimeter_name"].apply(_parse_area_kind)
    joined["area"] = parsed.apply(lambda x: x[0])
    joined["kind"] = parsed.apply(lambda x: x[1])

    # Prefer Perimeter over Puffer within (BelegungID, area)
    joined["kind_priority"] = joined["kind"].map({"Perimeter": 0, "Puffer": 1}).fillna(1)
    joined = (
        joined.sort_values(["BelegungID", "area", "kind_priority"])
        .drop_duplicates(subset=["BelegungID", "area"], keep="first")
        .drop(columns=["kind_priority"])
    )

    # Intersections
    perim_lookup = perim.set_index("perimeter_name")["geometry"]

    def _intersect(row):
        return row["allmend_geom"].intersection(perim_lookup.loc[row["perimeter_name"]])

    logging.info("Computing intersections…")
    joined["intersection"] = joined.apply(_intersect, axis=1)

    # Centroids
    def _centroid(g):
        if g is None or g.is_empty:
            return None
        try:
            return g.centroid
        except Exception:
            return None

    joined["intersection_centroid"] = joined["intersection"].apply(_centroid)

    # Drop empties
    joined = joined[~joined["intersection"].is_empty].copy()

    # Build GeoDataFrame with intersection as temp geometry (we’ll switch to centroid later)
    out = gpd.GeoDataFrame(joined, geometry="intersection", crs=CRS)

    # Keep perimeter geometry for audit (WKT later)
    out["perimeter_geom"] = out["perimeter_name"].map(perim_lookup)

    return out


def make_centroid_output(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Return a GeoDataFrame with ONLY centroid as geometry.
    All other geometries converted to WKT columns to avoid multiple-geometry error.
    """
    df = gdf.copy()

    # WKT snapshots for auditability
    df["intersection_wkt"] = df["intersection"].to_wkt()
    df["allmend_geom_wkt"] = df["allmend_geom"].to_wkt()
    df["perimeter_geom_wkt"] = df["perimeter_geom"].to_wkt()

    # Drop extra geometry-typed columns
    drop_geom_cols = []
    for col in ["intersection", "allmend_geom", "perimeter_geom"]:
        if col in df.columns:
            drop_geom_cols.append(col)
    df = df.drop(columns=drop_geom_cols)

    # Set geometry to centroid and drop rows with missing centroids
    df = gpd.GeoDataFrame(df, geometry="intersection_centroid", crs=CRS)
    df = df[~df["intersection_centroid"].isna()].copy()

    return df


def write_outputs(centroids_gdf: gpd.GeoDataFrame, data_dir: str = "data") -> None:
    Path(data_dir).mkdir(exist_ok=True)
    gpkg_path = f"{data_dir}/sbt_allmend_centroids.gpkg"
    geojson_path = f"{data_dir}/sbt_allmend_centroids.geojson"
    csv_path = f"{data_dir}/sbt_allmend_centroids.csv"

    # GPKG & GeoJSON (geometry = centroid only)
    centroids_gdf.to_file(gpkg_path, layer="centroids", driver="GPKG")
    centroids_gdf.to_file(geojson_path, driver="GeoJSON")

    # CSV: include centroid lon/lat plus WKT columns
    df_csv = pd.DataFrame(centroids_gdf.drop(columns=centroids_gdf.geometry.name))
    df_csv["centroid_x"] = centroids_gdf.geometry.x
    df_csv["centroid_y"] = centroids_gdf.geometry.y
    df_csv.to_csv(csv_path, index=False)

    logging.info("Wrote:\n  %s\n  %s\n  %s", gpkg_path, geojson_path, csv_path)


def main():
    logging.info("Loading datasets…")
    allmend = get_allmendbewilligungen()
    perimeters = get_perimeter_and_puffer()

    if len(perimeters) != 4:
        logging.warning("Expected 4 perimeter polygons; found %s.", len(perimeters))

    logging.info("Computing intersections and centroids…")
    result = build_intersections(perimeters, allmend)

    # Build centroid-only geometry output (others as WKT)
    centroids_gdf = make_centroid_output(result)
    write_outputs(centroids_gdf, data_dir="data")

    logging.info("Job successful!")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
