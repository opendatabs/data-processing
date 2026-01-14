import json
import logging
from pathlib import Path

import common
import common.change_tracking as ct
import geopandas as gpd
import pandas as pd
from shapely.wkb import dumps as wkb_dumps

CRS = "EPSG:4326"
ALLMEND_URL = "https://data.bs.ch/explore/dataset/100018/download/"
ALLMEND_CACHE = Path("data_orig/allmendbewilligungen.geojson")
PERIM_PATH = Path("data_orig/SBT_Perimeter_und_Puffer.json")


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


# ---- replacements for former lambdas ----


def _union_all(s):
    """Union all geometries in a Series into one geometry."""
    return gpd.GeoSeries(s, crs=CRS).union_all()


def _cent_sig_from_point(p):
    """Return rounded (x, y) tuple or None from a point-like geometry."""
    if p is None or getattr(p, "is_empty", False):
        return None
    return (round(p.x, 2), round(p.y, 2))


def _tuple_to_csv_or_none(t):
    """Stringify (x, y) tuple as 'x,y', else None."""
    if t is None:
        return None
    return f"{t[0]},{t[1]}"


def _unique_sorted_non_null(s):
    """Return sorted unique non-null values from a Series."""
    return sorted({x for x in s if pd.notna(x)})


def _list_merge(s):
    """Merge lists/values across a Series into a sorted unique flat list."""
    out = set()
    for v in s.dropna():
        if isinstance(v, (list, tuple)):
            out.update(v)
        else:
            out.add(v)
    return sorted(out)


def _pick_first(x):
    return x[0]


def _pick_second(x):
    return x[1]


def log_intra_group_differences(
    df: pd.DataFrame,
    group_keys: str | list[str],
    cols: list[str],
    *,
    group_label: str | None = None,
    max_values: int = 10,
    report_path: str = "data/inconsistent_attributes.csv",
):
    """
    For each group, log columns where values differ and show distinct values.
    Writes a CSV report (default: data/inconsistent_attributes.csv).
    - group_keys: a column name or list of column names to group by
    - group_label: label used in logs/CSV (defaults to group_keys joined by '+')
    """
    if isinstance(group_keys, str):
        by = [group_keys]
    else:
        by = list(group_keys)

    label = group_label or ("+".join(by) if len(by) > 1 else by[0])

    rows = []
    for gid, sub in df.groupby(by, dropna=False):
        # normalize gid display
        gid_disp = gid if isinstance(gid, tuple) else (gid,)
        for c in cols:
            vals = list(sub[c])  # keep originals for display
            hashed = {_make_hashable(v) for v in vals}
            if len(hashed) > 1:
                uniq, seen = [], set()
                for v in vals:
                    hv = _make_hashable(v)
                    if hv not in seen:
                        seen.add(hv)
                        uniq.append(v)
                sample = uniq[:max_values]
                logging.warning(
                    "%s %s: column '%s' has %d distinct values. Sample: %s",
                    label,
                    gid_disp,
                    c,
                    len(uniq),
                    [repr(x) for x in sample],
                )
                rows.append(
                    {
                        label: repr(gid_disp),
                        "column": c,
                        "num_distinct": len(uniq),
                        "distinct_values_sample": "; ".join(repr(x) for x in sample),
                    }
                )

    if rows:
        Path(Path(report_path).parent).mkdir(exist_ok=True, parents=True)
        pd.DataFrame(rows).to_csv(report_path, index=False)
        logging.warning("Wrote inconsistency report to %s", report_path)


def _write_if_bytes_changed(path: Path, new_bytes: bytes) -> bool:
    """
    Write bytes to path only if content differs.
    Returns True if file content changed (written), else False.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        old = path.read_bytes()
        if old == new_bytes:
            return False
    path.write_bytes(new_bytes)
    return True


def fetch_allmend_to_cache(cache_path: Path = ALLMEND_CACHE) -> tuple[Path, bool]:
    """
    Download Allmendbewilligungen GeoJSON, store to cache_path if bytes changed.
    Returns (cache_path, cache_bytes_changed).
    """
    r = common.requests_get(ALLMEND_URL, params={"format": "geojson"})
    r.raise_for_status()

    # Use raw bytes for stable change detection (independent of JSON key order parsing).
    content_bytes = r.content
    bytes_changed = _write_if_bytes_changed(cache_path, content_bytes)
    return cache_path, bytes_changed


def get_allmendbewilligungen(*, cache_path: Path = ALLMEND_CACHE) -> tuple[gpd.GeoDataFrame, bool]:
    """
    Returns (gdf, changed) where changed is True if the cached Allmend payload changed since last run.
    """
    cache_path, cache_bytes_changed = fetch_allmend_to_cache(cache_path)

    # Change tracking uses the cache file as the tracked artifact.
    changed_vs_last_run = ct.has_changed(str(cache_path))
    if changed_vs_last_run:
        ct.update_hash_file(str(cache_path))

    # Now load from cache (not from the network again)
    gj = json.loads(cache_path.read_text(encoding="utf-8"))
    gdf = gpd.GeoDataFrame.from_features(gj["features"], crs=CRS)
    gdf["geometry"] = gdf["geometry"].buffer(0)
    gdf = gdf[gdf["belgartbez"].isin(["Veranstaltung", "Aktivität", "Festivität"])].copy()

    with open("data_orig/allmend_fields.json", "r", encoding="utf-8") as f:
        colmap = json.load(f)
    gdf = gdf.rename(columns=colmap)

    # --- Normalize fields used for grouping ---
    for col in ["Bezeichnung"]:
        if col in gdf.columns:
            gdf[col] = gdf[col].astype(str).str.strip()

    # Parse dates to date (drop time) to avoid micro-variance; adjust if you need time-level precision
    def _to_date(s):
        x = pd.to_datetime(s, errors="coerce", utc=True)
        return x.dt.tz_localize(None).dt.date  # naive date

    if "Datum_von" in gdf.columns:
        gdf["Datum_von"] = _to_date(gdf["Datum_von"])
    if "Datum_bis" in gdf.columns:
        gdf["Datum_bis"] = _to_date(gdf["Datum_bis"])

    # --- Choose business-key grouping ---
    group_keys = ["BegehrenID", "Bezeichnung", "Datum_von", "Datum_bis"]

    # Validate sameness of non-geometry attributes within each business group
    meta_cols = [
        c
        for c in gdf.columns
        if c
        not in (
            "IDUnique",
            "LokalitätID",
            "BelegungID",
            "BelegungsartID",
            "Belegungsart-Bezeichnung",
            "BelegungsstatusID",
            "Belegungsstatus-Bezeichnung",
            "BelastungsartID",
            "Belastungsart-Bezeichnung",
            "Geschäftsmerkmal-Bezeichnung",
            "MerkmalWert",
            "EinheitID",
            "Belegungseinheit-Bezeichnung",
            "StrassenID",
            "MerkmalID",
            "Geo Point",
            "geometry",
        )
        + tuple(group_keys)
    ]

    # Hashable view only for nunique()
    hashable_view = gdf[meta_cols].map(_make_hashable)
    nunq = hashable_view.groupby(gdf[group_keys].apply(tuple, axis=1)).nunique(dropna=False)
    bad = nunq[(nunq > 1).any(axis=1)]
    if not bad.empty:
        logging.warning("Some attributes differ within business groups (Bezeichnung/Datum_von/Datum_bis).")
        report_cols = group_keys + meta_cols
        report_df = gdf.loc[:, report_cols]
        # For logging, synthesize a single key name
        log_intra_group_differences(report_df, group_keys, meta_cols, group_label="BUS_KEY")

    # --- Aggregate: first for meta, unary_union for geometry ---
    agg_dict = {c: "first" for c in meta_cols}
    agg_dict["geometry"] = _union_all
    grouped = gdf.groupby(group_keys, as_index=False).agg(agg_dict)
    grouped = gpd.GeoDataFrame(grouped, geometry="geometry", crs=CRS)

    # event_key used later in build_intersections()
    grouped["event_key"] = (
        grouped["Bezeichnung"].astype(str).str.strip()
        + "||"
        + grouped["Datum_von"].astype(str)
        + "||"
        + grouped["Datum_bis"].astype(str)
    )

    # --- signatures (force object dtype, avoid GeoSeries ops) ---
    def _geom_sig(g, precision=7):
        if g is None or g.is_empty:
            return None
        try:
            return wkb_dumps(g, rounding_precision=precision)  # bytes
        except Exception:
            return None

    # geom_sig: bytes/None -> plain object Series
    geom_sig = grouped["geometry"].apply(_geom_sig).astype("object")

    # cent_sig: tuple/None -> plain object Series
    geos = gpd.GeoSeries(grouped.geometry, crs=CRS)
    centroids_lv95 = geos.to_crs(2056).centroid
    cent_sig = centroids_lv95.apply(_cent_sig_from_point).astype("object")

    # build shape_sig as a plain pandas Series, then assign back
    shape_sig = pd.Series(geom_sig, index=grouped.index, dtype="object")
    mask = shape_sig.isna()
    # stringify tuple to be hashable/serializable
    shape_sig[mask] = cent_sig[mask].apply(_tuple_to_csv_or_none)
    grouped["geom_sig"] = geom_sig
    grouped["cent_sig"] = cent_sig
    grouped["shape_sig"] = shape_sig

    # keep ID lists for audit BEFORE dedup-by-shape
    bid = gdf.groupby(group_keys)["BelegungID"].agg(_unique_sorted_non_null).rename("BelegungID_list")
    grouped = grouped.merge(bid, on=group_keys, how="left")

    idu = gdf.groupby(group_keys)["IDUnique"].agg(_unique_sorted_non_null).rename("IDUnique_list")
    grouped = grouped.merge(idu, on=group_keys, how="left")

    locu = gdf.groupby(group_keys)["LokalitätID"].agg(_unique_sorted_non_null).rename("LokalitätID_list")
    grouped = grouped.merge(locu, on=group_keys, how="left")

    strid = gdf.groupby(group_keys)["StrassenID"].agg(_unique_sorted_non_null).rename("StrassenID_list")
    grouped = grouped.merge(strid, on=group_keys, how="left")

    art = (
        gdf.groupby(group_keys)["Belegungsart-Bezeichnung"]
        .agg(_unique_sorted_non_null)
        .rename("Belegungsart-Bezeichnung_list")
    )
    grouped = grouped.merge(art, on=group_keys, how="left")

    status = (
        gdf.groupby(group_keys)["Belegungsstatus-Bezeichnung"]
        .agg(_unique_sorted_non_null)
        .rename("Belegungsstatus-Bezeichnung_list")
    )
    grouped = grouped.merge(status, on=group_keys, how="left")

    # second-stage aggregation over identical shapes inside same event
    def _concat_unique_texts(s: pd.Series, sep=" | "):
        ignore = {"<unbekannt>", None}
        out, seen = [], set()
        for v in s.dropna().astype(str).str.strip():
            if not v or v in ignore:
                continue
            if v not in seen:
                seen.add(v)
                out.append(v)
        return sep.join(out)

    multi_text_cols = [
        "Belastungsart-Bezeichnung",
        "Geschäftsmerkmal-Bezeichnung",
        "MerkmalWert",
        "Belegungseinheit-Bezeichnung",
    ]
    present_text = [c for c in multi_text_cols if c in gdf.columns]

    if present_text:
        text_agg = (
            gdf.groupby(group_keys)[present_text].agg({c: _concat_unique_texts for c in present_text}).reset_index()
        )
        grouped = grouped.merge(text_agg, on=group_keys, how="left")

    keep_cols = ["BegehrenID", "Bezeichnung", "Datum_von", "Datum_bis"]
    agg2 = {c: "first" for c in (meta_cols + [c for c in keep_cols if c in grouped.columns])}
    agg2["geometry"] = _union_all
    agg2["BelegungID_list"] = _list_merge
    agg2["IDUnique_list"] = _list_merge
    if "LokalitätID_list" in grouped.columns:
        agg2["LokalitätID_list"] = _list_merge
    for c in multi_text_cols:
        if c in grouped.columns:
            agg2[c] = _concat_unique_texts

    grouped = grouped.groupby(["event_key", "shape_sig"], as_index=False).agg(agg2)
    grouped = gpd.GeoDataFrame(grouped, geometry="geometry", crs=CRS)
    return grouped, changed_vs_last_run


def get_perimeter_and_puffer(
    path: Path = PERIM_PATH,
) -> tuple[gpd.GeoDataFrame, bool]:
    """
    Returns (perim_gdf, changed) where changed is True if perimeter file changed since last run.
    """
    changed = ct.has_changed(str(path))
    if changed:
        ct.update_hash_file(str(path))

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
    return perim[["perimeter_name", "geometry"]], changed


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
    joined["area"] = parsed.apply(_pick_first)
    joined["kind"] = parsed.apply(_pick_second)

    # Prefer Perimeter over Puffer within (BelegungID, area)
    joined["kind_priority"] = joined["kind"].map({"Perimeter": 0, "Puffer": 1}).fillna(1)
    joined = (
        joined.sort_values(["event_key", "area", "kind_priority"])
        .drop_duplicates(subset=["event_key", "area"], keep="first")
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
    geojson_path = f"{data_dir}/sbt_allmend_centroids.geojson"
    csv_path = f"{data_dir}/sbt_allmend_centroids.csv"

    # GeoJSON (geometry = centroid only)
    centroids_gdf.to_file(geojson_path, driver="GeoJSON")

    # CSV: include centroid lon/lat plus WKT columns
    df_csv = pd.DataFrame(centroids_gdf.drop(columns=centroids_gdf.geometry.name))
    df_csv["centroid_x"] = centroids_gdf.geometry.x
    df_csv["centroid_y"] = centroids_gdf.geometry.y
    df_csv.to_csv(csv_path, index=False)

    logging.info("Wrote:\n  %s\n  %s", geojson_path, csv_path)


def main():
    logging.info("Checking inputs…")

    # Load + detect changes
    allmend_gdf, allmend_changed = get_allmendbewilligungen()
    perim_gdf, perim_changed = get_perimeter_and_puffer()

    if not allmend_changed and not perim_changed:
        logging.info("No changes detected in allmendbewilligungen or perimeter. Skipping processing.")
        return

    logging.info(
        "Changes detected: allmend=%s, perimeter=%s. Proceeding.",
        allmend_changed,
        perim_changed,
    )

    if len(perim_gdf) != 4:
        logging.warning("Expected 4 perimeter polygons; found %s.", len(perim_gdf))

    logging.info("Computing intersections and centroids…")
    result = build_intersections(perim_gdf, allmend_gdf)

    centroids_gdf = make_centroid_output(result)
    write_outputs(centroids_gdf, data_dir="data")

    logging.info("Job successful!")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f"Executing {__file__}...")
    main()
    logging.info("Job successful!")
