import logging
from io import StringIO
from pathlib import Path

import common
import geopandas as gpd
import numpy as np
import pandas as pd


def download_spatial_descriptors(ods_id: str) -> gpd.GeoDataFrame:
    """Download a GeoJSON spatial dataset from data.bs.ch and return it in EPSG:2056."""
    url = f"https://data.bs.ch/api/explore/v2.1/catalog/datasets/{ods_id}/exports/geojson"
    r = common.requests_get(url)
    gdf = gpd.read_file(StringIO(r.text))
    return gdf.to_crs("EPSG:2056")


def main():
    base = Path("data_orig")
    path_to_requisitionen = base / "Requisitionen.csv"
    path_to_eingaenge = base / "Eingaenge.csv"

    df = pd.read_csv(path_to_requisitionen)
    eing = pd.read_csv(path_to_eingaenge)

    df["Einsatzzeit"] = pd.to_datetime(df["Einsatzzeit"], format="%H:%M:%S")
    start = df["Einsatzzeit"].dt.floor("h")
    end = start + pd.Timedelta(hours=1)
    df["Einsatzzeit"] = start.dt.strftime("%H:00") + " - " + end.dt.strftime("%H:00")

    df = df.merge(
        eing,
        how="left",
        left_on=["ort_gemeinde_name", "ort_strasse_name", "ort_Hausnummer"],
        right_on=["plz_ort_name", "strasse_text", "eingang_hausnummer"],
        suffixes=("", "_eing"),
    )

    # Replace placeholder coords if building coords exist
    df["OriginalKoordinateX"] = np.where(
        (df["OriginalKoordinateX"] == 2000000) & df["gebaeude_koordinate_x"].notna(),
        df["gebaeude_koordinate_x"],
        df["OriginalKoordinateX"],
    )
    df["OriginalKoordinateY"] = np.where(
        (df["OriginalKoordinateY"] == 1000000) & df["gebaeude_koordinate_y"].notna(),
        df["gebaeude_koordinate_y"],
        df["OriginalKoordinateY"],
    )

    # Keep only rows with usable XY (recommended for spatial ops)
    df = df[df["OriginalKoordinateX"].notna() & df["OriginalKoordinateY"].notna()].copy()

    logging.info("Downloading Wohnviertel (100042) and Bezirke (100039)...")
    viertel_gdf = download_spatial_descriptors("100042")
    bez_gdf = download_spatial_descriptors("100039")

    pts_gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df["OriginalKoordinateX"], df["OriginalKoordinateY"]),
        crs="EPSG:2056",
    )

    logging.info("Joining Requisitionen coordinates with Wohnviertel and Bezirke...")
    joined = gpd.sjoin(
        pts_gdf,
        viertel_gdf[["wov_id", "wov_name", "geometry"]],
        how="left",
        predicate="within",
    ).drop(columns=["index_right"])

    joined = gpd.sjoin(
        joined,
        bez_gdf[["bez_id", "bez_name", "geometry"]],
        how="left",
        predicate="within",
    )
    joined["bez_geometry"] = bez_gdf.geometry.reindex(joined["index_right"]).values
    joined = joined.drop(columns=["index_right"]).set_geometry("bez_geometry")

    out = (
        joined.drop(columns=["geometry"])
        .rename(columns={"bez_geometry": "geometry"})
        .set_geometry("geometry")
        .set_crs("EPSG:2056")
    )

    out = out.to_crs("EPSG:4326")

    columns_of_interest = [
        "Ereignistyp",
        "EreignistypKlasse",
        "EinsatzJahr",
        "EinsatzMonat",
        "EinsatzDatum",
        "Einsatzzeit",
        "Lichtverhaeltnisse",
        "wov_id",
        "wov_name",
        "bez_id",
        "bez_name",
        "geometry",
    ]
    out = out[columns_of_interest]

    Path("data").mkdir(parents=True, exist_ok=True)

    out.to_file(Path("data/100517_requisitionen.geojson"), driver="GeoJSON")

    out_csv = out.copy()
    out_csv["geometry"] = out_csv.geometry.to_wkt()

    # longitude/latitude are already in df; they are carried through in `out`
    out_csv.to_csv(Path("data/100517_requisitionen.csv"), index=False)

    common.update_ftp_and_odsp("data/100517_requisitionen.geojson", "requisitionen", "100517")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f"Executing {__file__}...")
    main()
    logging.info("Job successful!")
