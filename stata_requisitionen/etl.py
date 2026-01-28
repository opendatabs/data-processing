import logging
from pathlib import Path

import common
import geopandas as gpd
import numpy as np
import pandas as pd


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

    # --- your existing hex join in EPSG:2056 ---
    shp_path = base / "hexagonalraster" / "hexaraster_kanton_100.shp"
    hex_gdf = gpd.read_file(shp_path)
    hex_gdf = hex_gdf.set_crs("EPSG:2056") if hex_gdf.crs is None else hex_gdf.to_crs("EPSG:2056")

    pts_gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df["OriginalKoordinateX"], df["OriginalKoordinateY"]),
        crs="EPSG:2056",
    )

    joined = gpd.sjoin(pts_gdf, hex_gdf[["geometry"]], how="left", predicate="within")
    joined["hex_geometry"] = hex_gdf.geometry.reindex(joined["index_right"]).values
    joined = joined.drop(columns=["index_right"]).set_geometry("hex_geometry")

    out = (
        joined.drop(columns=["geometry"])
        .rename(columns={"hex_geometry": "geometry"})
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
