import os
from pathlib import Path

import pandas as pd
import geopandas as gpd
import pyodbc
from dotenv import load_dotenv

load_dotenv()
PATH_TO_DSN = os.getenv("PATH_TO_DSN")

def main():
    conn = pyodbc.connect(PATH_TO_DSN, autocommit=True)
    df = pd.read_sql("SELECT * FROM polizei.FaktEinsaetze", conn)

    shp_path = Path("data_orig/hexagonalraster/hexaraster_kanton_50.shp")
    hex_gdf = gpd.read_file(shp_path)

    # Ensure CRS
    if hex_gdf.crs is None:
        hex_gdf = hex_gdf.set_crs("EPSG:2056")
    else:
        hex_gdf = hex_gdf.to_crs("EPSG:2056")

    pts_gdf = gpd.GeoDataFrame(
        df.copy(),
        geometry=gpd.points_from_xy(df["OriginalKoordinateX"], df["OriginalKoordinateY"]),
        crs="EPSG:2056",
    )
    hex_join = hex_gdf.copy()
    hex_join = hex_join.rename_geometry("hex_geometry")
    hex_join = hex_join[["hex_geometry"]]

    joined = gpd.sjoin(
        pts_gdf,
        hex_gdf[["geometry"]],
        how="left",
        predicate="within",
    )

    joined["hex_geometry"] = hex_gdf.geometry.reindex(joined["index_right"]).values
    joined = joined.drop(columns=["index_right"])
    joined = joined.set_geometry("hex_geometry")
    out = (
        joined
        .drop(columns=["geometry"])
        .rename(columns={"hex_geometry": "geometry"})
        .set_geometry("geometry")
        .set_crs("EPSG:2056")
    )

    Path("data").mkdir(parents=True, exist_ok=True)

    out.to_file(Path("data/100517_requisitionen.geojson"), driver="GeoJSON")
    out_csv = out.copy()
    out_csv["geometry"] = out_csv.geometry.to_wkt()
    out_csv.to_csv(Path("data/100517_requisitionen.csv"), index=False)


if __name__ == "__main__":
    main()
