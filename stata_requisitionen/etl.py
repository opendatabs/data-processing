import logging
from pathlib import Path

import common.change_tracking as ct
import geopandas as gpd
import pandas as pd


def main():
    path_to_requisitionen = Path("data_orig/Requisitionen.csv")
    if ct.has_changed(path_to_requisitionen):
        df = pd.read_csv(path_to_requisitionen)

        shp_path = Path("data_orig/hexagonalraster/hexaraster_kanton_100.shp")
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
            joined.drop(columns=["geometry"])
            .rename(columns={"hex_geometry": "geometry"})
            .set_geometry("geometry")
            .set_crs("EPSG:2056")
        )

        Path("data").mkdir(parents=True, exist_ok=True)

        out.to_file(Path("data/100517_requisitionen.geojson"), driver="GeoJSON")
        out_csv = out.copy()
        out_csv["geometry"] = out_csv.geometry.to_wkt()
        out_csv.to_csv(Path("data/100517_requisitionen.csv"), index=False)
        ct.update_hash_file(path_to_requisitionen)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f"Executing {__file__}...")
    main()
    logging.info("Job successful!")
