import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString, MultiLineString, Point
from shapely.ops import linemerge, nearest_points, split
import logging
import common
from dotenv import load_dotenv
import os

load_dotenv()
ODS_API_KEY = os.getenv('ODS_API_KEY')

CRS = "EPSG:4326"

# function to split line at point closest to a given point
def split_line_at_point(line, point):
    nearest_on_line = nearest_points(line, point)[0]
    coordinates_line = [coord for coord in line.coords]
    coordinates_point = [coord for coord in nearest_on_line.coords]
    list_coordinates_line_with_point = coordinates_line + coordinates_point
    line_with_point = LineString(sorted(list_coordinates_line_with_point))
    splitted_line = split(line_with_point, nearest_on_line)
    line1 = splitted_line.geoms[0]
    line2 = splitted_line.geoms[1]
    return line1, line2

def get_gewaesser():
    url = "https://data.bs.ch/explore/dataset/100261/download/"
    r = common.requests_get(url,headers={'Authorization': f'apikey {ODS_API_KEY}'}, params={
        "format": "geojson",
    })
    r.raise_for_status()
    gj = r.json()
    gdf = gpd.GeoDataFrame.from_features(gj["features"], crs=CRS)
    return gdf


def main():
    # Add df_gewaesser
    df_gewaesser = pd.DataFrame()
    df_gewaesser["Gewässer"] = [
        "Rhein - Basel-Stadt",
        "Wiese - Pachtstrecke Stadt Basel",
        "Wiese - Pachtstrecke Riehen",
        "Birs - Pachtstrecke Stadt Basel",
        "Neuer Teich / Mühleteich - Pachtstrecke Riehen",
    ]

    logging.info("Read in Gewässernetz Basel data from ODS")
    gdf = get_gewaesser()
    # 'Riehenteich - Pachtstrecke Riehen': Add Neuer Teich and Mühleteich, cut off at border (7.65363, 47.59551)
    # cut off piece of Neuer Teich that belongs to Wiese-Basel (7.62643, 47.57782)
    line1 = gdf[gdf["gz_gewaessername"] == "Neuer Teich"]["geometry"].iloc[0]
    line2 = gdf[gdf["gz_gewaessername"] == "Mühleteich"]["geometry"].iloc[0]
    multi_line = MultiLineString([line1, line2])
    merged_line = linemerge(multi_line)
    point_border = Point(7.65363, 47.59551)
    line_from_border, _ = split_line_at_point(merged_line, point_border)
    cutpoint = Point(7.62643, 47.57782)
    part_Wiese_Basel, line_riehenteich = split_line_at_point(line_from_border, cutpoint)

    # split up Wiese in Basel and Riehen part, breaking point: 47.57906, 7.62498
    line = gdf[gdf["gz_gewaessername"] == "Wiese"]["geometry"].iloc[0]
    point = Point(7.62498, 47.57906)
    Wiese_Basel, Wiese_Riehen = split_line_at_point(line, point)
    # cut of piece of Riehenteich for Wiese_Basel (7.62393, 47.57679)
    riehenteich = gdf[gdf["gz_gewaessername"] == "Riehenteich"]["geometry"].iloc[0]
    cutpoint = Point(7.62393, 47.57679)
    _, riehenteich_to_Wiese_Basel = split_line_at_point(riehenteich, cutpoint)
    # add all pieces to Wiese_Basel
    Wildschutzkanal = gdf[gdf["gz_gewaessername"] == "Wildschutzkanal"]["geometry"].iloc[0]
    multi_line = MultiLineString([Wiese_Basel, riehenteich_to_Wiese_Basel, part_Wiese_Basel, Wildschutzkanal])
    Wiese_Basel = linemerge(multi_line)


    # Construct gdf_gewaesser
    geo_gewaesser = gpd.GeoSeries(
        [
            gdf[gdf["gz_gewaessername"] == "Rhein"]["geometry"].iloc[0],
            Wiese_Basel,
            Wiese_Riehen,
            gdf[gdf["gz_gewaessername"] == "Birs"]["geometry"].iloc[0],
            line_riehenteich,
        ]
    )

    gdf_gewaesser = gpd.GeoDataFrame(df_gewaesser, geometry=geo_gewaesser)

    gdf_gewaesser.to_file("data/gewaesser_adapted.geojson")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
    logging.info("Job completed successfully.")