import pandas as pd
import geopandas as gpd
from shapely.ops import split, linemerge, nearest_points
from shapely.geometry import Point, LineString, MultiLineString


# Add df_gewaesser
df_gewaesser = pd.DataFrame()
df_gewaesser["Gewässer"] = [
    "Rhein - Basel-Stadt",
    "Wiese - Pachtstrecke Stadt Basel",
    "Wiese - Pachtstrecke Riehen",
    "Birs - Pachtstrecke Stadt Basel",
    "Neuer Teich / Mühleteich - Pachtstrecke Riehen",
]

# Read in relevant coordinates from Gewässernetz Basel
gdf = gpd.read_file("data/gewaesser/gewaesser.geojson")


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


# 'Riehenteich - Pachtstrecke Riehen': Add Neuer Teich and Mühleteich, cut off at border (7.65363, 47.59551)
# cut off piece of Neuer Teich that belongs to Wiese-Basel (7.62643, 47.57782)
line1 = gdf[gdf["gew_name"] == "Neuer Teich"]["geometry"].iloc[0]
line2 = gdf[gdf["gew_name"] == "Mühleteich"]["geometry"].iloc[0]
multi_line = MultiLineString([line1, line2])
merged_line = linemerge(multi_line)
point_border = Point(7.65363, 47.59551)
line_from_border, _ = split_line_at_point(merged_line, point_border)
cutpoint = Point(7.62643, 47.57782)
part_Wiese_Basel, line_riehenteich = split_line_at_point(line_from_border, cutpoint)

# split up Wiese in Basel and Riehen part, breaking point: 47.57906, 7.62498
line = gdf[gdf["gew_name"] == "Wiese"]["geometry"].iloc[0]
point = Point(7.62498, 47.57906)
Wiese_Basel, Wiese_Riehen = split_line_at_point(line, point)
# cut of piece of Riehenteich for Wiese_Basel (7.62393, 47.57679)
riehenteich = gdf[gdf["gew_name"] == "Riehenteich"]["geometry"].iloc[0]
cutpoint = Point(7.62393, 47.57679)
_, riehenteich_to_Wiese_Basel = split_line_at_point(riehenteich, cutpoint)
# add all pieces to Wiese_Basel
Wildschutzkanal = gdf[gdf["gew_name"] == "Wildschutzkanal"]["geometry"].iloc[0]
multi_line = MultiLineString(
    [Wiese_Basel, riehenteich_to_Wiese_Basel, part_Wiese_Basel, Wildschutzkanal]
)
Wiese_Basel = linemerge(multi_line)


# Construct gdf_gewaesser
geo_gewaesser = gpd.GeoSeries(
    [
        gdf[gdf["gew_name"] == "Rhein"]["geometry"].iloc[0],
        Wiese_Basel,
        Wiese_Riehen,
        gdf[gdf["gew_name"] == "Birs"]["geometry"].iloc[0],
        line_riehenteich,
    ]
)

gdf_gewaesser = gpd.GeoDataFrame(df_gewaesser, geometry=geo_gewaesser)

gdf_gewaesser.to_file("gewaesser_adapted.geojson")
