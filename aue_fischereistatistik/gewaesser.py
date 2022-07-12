import pandas as pd
import geopandas as gpd
from shapely.ops import split, linemerge, nearest_points
from shapely.geometry import Point, LineString, MultiLineString

pd.options.display.max_colwidth = 1000

# Add df_gewaesser
df_gewaesser = pd.DataFrame()
df_gewaesser['Gewässer'] = ['Rhein - Basel',
                             'Wiese - Pachtstrecke Stadt Basel',
                             'Wiese - Pachtstrecke Riehen',
                             'Birs - Pachtstrecke Stadt Basel',
                             'Riehenteich - Pachtstrecke Riehen',
                            ]

# Read in relevant coordinates from Gewässernetz Basel
gdf = gpd.read_file('data/gewaesser/gewaesser.geojson')
print(gdf)

# function to split line at point closest to a given point
def split_line_at_point(line,point):
    nearest_on_line = nearest_points(line, point)[0]
    coordinates_line = [coord for coord in line.coords]
    coordinates_point = [coord for coord in nearest_on_line.coords]
    list_coordinates_line_with_point = coordinates_line + coordinates_point
    line_with_point = LineString(sorted(list_coordinates_line_with_point))
    splitted_line = split(line_with_point, nearest_on_line)
    line1 = splitted_line.geoms[0]
    line2 = splitted_line.geoms[1]
    return line1, line2


# split up Wiese in Basel and Riehen part, breaking point: 47.57906, 7.62498
# Still add small pieces to Basel Wiese?
line = gdf[gdf['gew_name']== 'Wiese']['geometry'].iloc[0]
point = Point(7.62498, 47.57906)
split_Wiese = split_line_at_point(line, point)

# 'Riehenteich - Pachtstrecke Riehen': Add Neuer Teich and Mühleteich, cut off at border (7.65363, 47.59551)
line1 = gdf[gdf['gew_name']== 'Neuer Teich']['geometry'].iloc[0]
line2 = gdf[gdf['gew_name']== 'Mühleteich']['geometry'].iloc[0]
multi_line = MultiLineString([line1, line2])
merged_line = linemerge(multi_line)
point = Point(7.65363, 47.59551)
line_riehenteich, _ = split_line_at_point(merged_line, point)



# Construct gdf_gewaesser
geo_gewaesser = gpd.GeoSeries([gdf[gdf['gew_name']== 'Rhein']['geometry'].iloc[0],
                                split_Wiese[1],
                                split_Wiese[0],
                                gdf[gdf['gew_name']== 'Birs']['geometry'].iloc[0],
                                line_riehenteich
                                ])

gdf_gewaesser = gpd.GeoDataFrame(df_gewaesser, geometry=geo_gewaesser)

gdf_gewaesser.to_file("gewaesser_adapted.geojson")
