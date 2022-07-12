import pandas as pd
import geopandas as gpd
from shapely.ops import split
from shapely.geometry import Point, LineString

# dict_gew =  {   '0' : '-',
#                 '1' : 'Rhein - Basel',
#                 '2' : 'Rhein - Basel',
#                 '3' : 'Wiese - Pachtstrecke Stadt Basel',
#                 '4' : 'Birs - Pachtstrecke Stadt Basel',
#                 '5' : 'Riehenteich - Pachtstrecke Riehen',
#                 '6' : 'Wiese - Pachtstrecke Riehen',
#                 '7' : 'Wiese - Pachstrecke Riehen'
# }

df_gewaesser = pd.DataFrame()
df_gewaesser['Gewässer'] = ['Rhein - Basel',
                             'Wiese - Pachtstrecke Stadt Basel',
                             'Wiese - Pachtstrecke Riehen',
                             'Birs - Pachtstrecke Stadt Basel',
                             'Riehenteich - Pachtstrecke Riehen',
                            ]


pd.options.display.max_colwidth = 1000


gdf = gpd.read_file('data/gewaesser/gewaesser.geojson')
print(gdf)


# split up Wiese in Basel and Riehen part, breaking point: 47.57906, 7.62498
# (closest in geometry:  7.623379999359302 47.578078114071396)
line = gdf[gdf['gew_name']== 'Wiese']['geometry'].iloc[0]
point = Point(7.623379999359302, 47.578078114071396)

test = split(line, point)

geo_gewaesser = gpd.GeoSeries([gdf[gdf['gew_name']== 'Rhein']['geometry'].iloc[0],
                                test.geoms[1],
                                test.geoms[0],
                                gdf[gdf['gew_name']== 'Birs']['geometry'].iloc[0],
                                gdf[gdf['gew_name']== 'Mühleteich']['geometry'].iloc[0]
                                ])

gdf_gewaesser = gpd.GeoDataFrame(df_gewaesser, geometry=geo_gewaesser)

gdf_gewaesser.to_file("gewaesser_adapted.geojson")
