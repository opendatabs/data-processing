import logging
import geopandas as gpd
import os
from shapely.ops import transform
from pyproj import Transformer
import requests

# transform between  WGS84 and LV95
# WGS84 corresponds to  EPSG:4326
# LV95 corresponds to EPSG:2056
transformer = Transformer.from_crs("epsg:4326", "epsg:2056", always_xy=True)
transformer_inverse = Transformer.from_crs("epsg:2056", "epsg:4326", always_xy=True)


# Read in relevant coordinates from Gewässernetz Basel
filename = os.path.join(os.path.dirname(__file__), 'data', 'gewaesser.geojson')
gdf = gpd.read_file(filename)


def make_buffer(river, distance):
    if river == "Rhein":
        width = 100
    elif river == "Birs" or river == "Wiese":
        width = 25
    else:
        logging.warning("River unknown!")
        width = 0
    # get linestring river
    line = gdf[gdf['gew_name'] == river]['geometry'].iloc[0]
    # transform to LV95
    line = transform(transformer.transform, line)
    # buffer line
    surroundings = line.buffer(distance + width)
    # reduce number of coordinates
    surroundings = surroundings.simplify(10)
    # transform back to WGS84
    surroundings = transform(transformer_inverse.transform, surroundings)
    # switch x,y coordinates
    surroundings = transform(lambda x, y: (y, x), surroundings)
    # get string of coordinates in right form for dataportal api
    coordinates = list(surroundings.exterior.coords)
    coordinates = ','.join(str(xy) for xy in coordinates)
    return coordinates


def add_buffer_to_gdf(river, distance, gdf=gdf):
    buffer = make_buffer(river=river, distance=distance)
    gdf.loc[-1] = ("buffer_" + river + "_" + str(distance), 0, buffer)
    return gdf

def export_gdf(gdf):
    export_filename = os.path.join(os.path.dirname(__file__), 'data', 'gewaesser_adapted.geojson')
    gdf.to_file(export_filename)


def geofilter_dataset(dataset_id, coords_filter):
    url = f'https://data.bs.ch/explore/dataset/{dataset_id}/download/?format=geojson&geofilter.polygon={coords_filter}'
    req = requests.get(url=url)
    text = req.text
    gdf_filtered = gpd.read_file(text)
    return gdf_filtered

# example
# river = 'Rhein'
# distance = 40
# dataset_id = '100031' #Sanitäre Anlagen
#
# coords_filter = make_buffer(river=river, distance=distance)
# gdf_filtered = geofilter_dataset(dataset_id=dataset_id, coords_filter=coords_filter)

# # make list url's
# distances = [15, 50, 75]
# rivers = ['Rhein', 'Birs', 'Wiese']
# dataset_id = '100031'
# url_list = []
# for river in rivers:
#     for dist in distances:
#         coordinates = make_buffer(river, dist)
#         url = f'https://data.bs.ch/explore/dataset/{dataset_id}/download/?format=geojson&geofilter.polygon={coordinates}'
#         url_list.append(river + ' ' + str(dist) + ' Meter: ' + url)