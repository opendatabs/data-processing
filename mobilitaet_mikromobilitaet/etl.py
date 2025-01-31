import os
import io
import common
import logging
import pandas as pd
import geopandas as gpd
from owslib.wfs import WebFeatureService
from mobilitaet_mikromobilitaet import credentials


# Function to create Map_links
def create_map_links(geometry, p1, p2):
    # check whether the data is a geo point or geo shape
    logging.info(f'the type of the geometry is {geometry.geom_type}')
    # geometry_types = gdf.iloc[0][geometry].geom_type
    if geometry.geom_type == 'Polygon':
        centroid = geometry.centroid
    else:
        centroid = geometry

    #  create a Map_links
    lat, lon = centroid.y, centroid.x
    Map_links = f'https://opendatabs.github.io/map-links/?lat={lat}&lon={lon}&p1={p1}&p2={p2}'
    return Map_links


def main():
    url_wfs = 'https://wfs.geo.bs.ch/'
    wfs = WebFeatureService(url=url_wfs, version='2.0.0', timeout=120)

    shapes_to_load = ['XS_Bolt', 'XS_Carvelo', 'XS_Velospot', 'XS_Voi',
                      'XS_Bird', 'XS_Lime', 'XS_PickEBike', 'XS_PickEMoped']

    gdf_result = gpd.GeoDataFrame()
    for shapefile in shapes_to_load:
        # Retrieve and save the geodata for each layer name in shapes_to_load
        response = wfs.getfeature(typename=shapefile)
        gdf = gpd.read_file(io.BytesIO(response.read()))
        gdf_result = pd.concat([gdf_result, gdf])

    logging.info(f"Create Map urls")
    # Extract params from redirect
    tree_groups = 'Geteilte Mikromobilität'
    tree_group_layers_ = 'Geteilte Mikromobilität=XS_Bird,XS_Bolt,XS_Carvelo,XS_Lime,XS_PickEBike,XS_PickEMoped,XS_Velospot,XS_Voi'
    gdf_result['Map Links'] = gdf_result.apply(lambda row: create_map_links(row['geometry'], tree_groups, tree_group_layers_), axis=1, result_type='expand')

    gdf_result = gdf_result.to_crs(epsg=4326)
    gdf_result['timestamp'] = pd.to_datetime('now').replace(second=0, microsecond=0)

    filename = 'aktuelle_verfuegbarkeit.gpkg'
    path_export = os.path.join(credentials.data_path, filename)
    gdf_result.to_file(path_export, driver='GPKG')
    common.update_ftp_and_odsp(path_export, '/mobilitaet/mikromobilitaet', '100415')

    # Read Zeithreihe
    common.download_ftp(['zeitreihe_verfuegbarkeit.gpkg'], common.credentials.ftp_server,
                        common.credentials.ftp_user, common.credentials.ftp_pass,
                        '/mobilitaet/mikromobilitaet', credentials.data_path, '')
    filename = 'zeitreihe_verfuegbarkeit.gpkg'
    path_export = os.path.join(credentials.data_path, filename)
    gdf_zeitreihe = gpd.read_file(path_export)

    # Merge the two geodataframes and drop duplicates
    gdf_merged = pd.concat([gdf_result, gdf_zeitreihe], ignore_index=True)
    gdf_merged = gdf_merged.drop_duplicates()

    # Export the merged geodataframe into path_export
    gdf_merged.to_file(path_export, driver='GPKG')
    common.update_ftp_and_odsp(path_export, '/mobilitaet/mikromobilitaet', '100416')


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info('Job successful!')
