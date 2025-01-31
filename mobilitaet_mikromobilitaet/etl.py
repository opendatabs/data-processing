import os
import io
import logging
import pandas as pd
import geopandas as gpd
from owslib.wfs import WebFeatureService

import common
from mobilitaet_mikromobilitaet import credentials


# Function to create Map_links (unchanged)
def create_map_links(geometry, p1, p2):
    logging.info(f'the type of the geometry is {geometry.geom_type}')
    if geometry.geom_type == 'Polygon':
        centroid = geometry.centroid
    else:
        centroid = geometry

    lat, lon = centroid.y, centroid.x
    Map_links = f'https://opendatabs.github.io/map-links/?lat={lat}&lon={lon}&p1={p1}&p2={p2}'
    return Map_links


def main():
    url_wfs = 'https://wfs.geo.bs.ch/'
    wfs = WebFeatureService(url=url_wfs, version='2.0.0', timeout=120)

    shapes_to_load = [
        'XS_Bolt',
        'XS_Carvelo',
        'XS_Velospot',
        'XS_Voi',
        'XS_Bird',
        'XS_Lime',
        'XS_PickEBike',
        'XS_PickEMoped'
    ]

    # 1) Load all new data into gdf_result
    gdf_current = gpd.GeoDataFrame()
    for shapefile in shapes_to_load:
        response = wfs.getfeature(typename=shapefile)
        gdf = gpd.read_file(io.BytesIO(response.read()))
        gdf_current = pd.concat([gdf_current, gdf])

    # 2) Add the 'Map Links'
    logging.info("Create Map urls")
    tree_groups = 'Geteilte Mikromobilität'
    tree_group_layers_ = ('Geteilte Mikromobilität='
                          'XS_Bird,XS_Bolt,XS_Carvelo,XS_Lime,'
                          'XS_PickEBike,XS_PickEMoped,XS_Velospot,XS_Voi')
    gdf_current['Map Links'] = gdf_current.apply(
        lambda row: create_map_links(row['geometry'], tree_groups, tree_group_layers_),
        axis=1,
        result_type='expand'
    )

    # 3) Convert to EPSG:4326 and add a current timestamp localize in Europe/Zurich
    # Add + 1 because of the server time being wrong
    gdf_current = gdf_current.to_crs(epsg=4326)
    gdf_current['timestamp'] = (
                pd.to_datetime('now').replace(second=0, microsecond=0).tz_localize('Europe/Zurich') + pd.Timedelta(
            hours=1)).strftime('%Y-%m-%d %H:%M:%S%z')
    gdf_current = gdf_current.drop(columns='gml_id')

    # 4) Export the "aktuelle Verfügbarkeit" data into FTP and ODS
    filename_current = 'aktuelle_verfuegbarkeit.gpkg'
    path_export_current = os.path.join(credentials.data_path, filename_current)
    # If file does not exist, gdf_previous will be empty
    gdf_previous = gpd.read_file(path_export_current)  # Load the previous data for step 5
    gdf_current.to_file(path_export_current, driver='GPKG')
    common.update_ftp_and_odsp(path_export_current, 'mobilitaet/mikromobilitaet', '100415')
    # 4.1) Also save it into archive
    folder = pd.Timestamp.now().strftime('%Y-%m')
    common.ensure_ftp_dir(common.credentials.ftp_server,
                          common.credentials.ftp_user,
                          common.credentials.ftp_pass,
                          f'mobilitaet/mikromobilitaet/archiv/{folder}')
    # Localize the timestamp to Europe/Zurich
    # Add + 1 because of the server time being wrong
    current_time = pd.Timestamp.now().tz_localize('Europe/Zurich') + pd.Timedelta(hours=1)
    filename_ts = current_time.strftime('%Y-%m-%d_%H-%M%z')
    path_export_archive = os.path.join(credentials.data_path, 'archive', f'{filename_ts}.gpkg')
    gdf_current.to_file(path_export_archive, driver='GPKG')
    common.upload_ftp(path_export_archive,
                      common.credentials.ftp_server,
                      common.credentials.ftp_user,
                      common.credentials.ftp_pass,
                      f'mobilitaet/mikromobilitaet/archiv/{folder}')
    os.remove(path_export_archive)

    # 5) Compare gdf_previous and gdf_current for geometry changes > 100m
    # or if the previous geometry is missing (new bike)
    gdf_previous_2056 = gdf_previous.to_crs(epsg=2056)
    gdf_current_2056 = gdf_current.to_crs(epsg=2056)

    merged = gdf_current_2056.merge(
        gdf_previous_2056[['xs_bike_id', 'geometry']],
        on='xs_bike_id',
        how='left',
        suffixes=('_current', '_previous')
    )
    merged['distance_m'] = merged.geometry_current.distance(merged.geometry_previous)
    moved_over_100m = merged[(merged['distance_m'] > 100) | (merged['geometry_previous'].isna())].copy()
    moved_ids = moved_over_100m['xs_bike_id'].unique()
    gdf_current_moved = gdf_current[gdf_current['xs_bike_id'].isin(moved_ids)]
    logging.info(f"Filtered gdf_current down to {len(gdf_current_moved)} records with movement > 100m.")

    # 6) Push the moved data to ODS and FTP
    common.download_ftp(['zeitreihe_verfuegbarkeit.gpkg'],
                        common.credentials.ftp_server,
                        common.credentials.ftp_user,
                        common.credentials.ftp_pass,
                        'mobilitaet/mikromobilitaet/',
                        credentials.data_path, '')

    gdf_zeitreihe = gpd.read_file(os.path.join(credentials.data_path, 'zeitreihe_verfuegbarkeit.gpkg'))
    gdf_zeitreihe = pd.concat([gdf_zeitreihe, gdf_current_moved])
    path_export_zeitreihe = os.path.join(credentials.data_path, 'zeitreihe_verfuegbarkeit.gpkg')
    gdf_zeitreihe.to_file(path_export_zeitreihe, driver='GPKG')
    common.upload_ftp(path_export_zeitreihe,
                      common.credentials.ftp_server,
                      common.credentials.ftp_user,
                      common.credentials.ftp_pass,
                      'mobilitaet/mikromobilitaet/',
                      'zeitreihe_verfuegbarkeit.gpkg')

    gdf_current_moved['geo_point_2d'] = (
        gdf_current_moved['geometry']
        .apply(lambda x: x.wkt if x else None)  # Handle None or missing geometries
        .astype('str')  # Convert to string to avoid .str issues
        .str.replace('POINT ', '', regex=False)
        .str.replace('(', '', regex=False)
        .str.replace(')', '', regex=False)
    )
    df_to_push = gdf_current_moved.drop(columns=['geometry', 'Map Links']).copy()
    common.ods_realtime_push_df(df_to_push, credentials.push_url)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info('Job successful!')
