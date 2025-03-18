import os
import io
import logging
import shutil
import pandas as pd
import geopandas as gpd
from owslib.wfs import WebFeatureService

import common
from dotenv import load_dotenv

load_dotenv()

DATA_PATH = os.getenv("DATA_PATH")
TEMP_PATH = os.getenv("TEMP_PATH")


def create_map_links(geometry, p1, p2):
    """
    Create a map link for the given geometry, with parameters p1 and p2.
    """
    logging.info(f"The type of the geometry is {geometry.geom_type}")
    if geometry.geom_type == 'Polygon':
        centroid = geometry.centroid
    else:
        centroid = geometry

    lat, lon = centroid.y, centroid.x
    map_link = f'https://opendatabs.github.io/map-links/?lat={lat}&lon={lon}&p1={p1}&p2={p2}'
    return map_link


def load_current_data_from_wfs(url_wfs, shapes_to_load):
    """
    Load data from the given WFS URL for a list of layer names.

    :param url_wfs: The URL to the Web Feature Service
    :param shapes_to_load: List of WFS layer names to load
    :return: A GeoDataFrame with all combined features
    """
    logging.info(f"Connecting to WFS at {url_wfs}")
    wfs = WebFeatureService(url=url_wfs, version='2.0.0', timeout=120)

    gdf_current = gpd.GeoDataFrame()
    for shapefile in shapes_to_load:
        logging.info(f"Fetching data for layer: {shapefile}")
        response = wfs.getfeature(typename=shapefile)
        gdf = gpd.read_file(io.BytesIO(response.read()))
        gdf_current = pd.concat([gdf_current, gdf])

    return gdf_current


def add_map_links(gdf, tree_groups, tree_group_layers):
    """
    Add a 'Map Links' column to the GeoDataFrame, using create_map_links().
    """
    logging.info("Create Map urls for each row in the GeoDataFrame")
    gdf['Map Links'] = gdf.apply(
        lambda row: create_map_links(row['geometry'], tree_groups, tree_group_layers),
        axis=1,
        result_type='expand'
    )
    return gdf


def prepare_gdf(gdf, drop_cols=None):
    """
    Convert the GeoDataFrame to EPSG:4326, add timestamps, and drop specified columns.
    """
    logging.info("Converting CRS to EPSG:4326 and adding timestamp.")
    gdf = gdf.to_crs(epsg=4326)
    current_timestamp = (pd.to_datetime('now')
                         .replace(second=0, microsecond=0)
                         .tz_localize('Europe/Zurich')
                         .strftime('%Y-%m-%d %H:%M:%S%z'))
    gdf['timestamp'] = current_timestamp
    gdf['timestamp_moved'] = None

    if drop_cols:
        for col in drop_cols:
            if col in gdf.columns:
                gdf = gdf.drop(columns=col)

    return gdf


# TODO: Move this to common.
#  Keep in mind that than every Dockerfile needs to install geopandas.
def gpd_to_mounted_file(gdf, path, *args, **kwargs):
    """
    Writes a file using geopandas.to_file,
    but writes it first into a temporary file to avoid
    geopandas errors when reading from mounted volumes.
    """
    # Copy the file to a temporary folder which is not mounted
    filename = os.path.basename(path)
    temp_path = os.path.join(TEMP_PATH, filename)
    # Writes the file using geopandas
    gdf.to_file(temp_path, *args, **kwargs)
    shutil.copy(temp_path, path)
    if os.path.exists(temp_path):
        os.remove(temp_path)


def export_current_data(gdf_current, filename_current):
    """
    Export the current GeoDataFrame to a GeoPackage. If a previous file exists, load it
    and return it for subsequent comparison.
    """
    path_export_current = os.path.join(DATA_PATH, filename_current)
    # Attempt to load previous data (if file does not exist, gdf_previous will be empty).
    if os.path.exists(path_export_current):
        gdf_previous = gpd.read_file(path_export_current)
    else:
        gdf_previous = gpd.GeoDataFrame()

    # Save current data
    gpd_to_mounted_file(gdf_current, path_export_current, driver='GPKG')

    # Archiving
    folder = pd.Timestamp.now().strftime('%Y-%m')

    current_time = pd.Timestamp.now().tz_localize('Europe/Zurich')
    filename_ts = current_time.strftime('%Y-%m-%d_%H-%M%z')
    path_export_archive = os.path.join(DATA_PATH, 'archiv', folder, f'{filename_ts}.gpkg')
    gpd_to_mounted_file(gdf_current, path_export_archive, driver='GPKG')

    return gdf_previous


def compare_geometries_and_filter_moved(gdf_previous, gdf_current):
    """
    Compare the geometries from gdf_previous and gdf_current to find bikes
    that moved more than 100m or are new (previous geometry is missing).
    Skip rows where xs_provider_name is 'Bird'.
    Bird changes the ID every minute, so it's not possible to track movement.

    :return: (moved_ids_current, moved_ids_previous, gdf_current_moved)
    """
    # Filter out Bird
    gdf_previous_2056 = gdf_previous[gdf_previous['xs_provider_name'] != 'Bird'].to_crs(epsg=2056)
    gdf_current_2056 = gdf_current[gdf_current['xs_provider_name'] != 'Bird'].to_crs(epsg=2056)

    merged = gdf_current_2056[['xs_bike_id', 'geometry']].merge(
        gdf_previous_2056[['xs_bike_id', 'geometry']],
        on='xs_bike_id',
        how='outer',
        suffixes=('_current', '_previous')
    )

    merged['distance_m'] = merged.geometry_current.distance(merged.geometry_previous)

    # Bikes that moved >100m or are new
    moved_over_100m_current = merged[(merged['distance_m'] > 100) | (merged['geometry_previous'].isna())].copy()
    moved_ids_current = moved_over_100m_current['xs_bike_id'].unique()

    # Bikes that moved >100m or no longer exist
    moved_over_100m_previous = merged[(merged['distance_m'] > 100) | (merged['geometry_current'].isna())].copy()
    moved_ids_previous = moved_over_100m_previous['xs_bike_id'].unique()

    gdf_current_moved = gdf_current[gdf_current['xs_bike_id'].isin(moved_ids_current)]
    logging.info(f"Filtered gdf_current down to {len(gdf_current_moved)} records with movement > 100m or new ones.")
    logging.info(
        f"Filtered gdf_previous down to {len(moved_ids_previous)} records with movement > 100m or missing ones.")

    return moved_ids_previous, gdf_current_moved


def update_timeseries(moved_ids_previous, gdf_current_moved, timestamp):
    """
    Load the existing timeseries data, update 'timestamp_moved' for bikes that moved or no longer exist and append new
    """
    path_export_zeitreihe = os.path.join(DATA_PATH, 'zeitreihe_verfuegbarkeit.gpkg')
    gdf_zeitreihe = gpd.read_file(path_export_zeitreihe)

    # Update timestamp_moved for bikes that have not moved yet, but now have
    mask_to_update = (gdf_zeitreihe['xs_bike_id'].isin(moved_ids_previous) &
                      gdf_zeitreihe['timestamp_moved'].isna())
    gdf_zeitreihe.loc[mask_to_update, 'timestamp_moved'] = timestamp

    # Append new moved bikes to the timeseries
    gdf_zeitreihe = pd.concat([gdf_zeitreihe, gdf_current_moved])

    # Save and upload updated timeseries
    gpd_to_mounted_file(gdf_zeitreihe, path_export_zeitreihe, driver='GPKG')

    return gdf_zeitreihe


def convert_to_csv(gdf_zeitreihe):
    """
    Final step: convert geometry to geo_point_2d and save as csv
    """
    # Extract the geo_point_2d from the geometry and switch them
    gdf_zeitreihe['geo_point_2d'] = (
        gdf_zeitreihe['geometry']
        .astype(str)
        .str.replace('POINT ', '', regex=False)
        .str.replace('(', '', regex=False)
        .str.replace(')', '', regex=False)
    )
    gdf_zeitreihe['geo_point_2d'] = (
        gdf_zeitreihe['geo_point_2d']
        .str.split(' ')
        .apply(lambda x: f'{x[1]}, {x[0]}')
    )

    # Drop geometry and 'Map Links' before pushing
    df_zeitreihe = gdf_zeitreihe.drop(columns=['geometry', 'Map Links']).copy()

    # Save as CSV
    path_export_csv = os.path.join(DATA_PATH, 'zeitreihe_verfuegbarkeit.csv')
    df_zeitreihe.to_csv(path_export_csv, index=False)


def main():
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f"Executing {__file__}...")

    url_wfs = 'https://wfs.geo.bs.ch/'
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
    gdf_current = load_current_data_from_wfs(url_wfs, shapes_to_load)

    tree_groups = 'Geteilte Mikromobilität'
    tree_group_layers_ = (
        'Geteilte Mikromobilität='
        'XS_Bird,XS_Bolt,XS_Carvelo,XS_Lime,'
        'XS_PickEBike,XS_PickEMoped,XS_Velospot,XS_Voi'
    )
    gdf_current = add_map_links(gdf_current, tree_groups, tree_group_layers_)
    gdf_current = prepare_gdf(gdf_current, drop_cols=['gml_id'])

    filename_current = 'aktuelle_verfuegbarkeit.gpkg'
    gdf_previous = export_current_data(gdf_current, filename_current)

    moved_ids_previous, gdf_current_moved = compare_geometries_and_filter_moved(gdf_previous, gdf_current)

    current_timestamp = gdf_current['timestamp'].iloc[0]  # Same timestamp for the entire current dataset
    gdf_zeitreihe = update_timeseries(moved_ids_previous, gdf_current_moved, current_timestamp)

    convert_to_csv(gdf_zeitreihe)

    # FTP and ODS upload (in the end to avoid incomplete data, if something fails)
    path_export_current = os.path.join(DATA_PATH, filename_current)
    common.update_ftp_and_odsp(path_export_current, 'mobilitaet/mikromobilitaet', '100415')

    logging.info("Job successful!")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info('Job successful!')
