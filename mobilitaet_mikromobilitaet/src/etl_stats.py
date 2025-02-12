import os
import io
import zipfile
import logging
import pandas as pd
import geopandas as gpd
from datetime import datetime

import common
import ods_publish.etl_id as odsp
from mobilitaet_mikromobilitaet import credentials


def download_spatial_descriptors(ods_id):
    """
    Download and extract a shapefile from data.bs.ch for a given ODS dataset ID.
    Returns a GeoDataFrame in EPSG:2056.
    """
    url_to_shp = f'https://data.bs.ch/explore/dataset/{ods_id}/download/?format=shp'
    r = common.requests_get(url_to_shp)
    z = zipfile.ZipFile(io.BytesIO(r.content))

    # Create a folder for the extracted data
    extract_folder = os.path.join(credentials.data_path, ods_id)
    if not os.path.exists(extract_folder):
        os.makedirs(extract_folder)

    z.extractall(extract_folder)
    path_to_shp = os.path.join(extract_folder, f"{ods_id}.shp")

    gdf = gpd.read_file(path_to_shp, encoding='utf-8')
    return gdf.to_crs("EPSG:2056")


def get_files_from_ftp_for_day(date_str):
    """
    Given a date string (e.g., '2025-02-01'), download all archive .gpkg files
    for that day from the FTP.

    Assumes the archive directory structure is something like:
    mobilitaet/mikromobilitaet/archiv/YYYY-MM/
    with filenames containing timestamps like 'YYYY-MM-DD_HH-MM+zone.gpkg'
    """
    date_obj = pd.to_datetime(date_str)
    year_month = date_obj.strftime('%Y-%m')

    ftp_folder = f"mobilitaet/mikromobilitaet/archiv/{year_month}"
    local_folder = os.path.join(credentials.temp_path, "archive_downloaded")

    # List all files in that FTP folder
    common.download_ftp(
        [],
        common.credentials.ftp_server,
        common.credentials.ftp_user,
        common.credentials.ftp_pass,
        ftp_folder,
        local_folder,
        f'{date_str}*.gpkg'
    )


def combine_daily_files_to_gdf(date_str):
    """
    Read each local .gpkg file into a GeoDataFrame, concatenate them, and
    ensure they're in a projected CRS (EPSG:2056) for spatial operations.

    Returns the combined GeoDataFrame and a list of missing timestamps.
    """
    local_folder = os.path.join(credentials.temp_path, "archive_downloaded")

    gdf_list = []
    for file in os.listdir(local_folder):
        if not file.endswith(".gpkg"):
            continue
        path = os.path.join(local_folder, file)
        gdf_part = gpd.read_file(path)
        # Remove the file after reading
        os.remove(path)
        # Make sure everything is in a single projected CRS (EPSG:2056) for spatial ops
        if gdf_part.crs is not None and gdf_part.crs.to_epsg() != 2056:
            gdf_part = gdf_part.to_crs(epsg=2056)
        gdf_list.append(gdf_part)

    if not gdf_list:
        return gpd.GeoDataFrame()

    gdf_combined = pd.concat(gdf_list, ignore_index=True)
    logging.info(f"Combined {len(gdf_list)} daily files into a single with {len(gdf_combined)} records in total.")

    # Generate all possible timestamps for the day in 10-minute intervals
    date_obj = pd.to_datetime(date_str)
    start_time = date_obj.replace(hour=0, minute=0)
    end_time = date_obj.replace(hour=23, minute=59)
    all_timestamps = pd.date_range(start=start_time, end=end_time, freq='10T', tz='Europe/Zurich')

    # Find missing timestamps
    existing_timestamps = pd.to_datetime(gdf_combined['timestamp']).dt.tz_convert('Europe/Zurich')
    missing_timestamps = all_timestamps.difference(existing_timestamps)

    # Format missing timestamps
    logging.info(f"Found {len(missing_timestamps)} missing timestamps for {date_str}.")
    missing_timestamps_str = missing_timestamps.strftime('%Y-%m-%d_%H-%M%z').tolist()

    return gdf_combined, missing_timestamps_str


def compute_daily_stats(gdf_points, gdf_polygons, polygon_id_column, missing_timestamps_str):
    """
    Spatially joins point data (e.g., scooters/bikes) to a polygon layer and computes
    various daily statistics.

    For each group, the function calculates the different metrics for both the count
    of records and the current range in meters (xs_current_range_meters)

    Returns a pandas DataFrame with one row per group containing these statistics.
    """
    if gdf_points.empty:
        logging.warning("No points in the combined GDF; returning empty stats DataFrame.")
        return pd.DataFrame()

    gdf_joined = gpd.sjoin(gdf_points, gdf_polygons, how="left", predicate="intersects")

    group_cols = [
        "xs_provider_name",
        "xs_vehicle_type_name",
        "xs_form_factor",
        "xs_propulsion_type",
        "xs_max_range_meters",
        "xs_rental_uris",
        polygon_id_column
    ]

    # Ensure the timestamp column is in datetime format
    gdf_joined['timestamp'] = pd.to_datetime(gdf_joined['timestamp']).dt.tz_convert('Europe/Zurich')
    # Generate all possible timestamps (every 10 minutes in the data range)
    min_time = gdf_joined['timestamp'].min().floor('10T')
    max_time = gdf_joined['timestamp'].max().ceil('10T')
    all_timestamps = pd.date_range(start=min_time, end=max_time, freq='10T', tz='Europe/Zurich')
    # Remove timestamps which are completetly missing in the data
    all_timestamps = all_timestamps[~all_timestamps.strftime('%Y-%m-%d_%H-%M%z').isin(missing_timestamps_str)]

    # Create a DataFrame with all combinations of group columns and timestamps
    group_combinations = gdf_joined[group_cols].drop_duplicates()
    all_combinations = pd.merge(
        group_combinations.assign(key=1),
        pd.DataFrame({'timestamp': all_timestamps}).assign(key=1),
        on='key'
    ).drop('key', axis=1)

    # Merge with the actual data, filling missing rows with count = 0
    df_count_grouped = (
        all_combinations
        .merge(
            gdf_joined.groupby(group_cols + ['timestamp'], dropna=False)
            .agg(count=("xs_provider_name", "size"))
            .reset_index(),
            on=group_cols + ['timestamp'],
            how='left'
        )
        .fillna({'count': 0})  # Fill missing time intervals with count 0
    )

    # Compute counting stats
    counting_stats = df_count_grouped.groupby(group_cols, dropna=False).agg(
        mean=("count", "mean"),
        min=("count", "min"),
        max=("count", "max"),
        median=("count", "median"),
        q1=("count", lambda x: x.quantile(0.25)),
        q3=("count", lambda x: x.quantile(0.75)),
    ).reset_index()
    logging.info(f"Computed counting stats for {len(counting_stats)} groups.")

    # Compute range stats (without filling 0s since range metrics shouldn't be artificially set)
    range_stats = gdf_joined.groupby(group_cols, dropna=False).agg(
        current_range_meters_mean=("xs_current_range_meters", "mean"),
        current_range_meters_min=("xs_current_range_meters", "min"),
        current_range_meters_max=("xs_current_range_meters", "max"),
        current_range_meters_median=("xs_current_range_meters", "median"),
        current_range_meters_q1=("xs_current_range_meters", lambda x: x.quantile(0.25)),
        current_range_meters_q3=("xs_current_range_meters", lambda x: x.quantile(0.75))
    ).reset_index()
    logging.info(f"Computed range stats for {len(range_stats)} groups.")

    # Merge everything together
    grouped_stats = counting_stats.merge(range_stats, on=group_cols, how="left")
    grouped_stats = grouped_stats.merge(gdf_polygons, on=polygon_id_column, how="left")

    return grouped_stats


def save_daily_stats(df_stats, prefix, date_str):
    """
    Save the daily stats to a CSV and upload to the FTP.
    Publish the ODS dataset with the given ODS ID.
    Example: 'bezirke_stats_2025-02-01.csv'
    """
    if df_stats.empty:
        logging.warning(f"No stats to save for {prefix} on {date_str}.")
        return

    output_folder = os.path.join(credentials.data_path, "stats", prefix, date_str[:4])
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    output_file = os.path.join(output_folder, f"{prefix}_stats_{date_str}.csv")
    df_stats.to_csv(output_file, index=False, encoding="utf-8")
    logging.info(f"Saved daily stats to {output_file}")

    # Archiving
    remote_path = f"mobilitaet/mikromobilitaet/stats/{prefix}/{date_str[:4]}"
    common.ensure_ftp_dir(
        common.credentials.ftp_server,
        common.credentials.ftp_user,
        common.credentials.ftp_pass,
        remote_path
    )
    common.upload_ftp(
        output_file,
        common.credentials.ftp_server,
        common.credentials.ftp_user,
        common.credentials.ftp_pass,
        remote_path
    )


def main():
    logging.basicConfig(level=logging.INFO)

    date_str_start = (datetime.now() - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    date_str_end = (datetime.now() - pd.Timedelta(days=1)).strftime("%Y-%m-%d")

    for date_str in pd.date_range(date_str_start, date_str_end, freq="D").strftime("%Y-%m-%d"):
        get_files_from_ftp_for_day(date_str)

        gdf_daily_points, missing_timestamps_str = combine_daily_files_to_gdf(date_str)

        # Download spatial descriptors
        #    * Bezirke Basel-Stadt (100039)
        gdf_bezirke = download_spatial_descriptors("100039")
        #    * Wohnviertel Basel-Stadt (100042) to get wov_name and gemeinde_name of the bezirk
        gdf_wohnviertel = download_spatial_descriptors("100042")
        gdf_bezirke = gdf_bezirke.merge(gdf_wohnviertel[['wov_id', 'wov_name', 'gemeinde_na']],
                                        on='wov_id', how='left')
        #    * Sperr- und Parkverbotszonen (100332)
        gdf_verbotszonen = download_spatial_descriptors("100332")

        df_bezirke_stats = compute_daily_stats(gdf_daily_points, gdf_bezirke, "bez_id", missing_timestamps_str)
        df_verbotszonen_stats = compute_daily_stats(gdf_daily_points, gdf_verbotszonen, "id_verbot", missing_timestamps_str)

        save_daily_stats(df_bezirke_stats, prefix="bezirke", date_str=date_str)
        save_daily_stats(df_verbotszonen_stats, prefix="verbotszonen", date_str=date_str)

    for ods_id in ['100414', '100418']:
        odsp.publish_ods_dataset_by_id(ods_id)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info('Job successful!')
