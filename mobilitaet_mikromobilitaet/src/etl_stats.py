import os
import io
import zipfile
import logging
import pandas as pd
import geopandas as gpd
from datetime import datetime
from dateutil.relativedelta import relativedelta

import common
import ods_publish.etl_id as odsp
from dotenv import load_dotenv

load_dotenv()

DATA_PATH = os.getenv("DATA_PATH")
TEMP_PATH = os.getenv("TEMP_PATH")

CONFIGS = {
    'bezirke': {
        'ods_id_shapes': '100039',
        'ods_id': '100416',
        'remove_empty_polygon_columns': False,
        'group_cols': [
            'xs_provider_name', 'xs_vehicle_type_name', 'xs_form_factor',
            'xs_propulsion_type', 'xs_max_range_meters', 'bez_id'
        ],
        'output_cols': [
            'date', 'bez_id', 'bez_name', 'wov_id', 'wov_name', 'gemeinde_na', 'geometry',
            'xs_provider_name', 'xs_vehicle_type_name', 'xs_form_factor', 'xs_propulsion_type',
            'xs_max_range_meters', 'num_measures', 'mean', 'min', 'max'
        ]
    },
    'gemeinden': {
        'ods_id_shapes': '100017',
        'ods_id': '100422',
        'remove_empty_polygon_columns': True,
        'group_cols': ['xs_provider_name', 'objid'],
        'output_cols': [
            'date', 'objid', 'name', 'geometry', 'xs_provider_name',
            'num_measures', 'mean', 'min', 'max'
        ]
    }
}

MONTHLY_CONFIGS = {
    'bezirke': {
        'ods_id_shapes': '100039',
        'ods_id': '100418',
        'remove_empty_polygon_columns': False,
        'group_cols': [
            'xs_provider_name', 'xs_vehicle_type_name', 'xs_form_factor',
            'xs_propulsion_type', 'xs_max_range_meters', 'bez_id'
        ],
        'output_cols': [
            'date', 'bez_id', 'bez_name', 'wov_id', 'wov_name', 'gemeinde_na', 'geometry',
            'xs_provider_name', 'xs_vehicle_type_name', 'xs_form_factor', 'xs_propulsion_type',
            'xs_max_range_meters', 'num_measures', 'mean', 'min', 'max'
        ]
    }
}


def download_spatial_descriptors(ods_id):
    """
    Download and extract a shapefile from data.bs.ch for a given ODS dataset ID.
    Returns a GeoDataFrame in EPSG:2056.
    """
    url_to_shp = f'https://data.bs.ch/explore/dataset/{ods_id}/download/?format=shp'
    r = common.requests_get(url_to_shp)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    extract_folder = os.path.join(DATA_PATH, ods_id)
    if not os.path.exists(extract_folder):
        os.makedirs(extract_folder)

    z.extractall(extract_folder)
    path_to_shp = os.path.join(extract_folder, f"{ods_id}.shp")

    gdf = gpd.read_file(path_to_shp, encoding='utf-8')
    return gdf.to_crs("EPSG:2056")


def combine_files_to_gdf(dates, start_hour=0, end_hour=24):
    """
    Given a list of date strings (e.g. ["2025-02-03", "2025-02-10", ...]) and optional start_hour/end_hour,
    read each local .gpkg file in that date/hour range into a GeoDataFrame, concatenate them,
    and ensure they are in EPSG:2056.

    Returns the combined GeoDataFrame and a list of all missing timestamps (in string form).
    """
    year_month = pd.to_datetime(dates[0]).strftime("%Y-%m")
    local_folder = os.path.join(DATA_PATH, "archiv", year_month)

    if not os.path.exists(local_folder):
        logging.error(f"Local folder {local_folder} does not exist.")
        return gpd.GeoDataFrame(), []

    # We'll accumulate partial GDFs for each date.
    all_gdf_parts = []
    all_missing_timestamps = []
    for date_str in dates:
        date_obj = pd.to_datetime(date_str)
        start_dt = date_obj.replace(hour=start_hour, minute=0)
        if end_hour == 24:
            end_dt = date_obj.replace(hour=0, minute=0) + pd.Timedelta(days=1)
        else:
            end_dt = date_obj.replace(hour=end_hour, minute=0)

        # Generate the list of all 10-min timestamps in [start_hour, end_hour).
        all_timestamps = pd.date_range(
            start_dt, end_dt, freq='10T', inclusive='left', tz='Europe/Zurich'
        )
        
        found_timestamps = []
        gdf_list = []
        # Iterate over the gpkg files in our local folder.
        for file in os.listdir(local_folder):
            if file.endswith(".gpkg"):
                # For example: 2025-02-03_09-30_part.gpkg
                file_base = file.replace(".gpkg", "")
                file_ts = pd.to_datetime(file_base, format="%Y-%m-%d_%H-%M%z", errors='coerce')

                if file_ts is not None:
                    # Check if this file_ts is within our date_str day AND within the hour range.
                    if file_ts.date() == date_obj.date() and (start_hour <= file_ts.hour < end_hour):
                        found_timestamps.append(file_ts)

                        path = os.path.join(local_folder, file)
                        gdf_part = gpd.read_file(path)

                        # Reproject if needed
                        if gdf_part.crs is not None and gdf_part.crs.to_epsg() != 2056:
                            gdf_part = gdf_part.to_crs(epsg=2056)

                        gdf_list.append(gdf_part)
                else:
                    logging.warning(f"Skipping non-timestamped file {file}.")
            else:
                logging.warning(f"Skipping non-GPKG file {file}.")

        if gdf_list:
            combined_for_day = pd.concat(gdf_list, ignore_index=True)
            all_gdf_parts.append(combined_for_day)
            logging.info(
                f"Combined {len(gdf_list)} partial files into a single GDF with "
                f"{len(combined_for_day)} records for {date_str} (hours {start_hour}-{end_hour})."
            )
        else:
            logging.info(f"No GPKG files found for {date_str} in hours {start_hour}-{end_hour}.")
            continue

        # Identify missing timestamps for this specific date
        missing_timestamps = all_timestamps.difference(found_timestamps)
        missing_timestamps_str = missing_timestamps.strftime('%Y-%m-%d_%H-%M%z').tolist()

        logging.info(
            f"Found {len(missing_timestamps)} missing timestamps for {date_str} in hour range {start_hour}-{end_hour}."
        )
        all_missing_timestamps.extend(missing_timestamps_str)

    if not all_gdf_parts:
        # Return an empty GeoDataFrame if we never found anything.
        return gpd.GeoDataFrame(), []

    # Concatenate across all requested dates.
    gdf_combined = pd.concat(all_gdf_parts, ignore_index=True)

    logging.info(
        f"Final combined GDF has {len(gdf_combined)} records across {len(dates)} date(s) "
        f"for hours {start_hour}-{end_hour}."
    )

    return gdf_combined, all_missing_timestamps


def compute_stats(gdf_points, gdf_polygons, group_cols, remove_empty_polygon_columns,
                  missing_timestamps_str=None, start_hour=None, end_hour=None):
    """
    Spatially joins point data to a polygon layer and computes statistics.
    When start_time and end_time are provided, statistics are computed only for that period.
    If missing_timestamps_str is provided, those timestamps are removed from the period.
    Returns a pandas DataFrame with one row per group containing the computed statistics.
    """
    if gdf_points.empty:
        logging.warning("No points in the combined GDF; returning empty stats DataFrame.")
        return pd.DataFrame()

    gdf_joined = gpd.sjoin(gdf_points, gdf_polygons, how="left", predicate="intersects")

    # Ensure the timestamp column is in datetime format
    gdf_joined['timestamp'] = pd.to_datetime(gdf_joined['timestamp']).dt.tz_convert('Europe/Zurich')

    period_start = gdf_joined['timestamp'].min().floor('10T')
    period_end = gdf_joined['timestamp'].max().ceil('10T')
    all_timestamps = pd.date_range(start=period_start, end=period_end, freq='10T', tz='Europe/Zurich')
    if start_hour is not None and end_hour is not None:
        # Build a partial set of timestamps for each day
        all_timestamp_list = []
        days = gdf_joined['timestamp'].dt.normalize().unique()
        for day in days:
            daily_start = day + pd.Timedelta(hours=start_hour)
            daily_end = day + pd.Timedelta(hours=end_hour)

            # Generate just the 10-minute intervals we care about in [start_hour, end_hour)
            if daily_start < daily_end:
                times = pd.date_range(
                    start=daily_start,
                    end=daily_end,
                    freq='10T',
                    tz='Europe/Zurich',
                    inclusive='left'
                )
                all_timestamp_list.extend(times)

        all_timestamps = pd.DatetimeIndex(all_timestamp_list)

    if missing_timestamps_str:
        all_timestamps = all_timestamps[~all_timestamps.strftime('%Y-%m-%d_%H-%M%z').isin(missing_timestamps_str)]

    # Create combinations of groups and timestamps.
    group_combinations = gdf_joined[group_cols].drop_duplicates()
    all_combinations = pd.merge(
        group_combinations.assign(key=1),
        pd.DataFrame({'timestamp': all_timestamps}).assign(key=1),
        on='key'
    ).drop('key', axis=1)

    # Merge with the actual data, filling missing rows with count = 0.
    df_count_grouped = (
        all_combinations
        .merge(
            gdf_joined.groupby(group_cols + ['timestamp'], dropna=False)
            .agg(count=("xs_provider_name", "size"))
            .reset_index(),
            on=group_cols + ['timestamp'],
            how='left'
        )
        .fillna({'count': 0})
    )

    # Compute counting stats.
    counting_stats = df_count_grouped.groupby(group_cols, dropna=False).agg(
        sum=("count", "sum"),
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

    # Merge counting and range stats.
    grouped_stats = counting_stats.merge(range_stats, on=group_cols, how="left")
    polygon_id_column = group_cols[-1]
    grouped_stats = grouped_stats.merge(gdf_polygons, on=polygon_id_column, how="left")
    grouped_stats['num_measures'] = len(all_timestamps)

    # Remove rows where polygon_id is NaN if remove_empty_polygon_columns is True
    if remove_empty_polygon_columns:
        grouped_stats = grouped_stats.dropna(subset=[polygon_id_column])
        logging.info(f"Removed rows with NaN values in {polygon_id_column}")

    return grouped_stats


def save_stats(df_stats, prefix, date_str, columns_of_interest, timerange_label=None):
    """
    Save the stats to a CSV and upload to the FTP.
    If timerange_label is provided, it is included in the filename (for monthly timerange stats);
    otherwise, a daily stats filename is created.
    """
    if df_stats.empty:
        logging.warning(f"No stats to save for {prefix} on {date_str}.")
        return

    # Create output folder structure.
    output_folder = os.path.join(DATA_PATH, "stats", prefix, date_str[:4])
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    if timerange_label:
        output_file = os.path.join(output_folder, f"{prefix}_stats_{date_str}_{timerange_label}.csv")
    else:
        output_file = os.path.join(output_folder, f"{prefix}_stats_{date_str}.csv")

    df_stats = df_stats[columns_of_interest]
    df_stats.to_csv(output_file, index=False, encoding="utf-8")
    logging.info(f"Saved stats to {output_file}")

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


def process_daily_stats(date_str):
    """
    Process daily stats for a given date.
    """
    gdf_daily_points, missing_timestamps_str = combine_files_to_gdf([date_str])
    for stats_type, config in CONFIGS.items():
        gdf_shapes = download_spatial_descriptors(config['ods_id_shapes'])
        if stats_type == 'bezirke': 
            gdf_wohnviertel = download_spatial_descriptors("100042")
            gdf_shapes = gdf_shapes.merge(gdf_wohnviertel[['wov_id', 'wov_name', 'gemeinde_na']], on='wov_id', how='left')

        df_stats = compute_stats(
            gdf_daily_points,
            gdf_shapes,
            config['group_cols'],
            config['remove_empty_polygon_columns'],
            missing_timestamps_str
        )
        df_stats['date'] = date_str
        save_stats(
            df_stats,
            stats_type,
            date_str,
            config['output_cols']
        )
        odsp.publish_ods_dataset_by_id(config['ods_id'])


def process_monthly_timerange_stats(year, month):
    """
    Process stats for a given month. For each weekday (Monday to Sunday) and for each timerange,
    aggregate stats across all days (including missing timestamps).
    """
    # Define timeranges (adjust as needed)
    timeranges = [
        ("00:00", "03:00"),
        ("03:00", "06:00"),
        ("06:00", "09:00"),
        ("09:00", "12:00"),
        ("12:00", "15:00"),
        ("15:00", "18:00"),
        ("18:00", "21:00"),
        ("21:00", "24:00")
    ]
    # Define start and end of month.
    start_date = pd.Timestamp(year=year, month=month, day=1)
    end_date = start_date + relativedelta(months=1) - pd.Timedelta(days=1)
    
    # For each weekday: 0=Monday, …, 6=Sunday.
    for weekday in range(7):
        days_of_week = [day for day in pd.date_range(start_date, end_date, freq="D") if day.weekday() == weekday]
        if not days_of_week:
            continue

        # Process each timerange for the current weekday.
        for start_str, end_str in timeranges:
            date_strs = [day.strftime("%Y-%m-%d") for day in days_of_week]
            gdf_daily_points, missing_timestamps_str = combine_files_to_gdf(date_strs, start_hour=int(start_str[:2]), end_hour=int(end_str[:2]))
            
            # Download shapes for the given config. Here we assume processing for one config, e.g. 'bezirke'
            config = MONTHLY_CONFIGS['bezirke']
            gdf_shapes = download_spatial_descriptors(config['ods_id_shapes'])
            # For 'bezirke' we also merge additional attributes.
            gdf_wohnviertel = download_spatial_descriptors("100042")
            gdf_shapes = gdf_shapes.merge(gdf_wohnviertel[['wov_id', 'wov_name', 'gemeinde_na']], on='wov_id', how='left')
            
            df_stats = compute_stats(
                gdf_daily_points,
                gdf_shapes,
                config['group_cols'],
                config['remove_empty_polygon_columns'],
                missing_timestamps_str,
                start_hour=int(start_str[:2]),
                end_hour=int(end_str[:2])
            )
            df_stats['date'] = start_date.strftime("%Y-%m")
            df_stats['weekday'] = weekday
            df_stats['timerange_start'] = start_str
            df_stats['timerange_end'] = end_str

            timerange_label = f"{start_str.replace(':','')}_{end_str.replace(':','')}_wd{weekday}"
            # Using the date_str from the month start as an identifier.
            save_stats(
                df_stats,
                'bezirke_timerange',
                start_date.strftime("%Y-%m"),
                config['output_cols'],
                timerange_label=timerange_label
            )
        # Publish dataset after processing each weekday.
        odsp.publish_ods_dataset_by_id(MONTHLY_CONFIGS['bezirke']['ods_id'])


def main():
    # Process daily stats for each day between a given start and end date.
    date_str_start = (datetime.now() - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    date_str_end = (datetime.now() - pd.Timedelta(days=1)).strftime("%Y-%m-%d")

    for date_str in pd.date_range(date_str_start, date_str_end, freq="D").strftime("%Y-%m-%d"):
        process_daily_stats(date_str)

    # If today is the first of the month, process the previous month for timerange stats.
    if datetime.now().day == 12:
        last_month_date = datetime.now() - relativedelta(months=1)
        process_monthly_timerange_stats(last_month_date.year, last_month_date.month)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info('Job successful!')
