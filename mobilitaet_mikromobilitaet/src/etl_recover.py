import os
import logging
import pandas as pd
import geopandas as gpd
from dotenv import load_dotenv

load_dotenv()

DATA_PATH = os.getenv("DATA_PATH")
ARCHIVE_PATH = os.path.join(DATA_PATH, "archiv")
TIMESERIES_FILE = os.path.join(DATA_PATH, "zeitreihe_verfuegbarkeit.gpkg")
TEMP_PATH = os.getenv("TEMP_PATH")

logging.basicConfig(level=logging.INFO)

def gpd_to_mounted_file(gdf, path, *args, **kwargs):
    """
    Writes a file using geopandas.to_file,
    but writes it first into a temporary file to avoid
    geopandas errors when reading from mounted volumes.
    """
    filename = os.path.basename(path)
    temp_path = os.path.join(TEMP_PATH, filename)
    gdf.to_file(temp_path, *args, **kwargs)
    os.replace(temp_path, path)

def reconstruct_archived_files(start_time, end_time, interval_minutes=10):
    """
    Reconstruct missing archive files from the timeseries dataset.
    """
    logging.info("Loading timeseries data...")
    gdf_timeseries = gpd.read_file(TIMESERIES_FILE)
    gdf_timeseries['timestamp'] = pd.to_datetime(gdf_timeseries['timestamp'])
    gdf_timeseries['timestamp_moved'] = pd.to_datetime(gdf_timeseries['timestamp_moved'])
    gdf_timeseries['timestamp'] = gdf_timeseries['timestamp'].dt.tz_localize(None)  # Remove timezone
    gdf_timeseries['timestamp_moved'] = gdf_timeseries['timestamp_moved'].dt.tz_localize(None)

    current_time = start_time
    while current_time <= end_time:
        timestamp_str = current_time.strftime('%Y-%m-%d %H:%M:%S')
        folder = current_time.strftime('%Y-%m')
        filename_ts = current_time.strftime('%Y-%m-%d_%H-%M') + '+0100'
        path_export_archive = os.path.join(ARCHIVE_PATH, folder, f'{filename_ts}.gpkg')

        # Filter data where timestamp <= current_time < timestamp_moved
        gdf_filtered = gdf_timeseries[
            (gdf_timeseries['timestamp'] <= current_time) &
            ((gdf_timeseries['timestamp_moved'].isna()) | (gdf_timeseries['timestamp_moved'] >= current_time))
        ]
        gdf_filtered['timestamp_moved'] = None
        # Bring timestamp back to its original format
        gdf_filtered['timestamp'] = gdf_filtered['timestamp'].dt.tz_localize('Europe/Zurich').dt.strftime('%Y-%m-%d %H:%M:%S%z')
        if not gdf_filtered.empty:
            os.makedirs(os.path.dirname(path_export_archive), exist_ok=True)
            gpd_to_mounted_file(gdf_filtered, path_export_archive, driver='GPKG')
            logging.info(f"Reconstructed {path_export_archive}")
        else:
            logging.warning(f"No data found for {timestamp_str}, skipping...")

        current_time += pd.Timedelta(minutes=interval_minutes)

if __name__ == "__main__":
    start_time = pd.Timestamp("2025-03-12 17:20:00")
    end_time = pd.Timestamp("2025-03-13 08:10:00")
    reconstruct_archived_files(start_time, end_time)