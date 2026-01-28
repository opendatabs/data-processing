import ftplib
import logging
import os
from datetime import datetime, timedelta

import common
import pandas as pd
import urllib3
from dotenv import load_dotenv

load_dotenv()
FTP_SERVER = os.getenv("FTP_SERVER")
FTP_USER = os.getenv("FTP_USER_04")
FTP_PASS = os.getenv("FTP_PASS_04")
ODS_PUSH_URL = os.getenv("ODS_PUSH_URL_100087")

TODAY_STRING = datetime.today().strftime("%Y%m%d")
YESTERDAY_STRING = datetime.strftime(datetime.today() - timedelta(1), "%Y%m%d")


def main():
    logging.info("Connecting to FTP Server to read data...")
    stations, local_files = download_data_files()
    dfs = {}
    all_data = pd.DataFrame(columns=["LocalDateTime", "Value", "Latitude", "Longitude", "EUI"])
    logging.info("Reading csv files into data frames...")
    urllib3.disable_warnings()
    for station in stations:
        logging.info(f'Retrieving latest timestamp for station "{station}" from ODS...')
        r = common.requests_get(
            url=f"https://data.bs.ch/api/records/1.0/search/?dataset=100087&q=&rows=1&sort=timestamp&refine.station_id={station}",
            verify=False,
        )
        r.raise_for_status()
        latest_ods_timestamp = r.json()["records"][0]["fields"]["timestamp"]
        logging.info(f"Latest timestamp is {latest_ods_timestamp}.")
        for date_string in [YESTERDAY_STRING, TODAY_STRING]:
            try:
                logging.info(f"Reading {local_files[(station, date_string)]}...")
                df = pd.read_csv(local_files[(station, date_string)], sep=";", na_filter=False)
                # Filter out invalid measurements
                df = df[df["Value"] != 24.1]
                logging.info("Calculating ISO8601 time string...")
                df["timestamp"] = pd.to_datetime(
                    df.LocalDateTime, format="%d.%m.%Y %H:%M", errors="coerce"
                ).dt.tz_localize("Europe/Zurich", ambiguous="infer")
                # Handle bad cases 14.09.2023
                is_invalid_hour = df["timestamp"].dt.hour == 24
                df.loc[is_invalid_hour, "timestamp"] -= pd.DateOffset(hours=24)

                df.set_index("timestamp", drop=False, inplace=True)
                df["station_id"] = station
                all_data = pd.concat([all_data, df], sort=True)
                dfs[(station, date_string)] = df

                logging.info(f"Filtering data after {latest_ods_timestamp} for submission to ODS via realtime API...")
                realtime_df = df[df["timestamp"] > latest_ods_timestamp]

                logging.info(f"Pushing {realtime_df.timestamp.count()} rows to ODS realtime API...")
                for index, row in realtime_df.iterrows():
                    timestamp_text = row.timestamp.strftime("%Y-%m-%dT%H:%M:%S%z")
                    payload = {
                        "eui": row.EUI,
                        "timestamp": timestamp_text,
                        "value": row.Value,
                        "longitude": row.Longitude,
                        "latitude": row.Latitude,
                        "station_id": row.station_id,
                    }
                    logging.info(f"Pushing row {index} with with the following data to ODS: {payload}")
                    r = common.requests_post(url=ODS_PUSH_URL, json=payload, verify=False)
                    r.raise_for_status()
            except KeyError:
                logging.info(f"No file found with keys {(station, date_string)}, ignoring...")

    all_data = all_data[
        [
            "station_id",
            "timestamp",
            "Value",
            "Latitude",
            "Longitude",
            "EUI",
            "LocalDateTime",
        ]
    ]
    today_data_file = os.path.join("data", "schall_aktuell.csv")
    logging.info(f"Exporting yesterday's and today's data to {today_data_file}...")
    all_data.to_csv(today_data_file, index=False)

    # todo: Simplify code by pushing yesterday's and today's data to ODS in one batch (as in lufthygiene_pm25)

    logging.info("Creating stations file from current data file...")
    df_stations = all_data.drop_duplicates(["EUI"])[["station_id", "Latitude", "Longitude", "EUI"]]
    stations_file = os.path.join("data", "stations/stations.csv")
    logging.info(f"Exporting stations file to {stations_file}...")
    df_stations.to_csv(stations_file, index=False)

    common.upload_ftp(stations_file, remote_path="aue/schall_stationen")
    common.upload_ftp(today_data_file, remote_path="aue/schall_messung/realtime")


# Retry with some delay in between if any explicitly defined error is raised
@common.retry(common.ftp_errors_to_handle, tries=6, delay=10, backoff=1)
def download_data_files():
    ftp = ftplib.FTP(FTP_SERVER, FTP_USER, FTP_PASS)
    logging.info("Changing to remote dir schall...")
    ftp.cwd("schall")
    logging.info("Retrieving list of files...")
    stations = []
    local_files = {}
    for file_name, facts in ftp.mlsd():
        # If we only use today's date we might lose some values just before midnight yesterday.
        for date_string in [YESTERDAY_STRING, TODAY_STRING]:
            if date_string in file_name and "OGD" in file_name:
                logging.info(
                    f"File {file_name} has 'OGD' and '{date_string}' in its filename. "
                    f"Parsing station name from filename..."
                )
                station = file_name.replace(f"_{date_string}.csv", "").replace("airmet_auebs_", "").replace("_OGD", "")
                stations.append(station)
                logging.info(f"Downloading {file_name} for station {station}...")
                local_file = os.path.join("data", file_name)
                with open(local_file, "wb") as f:
                    ftp.retrbinary(f"RETR {file_name}", f.write)
                local_files[(station, date_string)] = local_file
    ftp.quit()

    return stations, local_files


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f"Executing {__file__}...")
    main()
    logging.info("Job successful!")
