import logging
import os
from datetime import datetime, timedelta

import common
import pandas as pd
from common import ODS_API_KEY
from dotenv import load_dotenv

load_dotenv()

ODS_PUSH_URL = os.getenv("ODS_PUSH_URL_100047")
HTTPS_URL = os.getenv("HTTPS_URL_LADESTATIONEN")
HTTPS_URL_AUTH = os.getenv("HTTPS_URL_AUTH_LADESTATIONEN")
HTTPS_USER = os.getenv("HTTPS_USER_LADESTATIONEN")
HTTPS_PASS = os.getenv("HTTPS_PASS_LADESTATIONEN")
API_KEY = os.getenv("API_KEY_LADESTATIONEN")


def main():
    latest_ods_start_time = get_latest_ods_start_time()
    from_filter = datetime.fromisoformat(latest_ods_start_time) - timedelta(days=7)
    logging.info(f"Latest starttime in ods: {latest_ods_start_time}, retrieving charges from {from_filter}...")

    token = authenticate()
    df = extract_data(token=token, from_filter=from_filter)
    size = df.shape[0]
    logging.info(f"{size} charges to be processed.")
    if size > 0:
        df_export = transform_data(df)
        common.ods_realtime_push_df(df_export, ODS_PUSH_URL)
    logging.info("Job successful!")


def transform_data(df):
    logging.info("Transforming data for export...")
    df_export = df[
        [
            "startTime",
            "stopTime",
            "duration",
            "wattHour",
            "connectorId",
            "station.location.coordinates.lat",
            "station.location.coordinates.lng",
        ]
    ].copy(deep=True)
    df_export["kiloWattHour"] = df_export["wattHour"] / 1000
    df_export["station.capacity"] = 22
    df_export["station.connectorType"] = 2
    df_export["startTimeText"] = df_export.startTime
    df_export["stopTimeText"] = df_export.stopTime
    df_export["station.location"] = (
        df_export["station.location.coordinates.lat"].astype(str)
        + ","
        + df_export["station.location.coordinates.lng"].astype(str)
    )
    return df_export


def get_latest_ods_start_time():
    logging.info("Getting latest entry from ODS dataset...")
    ods_dataset_query_url = "https://data.bs.ch/api/records/1.0/search/"
    params = {
        "dataset": "100047",
        "sort": "starttime",
        "apikey": ODS_API_KEY,
    }
    r = common.requests_get(url=ods_dataset_query_url, params=params)
    r.raise_for_status()
    record_count = len(r.json()["records"])
    # if dataset is empty: return 1970-01-01
    latest_ods_start_time = (
        "1970-01-01T00:00:00+00:00" if record_count == 0 else r.json()["records"][0]["fields"]["starttime"]
    )
    return latest_ods_start_time


def extract_data(token, from_filter):
    logging.info("Retrieving data...")
    headers = {"authorization": f"Bearer {token}", "x-api-key": API_KEY}
    r = common.requests_get(
        url=f"{HTTPS_URL}",
        params={"perPage": 1000, "from": from_filter},
        headers=headers,
    )
    r.raise_for_status()
    df = pd.json_normalize(r.json())
    return df


def authenticate():
    logging.info("Getting auth token...")
    payload = {
        "username": HTTPS_USER,
        "password": HTTPS_PASS,
    }
    headers = {"x-api-key": API_KEY, "content-type": "application/json"}
    r = common.requests_post(url=HTTPS_URL_AUTH, json=payload, headers=headers)
    return r.json()["token"]


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f"Executing {__file__}...")
    main()
