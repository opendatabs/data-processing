import json
import logging
import os
from datetime import datetime
from functools import reduce

import common
import pandas as pd
import urllib3
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth

load_dotenv()

HTTPS_URL = os.getenv("HTTPS_URL_01")
HTTPS_USER = os.getenv("HTTPS_USER_01")
HTTPS_PASS = os.getenv("HTTPS_PASS_01")
RHEIN_FILES = json.loads(os.getenv("RHEIN_FILES", "[]"))
BIRS_FILES = json.loads(os.getenv("BIRS_FILES", "[]"))
WIESE_FILES = json.loads(os.getenv("WIESE_FILES", "[]"))
RHEIN_KLINGENTHAL_FILES = json.loads(os.getenv("RHEIN_KLINGENTHAL_FILES", "[]"))
RHEIN_ODS_PUSH_URL = os.getenv("ODS_PUSH_URL_100089")
BIRS_ODS_PUSH_URL = os.getenv("ODS_PUSH_URL_100236")
WIESE_ODS_PUSH_URL = os.getenv("ODS_PUSH_URL_100235")
RHEIN_KLINGENTHAL_ODS_PUSH_URL = os.getenv("ODS_PUSH_URL_100243")


def process_river(river_files, river_name, river_id, variable_names, push_url):
    print("Loading data into data frames...")
    dfs = []
    for file in river_files:
        response = common.requests_get(
            f"{HTTPS_URL}/{file}",
            auth=HTTPBasicAuth(HTTPS_USER, HTTPS_PASS),
            stream=True,
        )
        df = pd.read_csv(response.raw, parse_dates=True, infer_datetime_format=True)
        dfs.append(df)
    print("Merging data frames...")
    all_df = reduce(lambda left, right: pd.merge(left, right, on=["Time"], how="outer"), dfs)
    all_filename = f"data/{river_name}/{river_name}_hydrodata_{datetime.today().strftime('%Y-%m-%d')}.csv"
    all_df.to_csv(all_filename, index=False)
    ftp_dir = f"hydrodata.ch/data/{river_name}"
    common.upload_ftp(all_filename, remote_path=ftp_dir)
    print("Processing data...")
    merged_df = all_df.copy(deep=True)
    merged_df["timestamp"] = pd.to_datetime(merged_df.Time, infer_datetime_format=True)
    # timestamp is a text column used tof pushing into ODS realtime API
    merged_df["timestamp_text"] = merged_df.timestamp.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
    merged_df["datum"] = merged_df.timestamp.dt.strftime("%d.%m.%Y")
    merged_df["zeit"] = merged_df.timestamp.dt.strftime("%H:%M")
    merged_df["intervall"] = 5
    merged_df["pegel"] = merged_df[variable_names["pegel"]]
    columns_to_export = ["timestamp", "pegel", "datum", "zeit", "intervall"]
    columns_to_push = ["timestamp_text", "pegel"]
    if "temperatur" in variable_names:
        merged_df["temperatur"] = merged_df[variable_names["temperatur"]]
        columns_to_export.insert(2, "temperatur")
        columns_to_push.insert(2, "temperatur")
    if "abfluss" in variable_names:
        merged_df["abfluss"] = merged_df[variable_names["abfluss"]]
        columns_to_export.insert(2, "abfluss")
        columns_to_push.insert(2, "abfluss")
        merged_df = merged_df.dropna(subset=["abfluss"], how="all")
    # merged_df = merged_df[['datum', 'zeit', 'abfluss', 'intervall', 'pegel', 'timestamp_dt', 'timestamp']]
    # drop rows if all cells are empty in certain columns
    merged_df = merged_df.dropna(subset=["pegel"], how="all")
    local_path = f"data/{river_name}"
    merged_filename = os.path.join(
        local_path,
        f"{river_id}_pegel_abfluss_{datetime.today().strftime('%Y-%m-%d')}.csv",
    )
    print(f"Exporting data to {merged_filename}...")
    merged_df.to_csv(merged_filename, columns=columns_to_export, index=False)
    ftp_remote_dir = f"hydrodata.ch/hydropro/{river_id}/realtime"
    common.upload_ftp(merged_filename, remote_path=ftp_remote_dir)
    urllib3.disable_warnings()
    # print(f'Retrieving latest record from ODS...')
    # r = common.requests_get(url='https://data.bs.ch/api/records/1.0/search/?dataset=100089&q=&rows=1&sort=timestamp', verify=False)
    # r.raise_for_status()
    # latest_ods_value = r.json()['records'][0]['fields']['timestamp']
    # print(f'Filtering data after {latest_ods_value} for submission to ODS via realtime API...')
    # realtime_df = merged_df[merged_df['timestamp'] > latest_ods_value]
    realtime_df = merged_df
    if len(realtime_df) == 0:
        print("No rows to push to ODS... ")
    else:
        # Realtime API bootstrap data:
        # {
        #   "timestamp": "2020-07-28T01:35:00+02:00",
        #   "pegel": "245.16",
        #   "abfluss": "591.2",
        #   "temperatur": "12.3"
        # }

        # only keep columns that need to be pushed, and rename if necessary.
        realtime_df = realtime_df[columns_to_push]
        realtime_df = realtime_df.rename(columns={"timestamp_text": "timestamp"})

        payload = realtime_df.to_json(orient="records")
        print(f"Pushing {realtime_df.timestamp.count()} rows to ODS realtime API...")
        # print(f'Pushing the following data to ODS: {json.dumps(json.loads(payload), indent=4)}')
        # use data=payload here because payload is a string. If it was an object, we'd have to use json=payload.
        r = common.requests_post(url=push_url, data=payload, verify=False)
        r.raise_for_status()


def main():
    process_river(
        river_files=RHEIN_FILES,
        river_name="Rhein",
        river_id="2289",
        variable_names={
            "abfluss": "BAFU_2289_AbflussRadar",
            "pegel": "BAFU_2289_PegelRadar",
        },
        push_url=RHEIN_ODS_PUSH_URL,
    )
    process_river(
        river_files=BIRS_FILES,
        river_name="Birs",
        river_id="2106",
        variable_names={
            "abfluss": "BAFU_2106_AbflussRadar",
            "pegel": "BAFU_2106_PegelRadar",
            "temperatur": "BAFU_2106_Wassertemperatur",
        },
        push_url=BIRS_ODS_PUSH_URL,
    )
    process_river(
        river_files=WIESE_FILES,
        river_name="Wiese",
        river_id="2199",
        variable_names={
            "abfluss": "BAFU_2199_AbflussRadarSchacht",
            "pegel": "BAFU_2199_PegelRadarSchacht",
        },
        push_url=WIESE_ODS_PUSH_URL,
    )
    process_river(
        river_files=RHEIN_KLINGENTHAL_FILES,
        river_name="Rhein_Klingenthal",
        river_id="2615",
        variable_names={"pegel": "BAFU_2615_PegelPneumatik"},
        push_url=RHEIN_KLINGENTHAL_ODS_PUSH_URL,
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f"Executing {__file__}...")
    main()
    print("Job successful!")
