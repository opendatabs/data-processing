import logging
import os
import pathlib

import pandas as pd
from dotenv import load_dotenv

import common
from common import FTP_PASS, FTP_SERVER, FTP_USER

load_dotenv()

URL = os.getenv("HTTPS_URL_TBA_WIESE")
USER = os.getenv("HTTPS_USER_TBA_WIESE")
PASS = os.getenv("HTTPS_PASS_TBA_WIESE")
ODS_PUSH_URL = os.getenv("ODS_PUSH_URL_100269")


def main():
    # Comment out to upload backup
    # upload_backup()
    r = common.requests_get(url=URL, auth=(USER, PASS))
    data = r.json()
    df = pd.DataFrame.from_dict([data])[["datum", "temperatur"]]
    df["timestamp"] = pd.to_datetime(df.datum, format="%Y-%m-%d %H:%M:%S").dt.tz_localize("Europe/Zurich")
    df["timestamp_text"] = df.timestamp.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
    df_export = df[["timestamp_text", "temperatur"]]
    common.ods_realtime_push_df(df_export, ODS_PUSH_URL)
    filename = f"{df_export.loc[0].timestamp_text.replace(':', ' - ').replace(' ', '')}.csv"
    folder = filename[:7]
    filepath = os.path.join(os.path.dirname(__file__), "data", filename)
    df_export.to_csv(filepath, index=False)
    common.ensure_ftp_dir(FTP_SERVER, FTP_USER, FTP_PASS, f"tba/wiese/temperatur/{folder}")
    common.update_ftp_and_odsp(filepath, f"tba/wiese/temperatur/{folder}", "100269")
    pass


def upload_backup():
    data_path = os.path.join(pathlib.Path(__file__).parent.absolute(), "data")
    # Iterate over month starting from january 2023 to now with while loop
    date = pd.Timestamp("2023-01-01")
    while date < pd.Timestamp.now():
        folder = date.strftime("%Y-%m")
        list_files = common.download_ftp(
            [],
            FTP_SERVER,
            FTP_USER,
            FTP_PASS,
            f"tba/wiese/temperatur/{folder}",
            data_path,
            "*.csv",
        )
        for file in list_files:
            file_path = file["local_file"]
            df = pd.read_csv(file_path)
            common.ods_realtime_push_df(df, ODS_PUSH_URL)
        date = date + pd.DateOffset(months=1)
    quit()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f"Executing {__file__}...")
    main()
    logging.info("Job successful!")
