import logging
import os

import common
import pandas as pd
from common import FTP_PASS, FTP_SERVER, FTP_USER

ODS_PUSH_URL = os.getenv("ODS_PUSH_URL_100269")


def main():
    data_path = ".\data\\temperatur_wrong_format"
    # Iterate over all files in the folder
    df = pd.DataFrame()
    for file in os.listdir(data_path):
        df = pd.read_csv(os.path.join(data_path, file))
        df["timestamp_text"] = pd.to_datetime(
            df.timestamp_text, format="%Y-%d-%mT%H:%M:%S%z"
        ).dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        common.ods_realtime_push_df(df, ODS_PUSH_URL)
        filename = (
            f"{df.loc[0].timestamp_text.replace(':', ' - ').replace(' ', '')}.csv"
        )
        folder = filename[:7]
        filepath = os.path.join(os.path.dirname(__file__), "data", filename)
        df.to_csv(filepath, index=False)
        common.ensure_ftp_dir(
            FTP_SERVER,
            FTP_USER,
            FTP_PASS,
            f"tba/wiese/temperatur/{folder}",
        )
        common.upload_ftp(
            filepath,
            remote_path=f"tba/wiese/temperatur/{folder}",
        )
    pass


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f"Executing {__file__}...")
    main()
    logging.info("Job successful!")
