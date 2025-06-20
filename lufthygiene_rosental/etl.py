import io
import logging
import os
import pathlib
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv

import common

load_dotenv()

FTP_SERVER = os.getenv("FTP_SERVER")
FTP_USER = os.getenv("FTP_USER_07")
FTP_PASS = os.getenv("FTP_PASS_07")
ODS_PUSH_URL = os.getenv("ODS_PUSH_URL_100275")


def main():
    url = "https://data-bs.ch/lufthygiene/Rosental-Mitte/online/airmet_bs_rosental_pm25_aktuell"
    logging.info(f"Downloading data from {url}...")
    r = common.requests_get(url)
    r.raise_for_status()
    s = r.text
    if s == "":
        logging.info("No rows to push to ODS... ")
    df = pd.read_csv(io.StringIO(s), sep=";")
    df = df.dropna(subset=["Anfangszeit"])
    df["timestamp"] = pd.to_datetime(df.Anfangszeit, format="%d.%m.%Y %H:%M:%S").dt.tz_localize(
        "Europe/Zurich", ambiguous=True, nonexistent="shift_forward"
    )

    logging.info("Melting dataframe...")
    ldf = df.melt(id_vars=["Anfangszeit", "timestamp"], var_name="station", value_name="pm_2_5")
    logging.info("Dropping rows with empty pm25 value...")
    ldf["pm_2_5"] = pd.to_numeric(ldf["pm_2_5"], errors="coerce")
    ldf = ldf.dropna(subset=["pm_2_5"])
    row_count = ldf.timestamp.count()
    if row_count == 0:
        logging.info("No rows to push to ODS... ")
    else:
        ldf.timestamp = ldf.timestamp.dt.strftime("%Y-%m-%d %H:%M:%S%z")
        filename = os.path.join(
            pathlib.Path(__file__).parent,
            "data",
            f"airmet_bs_rosental_pm25_{datetime.today().strftime('%Y-%m-%d')}.csv",
        )
        logging.info(f"Exporting data to {filename}...")
        ldf.to_csv(filename, index=False)
        ftp_dir = "Rosental-Mitte/online_backup/"
        logging.info(f"upload data to {ftp_dir}")
        common.upload_ftp(filename, FTP_SERVER, FTP_USER, FTP_PASS, ftp_dir)
        logging.info(f"Pushing {row_count} rows to ODS realtime API...")
        common.ods_realtime_push_df(ldf, ODS_PUSH_URL)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f"Executing {__file__}...")
    main()
    print("Job successful!")
