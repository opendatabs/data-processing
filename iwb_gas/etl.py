import datetime
import logging
import os

import pandas as pd
from dotenv import load_dotenv

import common

load_dotenv()

FTP_SERVER = os.getenv("FTP_SERVER")
FTP_USER = os.getenv("FTP_USER_04")
FTP_PASS = os.getenv("FTP_PASS_04")


def main():
    path_def = os.path.join("data", "gas", "def")
    list_files = common.download_ftp([], FTP_SERVER, FTP_USER, FTP_PASS, "gas", path_def, "*_DEF_????????.csv")
    # Add data from the "raw files" for the dates which are not yet in a monthly file
    # Take every RAW file from the current and the last month and drop duplicate lines later
    today = datetime.date.today()
    this_month = today.strftime("%Y%m")
    list_files += common.download_ftp([], FTP_SERVER, FTP_USER, FTP_PASS, "gas", path_def, f"*_RAW_{this_month}??.csv")
    first = today.replace(day=1)
    last_month = (first - datetime.timedelta(days=1)).strftime("%Y%m")
    list_files += common.download_ftp([], FTP_SERVER, FTP_USER, FTP_PASS, "gas", path_def, f"*_RAW_{last_month}??.csv")
    df = pd.DataFrame()
    for file in list_files:
        path = file["local_file"]
        df_file = pd.read_csv(path, skiprows=5, sep=";")
        df = pd.concat([df, df_file], ignore_index=True)
    df["Date"] = pd.to_datetime(df["Date"], format="%d.%m.%Y")
    # to do: fix timezone
    df["Timestamp"] = df["Date"].astype(str) + " " + df["Time"].astype(str)
    df = df.drop_duplicates(subset=["Timestamp"])
    df["year"] = df["Date"].dt.year
    df["month"] = df["Date"].dt.month
    df["day"] = df["Date"].dt.day
    df["weekday"] = df["Date"].dt.weekday
    df["dayofyear"] = df["Date"].dt.dayofyear
    df["quarter"] = df["Date"].dt.quarter
    df["weekofyear"] = df["Date"].dt.isocalendar().week
    path_export = os.path.join("data", "export", "100304.csv")
    df.to_csv(path_export, index=False)
    common.update_ftp_and_odsp(path_export, "iwb/gas", "100304")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f"Executing {__file__}...")
    main()
    logging.info("Job successful!")
