import datetime
import logging
import os

import common
import pandas as pd
import pytz
from common import FTP_SERVER, FTP_USER, FTP_PASS
from common import change_tracking as ct


def main():
    df_calendar = pd.read_excel(
        os.path.join("data_orig", "RIK Kalender.xlsx"),
        sheet_name="Daten LIK",
        skiprows=2,
    )
    df_embargo = df_calendar[df_calendar["EMBARGO"].notnull()]["EMBARGO"]
    df_embargo = pd.to_datetime(df_embargo, format="%Y-%m-%d %H:%M:%S")
    df_embargo = df_embargo.apply(lambda x: x.replace(hour=8, minute=30))
    df_embargo = df_embargo.dt.tz_localize("Europe/Zurich")
    if df_embargo[
        (df_embargo.dt.month == datetime.datetime.now().month)
        & (df_embargo.dt.year == datetime.datetime.now().year)
    ].empty:
        raise ValueError(
            "No embargo date found for this month and year. Please add it to the calendar."
        )
    embargo = df_embargo[
        (df_embargo.dt.month == datetime.datetime.now().month)
        & (df_embargo.dt.year == datetime.datetime.now().year)
    ].iloc[0]
    current_time = datetime.datetime.now(tz=datetime.timezone.utc).astimezone(
        pytz.timezone("Europe/Zurich")
    )
    if embargo > current_time:
        logging.info("Embargo is not over yet.")
        return
    logging.info("Embargo is over in this month")
    common.download_ftp(
        [], FTP_SERVER, FTP_USER, FTP_PASS, "bik", "data", "bik_full.csv"
    )
    path_import = os.path.join("data", "bik_full.csv")
    if not ct.has_changed(path_import):
        logging.info("No changes in the data.")
        return

    logging.info("Changes in the data. Publishing on ODS")
    common.publish_ods_dataset_by_id("100003")
    ct.update_hash_file(path_import)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f"Executing {__file__}...")
    main()
    logging.info("Job successful!")
