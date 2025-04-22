import glob
import logging
import os
from datetime import datetime

import common
import pandas as pd
from common import ODS_API_KEY
from common import change_tracking as ct


def get_date_latest_file():
    latest_date = datetime.strptime("27112022", "%d%m%Y")
    pattern = "Stadtlast_????????.xlsx"
    file_list = glob.glob(os.path.join("data", "latest_data", pattern))
    for file in file_list:
        datetime_file = os.path.basename(file).split("_", 1)[1][:8]
        datetime_file = datetime.strptime(datetime_file, "%d%m%Y")
        if datetime_file > latest_date:
            latest_date = datetime_file
    return latest_date.date()


def get_path_latest_file():
    date = get_date_latest_file()
    date_str = date.strftime("%d%m%Y")
    path = "Stadtlast_" + date_str + ".xlsx"
    return path


def get_path_realtime_push_file():
    raw_data_file = os.path.join("data", "realtime_push_data.csv")
    logging.info(
        f"Downloading raw realtime push data from ods to file {raw_data_file}..."
    )
    r = common.requests_get(ODS_API_KEY)
    # If content just contains the header, do not write to file
    newline_count = r.text.count("\n")
    if newline_count > 1:
        logging.info(f"Writing raw realtime push data to file {raw_data_file}...")
        with open(raw_data_file, "wb") as f:
            f.write(r.content)
        # Unpublish and publish to empty dataset
        # This has to be done in case the data owner corrects data-points in the dataset
        logging.info("Unpublishing and publishing dataset 100237 to empty dataset...")
        common.publish_ods_dataset_by_id("100237", unpublish_first=True)
    else:
        logging.info("No data in realtime push dataset 100237. Not writing to file...")
    return raw_data_file


def create_timestamp(df):
    return (
        df["Ab-Datum"].dt.to_period("d").astype(str) + "T" + df["Ab-Zeit"].astype(str)
    )


def create_timestamp_realtime_push(df):
    return (
        df["0calday"].str[:4]
        + "-"
        + df["0calday"].str[4:6]
        + "-"
        + df["0calday"].str[6:]
        + "T"
        + df["0time"].str[:2]
        + ":"
        + df["0time"].str[2:4]
        + ":"
        + df["0time"].str[4:]
    )


def create_time_fields(df):
    df["timestamp_interval_start"] = pd.to_datetime(
        df["timestamp_interval_start_raw_text"], format="%Y-%m-%dT%H:%M:%S"
    ).dt.tz_localize("Europe/Zurich", ambiguous=True)
    df["timestamp_interval_start_text"] = df["timestamp_interval_start"].dt.strftime(
        "%Y-%m-%dT%H:%M:%S%z"
    )
    df["year"] = df["timestamp_interval_start"].dt.year
    df["month"] = df["timestamp_interval_start"].dt.month
    df["day"] = df["timestamp_interval_start"].dt.day
    df["weekday"] = df["timestamp_interval_start"].dt.weekday
    df["dayofyear"] = df["timestamp_interval_start"].dt.dayofyear
    df["quarter"] = df["timestamp_interval_start"].dt.quarter
    df["weekofyear"] = df["timestamp_interval_start"].dt.isocalendar().week
    return df


def create_export_df_from_realtime_push():
    realtime_push_file = get_path_realtime_push_file()
    export_filename = os.path.join(
        os.path.dirname(__file__), "data/export", "netzlast_realtime_push.csv"
    )
    if ct.has_changed(realtime_push_file):
        logging.info("Processing realtime push file...")
        df_push = pd.read_csv(
            realtime_push_file,
            sep=";",
            dtype={
                "0calday": str,
                "0time": str,
                "0uc_profval": float,
                "0uc_profile_t": str,
            },
        )
        df_push["timestamp_interval_start_raw_text"] = create_timestamp_realtime_push(
            df_push
        )
        df_push.drop(columns=["0calday", "0time"], inplace=True)

        grouped = df_push.groupby("0uc_profile_t")
        dfs = {}
        for group_name, group_df in grouped:
            dfs[group_name] = create_time_fields(
                group_df.sort_values(by=["timestamp_interval_start_raw_text"])
            )
        df_stadtlast = (
            dfs["Stadtlast"]
            .drop(columns=["0uc_profile_t"])
            .rename(columns={"0uc_profval": "stromverbrauch_kwh"})
        )
        df_private = (
            dfs["Grundversorgung"]
            .drop(columns=["0uc_profile_t"])
            .rename(columns={"0uc_profval": "grundversorgte_kunden_kwh"})
        )
        df_free = (
            dfs["Freie Kunden"]
            .drop(columns=["0uc_profile_t"])
            .rename(columns={"0uc_profval": "freie_kunden_kwh"})
        )

        df_push = df_stadtlast.merge(
            df_private[["timestamp_interval_start", "grundversorgte_kunden_kwh"]],
            how="left",
            on="timestamp_interval_start",
        )
        df_push = df_push.merge(
            df_free[["timestamp_interval_start", "freie_kunden_kwh"]],
            how="left",
            on="timestamp_interval_start",
        )
        df_export = df_push[
            [
                "timestamp_interval_start",
                "stromverbrauch_kwh",
                "grundversorgte_kunden_kwh",
                "freie_kunden_kwh",
                "timestamp_interval_start_text",
                "year",
                "month",
                "day",
                "weekday",
                "dayofyear",
                "quarter",
                "weekofyear",
            ]
        ]
        df_export.to_csv(export_filename, index=False, sep=";")
        if ct.has_changed(export_filename):
            common.upload_ftp(export_filename, remote_path="iwb/netzlast")
            ct.update_hash_file(export_filename)
        ct.update_hash_file(realtime_push_file)
        return df_export
    else:
        logging.info("Getting realtime push file from memory...")
        return pd.read_csv(export_filename, sep=";")


def create_export_df_from_ftp():
    LATEST_DATA_FILE = get_path_latest_file()
    file_path_latest = os.path.join("data", "latest_data", LATEST_DATA_FILE)
    export_filename = os.path.join("data", "export", "netzlast_ftp.csv")
    logging.info("Check if latest data file has changed...")
    if ct.has_changed(file_path_latest, method="modification_date"):
        logging.info("Processing historical data 2012-2020...")
        hist_dfs = []
        for year in range(2012, 2021):
            logging.info(f"Processing year {year}...")
            file_hist = os.path.join(
                "data",
                "historical_data",
                f"Stadtlast_IDS_{year}.xls",
            )
            df_hist = pd.read_excel(file_hist, skiprows=22, usecols="B,E", sheet_name=0)
            hist_dfs.append(df_hist)
        df_history = pd.concat(hist_dfs).rename(
            columns={"Zeitstempel": "timestamp", "Werte (kWh)": "stromverbrauch_kwh"}
        )
        # In these files, timestamps are at the end of a 15-minute interval. Subtract 15 minutes to match to newer files.
        # df_history['timestamp_start'] = df_history['timestamp'] + pd.Timedelta(minutes=-15)
        # Export as string to match newer files
        df_history["timestamp"] = df_history["timestamp"].dt.strftime(
            "%Y-%m-%dT%H:%M:%S"
        )

        logging.info("Processing 2020-07-01 until 2020-08-31...")
        hist2 = os.path.join("data", "latest_data", "Stadtlast_2020.xlsx")
        df_history2 = pd.read_excel(hist2, sheet_name="Tabelle1")
        df_history2["timestamp"] = create_timestamp(df_history2)
        df_history2 = df_history2[["timestamp", "Profilwert"]].rename(
            columns={"Profilwert": "stromverbrauch_kwh"}
        )
        df_history2 = df_history2.query('timestamp < "2020-09-01"')

        logging.info(
            "Processing 2020 (starting 2020-09.01), 2021, 2022 (until 2022-09-30), and 2022 (starting 2022-10-01)..."
        )
        market_files = [
            "Stadtlast_2020_market.xlsx",
            "Stadtlast_2021_market.xlsx",
            "Stadtlast_2022_market.xlsx",
            LATEST_DATA_FILE,
        ]
        new_dfs = []
        for file in market_files:
            logging.info(f"Processing {file}...")
            file_2 = os.path.join("data", "latest_data", file)
            df2 = pd.read_excel(file_2, sheet_name="Stadtlast")
            df2["timestamp"] = create_timestamp(df2)
            df_update = df2[["timestamp", "Stadtlast"]].rename(
                columns={"Stadtlast": "stromverbrauch_kwh"}
            )
            new_dfs.append(df_update)
        latest_dfs = pd.concat(new_dfs)
        logging.info('Processing "Freie Kunden" and "Grundversorgte Kunden"...')
        free_dfs = []
        base_dfs = []
        for file in market_files:
            logging.info(f"Processing frei/grundversorgt in file {file}...")
            market_file = os.path.join("data", "latest_data", file)
            market_sheets = pd.read_excel(market_file, sheet_name=None)
            if (
                "Freie Kunden" in market_sheets
                and "Grundversorgte Kunden" in market_sheets
            ):
                free_dfs.append(market_sheets["Freie Kunden"])
                base_dfs.append(market_sheets["Grundversorgte Kunden"])
        df_free = pd.concat(free_dfs)
        df_free["timestamp_interval_start_raw_text"] = create_timestamp(df_free)
        df_free["timestamp_interval_start"] = pd.to_datetime(
            df_free.timestamp_interval_start_raw_text, format="%Y-%m-%dT%H:%M:%S"
        ).dt.tz_localize("Europe/Zurich", ambiguous="infer")
        df_free = df_free.rename(columns={"Freie Kunden": "freie_kunden_kwh"})[
            [
                "timestamp_interval_start",
                "timestamp_interval_start_raw_text",
                "freie_kunden_kwh",
            ]
        ]
        df_private = pd.concat(base_dfs)
        df_private["timestamp_interval_start_raw_text"] = create_timestamp(df_private)
        df_private["timestamp_interval_start"] = pd.to_datetime(
            df_private.timestamp_interval_start_raw_text, format="%Y-%m-%dT%H:%M:%S"
        ).dt.tz_localize("Europe/Zurich", ambiguous="infer")
        df_private = df_private.rename(
            columns={"Grundversorgte Kunden": "grundversorgte_kunden_kwh"}
        )[
            [
                "timestamp_interval_start",
                "timestamp_interval_start_raw_text",
                "grundversorgte_kunden_kwh",
            ]
        ]

        df_export = (
            pd.concat([df_history, df_history2, latest_dfs])
            .dropna(subset=["stromverbrauch_kwh"])
            .reset_index(drop=True)
            .rename(columns={"timestamp": "timestamp_interval_start_raw_text"})
        )
        df_export = create_time_fields(df_export)

        df_export = df_export.merge(df_free, how="left", on="timestamp_interval_start")
        df_export = df_export.merge(
            df_private, how="left", on="timestamp_interval_start"
        )

        df_export = df_export[
            [
                "timestamp_interval_start",
                "stromverbrauch_kwh",
                "grundversorgte_kunden_kwh",
                "freie_kunden_kwh",
                "timestamp_interval_start_text",
                "year",
                "month",
                "day",
                "weekday",
                "dayofyear",
                "quarter",
                "weekofyear",
            ]
        ]
        df_export.to_csv(export_filename, index=False, sep=";")
        if ct.has_changed(export_filename):
            common.upload_ftp(export_filename, remote_path="iwb/netzlast")
            ct.update_hash_file(export_filename)
        ct.update_mod_timestamp_file(file_path_latest)
        return df_export
    else:
        logging.info("Getting ftp files from memory...")
        return pd.read_csv(export_filename, sep=";")


def main():
    df_export_rp = create_export_df_from_realtime_push()
    df_export_ftp = create_export_df_from_ftp()
    df_export = pd.concat(
        [df_export_rp, df_export_ftp], ignore_index=True
    ).drop_duplicates("timestamp_interval_start", keep="first")
    export_filename = os.path.join(
        os.path.dirname(__file__), "data/export", "netzlast.csv"
    )
    df_export.to_csv(export_filename, index=False, sep=";")
    common.update_ftp_and_odsp(export_filename, "iwb/netzlast", "100233")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f"Executing {__file__}...")
    main()
    logging.info("Job successful!")
