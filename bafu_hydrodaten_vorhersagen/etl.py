import json
import logging
import os
import re
from datetime import datetime, timedelta

import common
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from pytz import timezone
from requests.auth import HTTPBasicAuth

load_dotenv()

DICT_URL = json.loads(os.getenv("DICT_URL_BAFU_VORHERSAGEN"))
HTTPS_USER = os.getenv("HTTPS_USER_01")
HTTPS_PASS = os.getenv("HTTPS_PASS_01")

RIVERS = ["Rhein", "Birs"]
METHODS = ["ICON-CH1-EPS", "ICON-CH2-EPS", "IFS"]
DICT_ID = {"Rhein": "100271", "Birs": "100272"}


def main():
    for river in RIVERS:
        logging.info(f"process data for {river}")
        df = pd.DataFrame()
        for method in METHODS:
            logging.info(f"process data for {method}")
            df_method = extract_data(river, method)
            df_method["timestamp"] = (
                df_method["dd"].astype(str)
                + "."
                + df_method["mm"].astype(str)
                + "."
                + df_method["yyyy"].astype(str)
                + " "
                + df_method["hh"].astype(str)
            )
            #  manually specify which datetime objects must be considered DT
            # (https://stackoverflow.com/questions/36757981/python-pandas-tz-localize-ambiguoustimeerror-cannot-infer-dst-time-with-non-d)
            infer_dst = np.array([False] * df_method.shape[0])  # all False -> every row considered DT
            df_method["timestamp"] = pd.to_datetime(df_method.timestamp, format="%d.%m.%Y %H").dt.tz_localize(
                "Europe/Zurich", nonexistent="shift_forward", ambiguous=infer_dst
            )
            duplicate_index = [idx for idx, value in enumerate(df_method.timestamp.duplicated(keep="last")) if value]
            if duplicate_index:
                df_method["timestamp"] = [
                    correct_dst_timezone(x) if idx != duplicate_index[0] else x
                    for idx, x in enumerate(df_method["timestamp"])
                ]
            else:
                df_method["timestamp"] = [correct_dst_timezone(x) for x in df_method["timestamp"]]
            df = pd.concat([df, df_method])
            df = df.reset_index(drop=True)
        logging.info("add timestamp with daylight saving time if needed")
        for column in ["hh", "dd", "mm"]:
            df[column] = [x if len(x) == 2 else ("0" + x) for x in df[column].astype(str)]
        # Alle Zeitstempel sind immer in Winterzeit (UTC+1)

        logging.info('remove measured data and add once with method "gemessen"')
        df = take_out_measured_data(df)
        df = df.reset_index(drop=True)
        logging.info("define df_export and uplad to ftp")
        df_export = df[
            [
                "timestamp",
                "Wasserstand",
                "Abfluss",
                "methode",
                "ausgegeben_an",
                "meteolauf",
                "gemessene_werten_bis",
                "H_min",
                "H_p25",
                "H_p50",
                "H_p75",
                "H_max",
                "Q_min",
                "Q_p25",
                "Q_p50",
                "Q_p75",
                "Q_max",
            ]
        ]
        export_filename = os.path.join(
            os.path.dirname(__file__),
            "data/vorhersagen/export",
            f"{river}_Vorhersagen.csv",
        )
        df_export.to_csv(export_filename, index=False, sep=";")
        common.update_ftp_and_odsp(export_filename, "hydrodata.ch/data/vorhersagen", DICT_ID[river])

def get_date_time(line):
    match = re.search(r"\d{1,2}\.\d{1,2}\.\d{4}, \d{2}\.\d{2}", line)
    if not match:
        return None

    dt = datetime.strptime(match.group(), "%d.%m.%Y, %H.%M")
    dt = dt.replace(tzinfo=timezone("Europe/Zurich"))
    return correct_dst_timezone(dt)


def correct_dst_timezone(timestamp):
    if timestamp.dst() == timedelta(hours=1):
        timestamp = timestamp + timedelta(hours=1)
    else:
        pass
    return timestamp


def extract_data(river, method):
    url = DICT_URL[river][method]
    req = common.requests_get(url, auth=HTTPBasicAuth(HTTPS_USER, HTTPS_PASS))
    lines = req.content.splitlines()
    ausgabe_info = str(lines[6])
    ausgabe = get_date_time(ausgabe_info)
    meteolauf_info = str(lines[7])
    meteolauf = get_date_time(meteolauf_info)
    gemessen_info = str(lines[8])
    gemessen = get_date_time(gemessen_info)
    curr_dir = os.path.dirname(os.path.realpath(__file__))
    path_to_file = f"{curr_dir}/data/vorhersagen/latest_data/det_{method}_{river}_table.txt"
    with open(path_to_file, mode="wb") as file:
        for line in lines[14::]:
            file.write(line)
            file.write(b"\n")
    df = pd.read_table(path_to_file, sep="\s+")
    df["methode"] = method
    df["ausgegeben_an"] = ausgabe
    df["meteolauf"] = meteolauf
    df["gemessene_werten_bis"] = gemessen
    if "ICON" in method:
        df_quant = get_quantiles(river, method, url)
        df = pd.merge(
            df,
            df_quant[
                [
                    "dd",
                    "mm",
                    "yyyy",
                    "hh",
                    "H_min",
                    "H_p25",
                    "H_p50",
                    "H_p75",
                    "H_max",
                    "Q_min",
                    "Q_p25",
                    "Q_p50",
                    "Q_p75",
                    "Q_max",
                ]
            ],
            on=["dd", "mm", "yyyy", "hh"],
            how="left",
        )
    return df


def get_quantiles(river, method, url):
    url = url.replace("_Ctrl", "")
    req = common.requests_get(url, auth=HTTPBasicAuth(HTTPS_USER, HTTPS_PASS))
    lines = req.content.splitlines()
    curr_dir = os.path.dirname(os.path.realpath(__file__))
    path_to_file = f"{curr_dir}/data/vorhersagen/latest_data/quant_{method}_{river}_table.txt"
    with open(path_to_file, mode="wb") as file:
        for line in lines[14::]:
            file.write(line)
            file.write(b"\n")
    return pd.read_table(path_to_file, sep="\s+")


def take_out_measured_data(df):
    for idx in df.index:
        if df["timestamp"][idx] <= df["gemessene_werten_bis"][idx]:
            df.loc[idx, "methode"] = "gemessen"
        df = df.drop_duplicates(subset=["methode", "timestamp"])
    return df


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f"Executing {__file__}...")
    main()
    logging.info("Job successful!")
