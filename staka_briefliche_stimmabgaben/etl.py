import glob
import logging
import os
from datetime import datetime

import common
import pandas as pd
from common import change_tracking as ct
from common import email_message

ODS_PUSH_URL = os.getenv("ODS_PUSH_URL_100223")


def main():
    logging.info("get data from 2020")
    df_2020 = get_data_2020()
    logging.info("get newest data starting from 07-03-2021")
    df_publ = get_previous_data_from_20210307()
    df_publ = pd.concat([df_publ, df_2020], ignore_index=True)
    logging.info("get file and date of latest available file")
    latest_file, datetime_urnengang = get_latest_file_and_date()
    date_urnengang = str(datetime_urnengang.date())
    logging.info(f"date of latest Urnengang is {date_urnengang}")
    # to do: check if this is the date of currently active Urnengang...
    dates = [str(x.date()) for x in df_publ["datum_urnengang"]]
    logging.info("check if data from latest Urnengang is already in the df, if not add it")
    if date_urnengang not in dates:
        logging.info(f"Add data of currently active Urnengang of {date_urnengang}")
        df_latest = make_df_for_publ(latest_file=latest_file, datetime_urnengang=datetime_urnengang)
        df_publ = pd.concat([df_latest, df_publ], ignore_index=True)
    # add 'tage_bis_urnengang'
    df_publ["tage_bis_urnengang"] = df_publ["datum_urnengang"] - df_publ["datum"]
    df_publ["tage_bis_urnengang"] = [x.days for x in df_publ["tage_bis_urnengang"]]
    # make date columns of string type
    df_publ["datum"] = df_publ["datum"].dt.strftime("%Y-%m-%d")
    df_publ["datum_urnengang"] = df_publ["datum_urnengang"].dt.strftime("%Y-%m-%d")
    # check if Abstimmung/Wahlen
    df_wahlen = pd.read_csv(os.path.join("data/Basel_briefliche_stimmabgaben/Termine/wahlen.csv"))
    df_abst = pd.read_csv(os.path.join("data/Basel_briefliche_stimmabgaben/Termine/abstimmungen.csv"))
    df_publ["abstimmungen"] = [
        "Ja" if date in df_abst["Abstimmungstermin"].values else "Nein" for date in df_publ["datum_urnengang"]
    ]
    df_publ["wahlen"] = ["Ja" if date in df_wahlen["Datum"].values else "Nein" for date in df_publ["datum_urnengang"]]
    df_publ["wahlen_typ"] = [
        df_wahlen.loc[df_wahlen["Datum"] == row["datum_urnengang"], "Typ"].item() if row["wahlen"] == "Ja" else ""
        for index, row in df_publ.iterrows()
    ]
    # remove rows for which datum_urnengang is not listed, send email
    logging.info("check if all values in the column datum_urnengang are found")
    dates_not_listed = df_publ[df_publ["abstimmungen"] == "Nein"][df_publ["wahlen"] == "Nein"]["datum_urnengang"]
    dates_not_listed = list(dates_not_listed.unique())
    df_publ = df_publ[(df_publ["abstimmungen"] != "Nein") | (df_publ["wahlen"] != "Nein")]
    if dates_not_listed != []:
        logging.info(f"The dates {dates_not_listed} are not listed, send email.")
        text = (
            f"The following dates were not found in either wahlen.csv or abstimmungen.csv:{dates_not_listed}.\n"
            f"Rows with datum_urnengang in {dates_not_listed} have therefore not been pushed. \n\n"
            f"Kind regards, \nYour automated Open Data Basel-Stadt Python Job"
        )
        msg = email_message(
            subject="Warning Briefliche Stimmabgaben",
            text=text,
            img=None,
            attachment=None,
        )
        common.send_email(msg)
    # upload csv files
    path_export_file_publ = os.path.join("data", "export", "100223.csv")
    logging.info(f"upload csv file to {path_export_file_publ}")
    df_publ.to_csv(path_export_file_publ, index=False)
    # push df_publ
    if ct.has_changed(path_export_file_publ):
        ct.update_hash_file(path_export_file_publ)
        logging.info("push data to ODS realtime API")
        logging.info("push for dataset 100223")
        common.ods_realtime_push_df(df_publ, ODS_PUSH_URL)


def get_data_2020():
    pattern = "2020_Eingang_Stimmabgaben_Basel*_2020.xlsx"
    file_list = glob.glob(os.path.join("data/Basel_briefliche_stimmabgaben/2020", pattern))
    df_all = pd.DataFrame()
    for file in file_list:
        tabs = pd.ExcelFile(file).sheet_names
        datetime_urnengang = tabs[0].split("_", 1)[0]
        datetime_urnengang = datetime.strptime(datetime_urnengang, "%d.%m.%Y")
        df = make_df_for_publ(latest_file=file, datetime_urnengang=datetime_urnengang)
        df_all = pd.concat([df_all, df], ignore_index=True)
    return df_all


def get_previous_data_from_20210307():
    pattern = "????????_Eingang_Stimmabgaben*morgen.xlsx"
    file_list = glob.glob(os.path.join("data", "Basel_briefliche_stimmabgaben", pattern))
    df_all = pd.DataFrame()
    for file in file_list:
        datetime_urnengang = os.path.basename(file).split("_", 1)[0]
        datetime_urnengang = datetime.strptime(datetime_urnengang, "%Y%m%d")
        df = make_df_for_publ(latest_file=file, datetime_urnengang=datetime_urnengang)
        df_all = pd.concat([df_all, df], ignore_index=True)
    return df_all


def get_latest_file_and_date():
    pattern = "????????_Eingang_Stimmabgaben*.xlsx"
    data_file_names = []
    file_list = glob.glob(os.path.join("data", "Basel_briefliche_stimmabgaben", pattern))
    if len(file_list) > 0:
        latest_file = max(file_list, key=os.path.getmtime)
        data_file_names.append(os.path.basename(latest_file))
    datetime_urnengang = data_file_names[0].split("_", 1)[0]
    datetime_urnengang = datetime.strptime(datetime_urnengang, "%Y%m%d")
    return latest_file, datetime_urnengang


def make_df_for_publ(latest_file, datetime_urnengang):
    columns = [
        "tag",
        "datum",
        "eingang_pro_tag",
        "eingang_kumuliert",
        "stimmbeteiligung",
    ]
    df_stimmabgaben = pd.read_excel(latest_file, sheet_name=0, header=None, names=columns, skiprows=6)
    df_stimmabgaben["stimmbeteiligung"] = 100 * df_stimmabgaben["stimmbeteiligung"]
    # add column datum_urnengang
    df_stimmabgaben["datum_urnengang"] = datetime_urnengang
    # remove empty rows
    df_stimmabgaben = df_stimmabgaben.dropna()
    return df_stimmabgaben


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f"Executing {__file__}...")
    main()
