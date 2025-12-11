import io
import logging
import os

import common
import pandas as pd
from common import change_tracking as ct
from common import email_message


def main():
    # Process Abfuhrtermine from 2017 to 2019 which is available in xlsx format
    # abfuhrtermine_2017_2019()
    # Process Abfuhrtermine from 2020 to 2023 which is available in csv format
    # abfuhrtermine_2020_2023()
    # Process Abfuhrtermine from 2024 onwards which is available in csv format
    future_abfuhrtermine()


def future_abfuhrtermine():
    file = os.path.join("data_orig", "AF", "Abfuhrtermine", "Abfuhrtermine.csv")
    logging.info(f"Processing file {file}...")
    df = pd.read_csv(file, sep=";", encoding="cp1252", index_col=False)
    df = df.rename(
        columns={
            "Art": "art",
            "Zone": "zone",
            "Termin": "termin",
            "Wochentag": "wochentag",
        }
    )
    
    df = df[df["Feiertage"].isna() | (df["Feiertage"] == "")]
    df = df.drop(columns=["Feiertage"])
    df["wochentag"] = df["wochentag"].str.capitalize()
    df["termin"] = df["termin"].str.strip()
    df["termin"] = pd.to_datetime(df["termin"], format="%d.%m.%Y")
    # Read the max year from the column 'termin'
    max_year = df["termin"].dt.year.max()
    # Filter the data for the max year
    df = df[df["termin"].dt.year == max_year]
    df["dayofweek"] = df["termin"].dt.dayofweek
    df["termin"] = df["termin"].dt.strftime("%d.%m.%Y")
    df = df.merge(download_abfuhrzonen(), on="zone", how="left")
    path_export = os.path.join("data", f"Abfuhrtermine_{max_year}.csv")
    df.to_csv(path_export, index=False, sep=";", encoding="utf-8")
    if ct.has_changed(path_export):
        common.update_ftp_and_odsp(path_export, "tba/abfuhrtermine", "100096")
        text = f"New Abfuhrtermine (dataset 100096) available for the year {max_year}.\n"
        text += (
            f"The new data can be found here: https://data-bs.ch/stata/tba/abfuhrtermine/Abfuhrtermine_{max_year}.csv\n"
        )
        text += "Kind regards, \nYour automated Open Data Basel-Stadt Python Job"
        msg = email_message(subject=f"Abfuhrtermine {max_year}", text=text, img=None, attachment=None)
        common.send_email(msg)
        ct.update_hash_file(path_export)


def abfuhrtermine_2020_2023():
    csv_path = os.path.join("data_orig", "csv")
    for csv_file in os.listdir(csv_path):
        logging.info(f"Processing file {csv_file}...")
        df = pd.read_csv(
            os.path.join(csv_path, csv_file),
            sep=";",
            encoding="cp1252",
            index_col=False,
        )
        df = append_columns(df)
        year = csv_file.split("_")[1].split(".")[0]
        path_export = os.path.join("data", f"Abfuhrtermine_{year}.csv")
        df.to_csv(path_export, index=False, sep=";", encoding="utf-8")
        common.upload_ftp(path_export, remote_path="tba/abfuhrtermine")


def abfuhrtermine_2017_2019():
    xlsx_path = os.path.join("data_orig", "xlsx")
    for xlsx_file in os.listdir(xlsx_path):
        logging.info(f"Processing file {xlsx_file}...")
        df = pd.read_excel(os.path.join(xlsx_path, xlsx_file), usecols="A:E")
        df = df.rename(
            columns={
                "Fraktion": "art",
                "ABFUHR_ZONE": "zone",
                "TERMIN": "termin",
                "WOCHENTAG": "wochentag",
            }
        )
        df = df.drop(columns=["Feiertage"])
        df["wochentag"] = df["wochentag"].str.capitalize()
        df = append_columns(df)
        year = xlsx_file.split(" ")[1].split(".")[0]
        path_export = os.path.join("data", f"Abfuhrtermine_{year}.csv")
        df.to_csv(path_export, index=False, sep=";", encoding="utf-8")
        common.upload_ftp(path_export, remote_path="tba/abfuhrtermine")


def append_columns(df):
    df["termin"] = pd.to_datetime(df["termin"], format="%d.%m.%Y")
    df["dayofweek"] = df["termin"].dt.dayofweek
    df["termin"] = df["termin"].dt.strftime("%d.%m.%Y")
    return df.merge(download_abfuhrzonen(), on="zone", how="left")


def download_abfuhrzonen():
    url_to_shp = "https://data.bs.ch/explore/dataset/100095/download/?format=csv&timezone=Europe/Zurich&lang=de"
    r = common.requests_get(url_to_shp)
    return pd.read_csv(io.StringIO(r.content.decode("utf-8")), sep=";")[["zone", "geo_shape", "geo_point_2d"]]


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f"Executing {__file__}...")
    main()
    logging.info("Job successful!")
