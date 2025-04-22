import logging
import os
from datetime import datetime

import common
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

FTP_SERVER = os.getenv("FTP_SERVER")
FTP_USER = os.getenv("FTP_USER_06")
FTP_PASS = os.getenv("FTP_PASS_06")


def main():
    df_old = process_old_data()
    make_backup()
    df_all = process_data_from_04_2023(df_old)
    export_file_name = os.path.join("data", "EAP_daily_data.csv")
    logging.info(f"Exporting to {export_file_name}...")
    df_all.to_csv(export_file_name, index=False)
    common.upload_ftp(export_file_name, FTP_SERVER, FTP_USER, FTP_PASS, "")
    logging.info("Job successful!")


def make_backup():
    d = datetime.now()
    year = d.year
    month = f"{d:%m}"
    from_name = "eap_data.xlsx"
    to_name = f"{year}_{month}_backup_eap_data.xlsx"
    common.download_ftp([from_name], FTP_SERVER, FTP_USER, FTP_PASS, "", "data", "")
    import_file_name = os.path.join("data", from_name)
    export_file_name = os.path.join("data", to_name)
    os.rename(import_file_name, export_file_name)
    common.upload_ftp(export_file_name, FTP_SERVER, FTP_USER, FTP_PASS, "")


def get_file(file):
    logging.info(f"Downloading {file} from FTP server...")
    common.download_ftp([file], FTP_SERVER, FTP_USER, FTP_PASS, "", "data", "")
    import_file_name = os.path.join("data", file)
    logging.info(f"Reading dataset from {import_file_name}...")
    df = pd.read_excel(import_file_name, index_col=None)
    return df


def list_files_from_04_2023():
    data_list = common.download_ftp(
        [], FTP_SERVER, FTP_USER, FTP_PASS, "", "data", "*backup_eap_data.xlsx", True
    )
    filenames = [data["remote_file"] for data in data_list]
    return filenames


def process_data_from_04_2023(df_old):
    files = list_files_from_04_2023()
    files.sort()
    df_all = df_old
    for file in files:
        logging.info(f"make df from {file}")
        df = get_file(file)
        df = transform_df(df)
        logging.info(f"check what is oldest date in file {file}")
        oldest_date = df["date"].min()
        logging.info(f"in df_all remove all entries after oldest date {oldest_date}")
        df_all = df_all[df_all["date"] < oldest_date]
        logging.info(f"concat the entries from {oldest_date} in {file} to df_all")
        df_all = pd.concat([df_all, df], ignore_index=True)
    return df_all


def process_old_data():
    # 'eap_data_01.01.2019_29.05.2020.xlsx': complete, want to read out until 31.12.2019
    df1 = get_file("eap_data_01.01.2019_29.05.2020.xlsx")
    df1 = transform_df(df1)
    df1 = df1[df1["date"] <= pd.Timestamp("2019-12-31")]
    # 'eap_data_01.01.2019_09.09.2022.xlsx': has missing data in 2019, read out from 01.01.2020 (until 30.11.2021)
    df2 = get_file("eap_data_01.01.2019_09.09.2022.xlsx")
    df2 = transform_df(df2)
    df2 = df2[df2["date"] >= pd.Timestamp("2020-01-01")]
    df2 = df2[df2["date"] <= pd.Timestamp("2021-11-30")]
    # 'eap_data_01.12.2021_31.12.2022.xlsx': read out from 01.12.2021
    df3 = get_file("eap_data_01.12.2021_31.12.2022.xlsx")
    df3 = transform_df(df3)
    df = pd.concat([df1, df2, df3], ignore_index=True)
    return df


def transform_df(df):
    logging.info("Create date column as a first column, then drop d, m, y columns...")
    df["date"] = pd.to_datetime(
        df.Annee * 10000 + df.Mois * 100 + df.Jour, format="%Y%m%d"
    )
    df.insert(0, "date", df.pop("date"))
    df2 = df.drop(columns=["Annee", "Mois", "Jour"])

    logging.info("Removing rows with empty date...")
    df3 = df2.dropna(subset=["date"])

    logging.info("Unpivoting table...")
    df_pax = df3.melt(
        id_vars=["date"],
        value_name="Pax",
        var_name="variable_pax",
        value_vars=[
            "PAX_Pax",
            "FRET_EXPRESS_Pax",
            "FRET_CARGO_Pax",
            "AUTRES_Pax",
            "Total_Pax",
        ],
    )
    df_fret = df3.melt(
        id_vars=["date"],
        value_name="Fret",
        var_name="variable_fret",
        value_vars=[
            "PAX_Fret",
            "FRET_EXPRESS_Fret",
            "FRET_CARGO_Fret",
            "AUTRES_Fret",
            "Total_Fret",
        ],
    )
    df_mvt = df3.melt(
        id_vars=["date"],
        value_name="Mvt",
        var_name="variable_mvt",
        value_vars=[
            "PAX_Mvt",
            "FRET_EXPRESS_Mvt",
            "FRET_CARGO_Mvt",
            "AUTRES_Mvt",
            "Total_Mvt",
        ],
    )

    logging.info("Getting Kategorie as first part of string...")
    # df_pax['Kategorien'] = df_pax['variable'].str.split('_', n=1)
    # df_pax['Kategorie'] = df_pax['Kategorien'].apply(lambda x: x[0])
    df_pax["Kategorie"] = (
        df_pax["variable_pax"].str.rsplit("_", n=1).apply(lambda x: x[0])
    )
    df_fret["Kategorie"] = (
        df_fret["variable_fret"].str.rsplit("_", n=1).apply(lambda x: x[0])
    )
    df_mvt["Kategorie"] = (
        df_mvt["variable_mvt"].str.rsplit("_", n=1).apply(lambda x: x[0])
    )

    logging.info("Merging data frames into one again...")
    df_merged1 = pd.merge(df_pax, df_fret, on=["date", "Kategorie"], how="outer")
    df_merged = pd.merge(df_merged1, df_mvt, on=["date", "Kategorie"], how="outer")

    logging.info("Sorting...")
    df_sort = df_merged.sort_values(by=["date", "Kategorie"], ascending=False)

    logging.info("Replacing french with german words in Kategorie...")
    df_german = df_sort.replace(
        {
            "Kategorie": {
                "PAX": "Passagierverkehr",
                "FRET_EXPRESS": "Fracht Express",
                "FRET_CARGO": "Fracht Cargo",
                "AUTRES": "Andere Kategorien",
            }
        }
    )

    logging.info("Removing Totals...")
    df_nototal = df_german[df_german.Kategorie != "Total"]
    return df_nototal


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f"Executing {__file__}...")
    main()
    logging.info("Job successful!")
