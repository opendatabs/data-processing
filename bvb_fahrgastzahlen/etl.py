import logging
import os
from datetime import datetime

import common
import pandas as pd


def main():
    # Load newest file (2024+)
    df_new = get_the_new_file(directory="data_orig", sheet_name="Monatswerte")

    # Load fixed historical file (2020–2023)
    historical_file = os.path.join(
        "data_orig",
        "20260220 Anfrage Amt für Statistik Monatswerte 2020-2023.xlsx",
    )
    df_old = pd.read_excel(historical_file, sheet_name="Monatswerte")

    # Transform both
    df_new = transform_the_file(df_new)
    df_old = transform_the_file(df_old)

    # Combine
    df = pd.concat([df_old, df_new], ignore_index=True)

    # Optional but recommended: remove duplicates if overlap ever occurs
    df = df.drop_duplicates(
        subset=["Startdatum Kalenderwoche/Monat"], keep="last"
    )

    df = df.sort_values("Startdatum Kalenderwoche/Monat")

    path_export = os.path.join("data", "export", "BVB_monthly.csv")
    save_the_file(df=df, directory=path_export)
    common.update_ftp_and_odsp(path_export, "bvb/fahrgastzahlen", "100075")


def get_the_new_file(directory, sheet_name):
    # Liste der Excel-Dateien im Ordner
    excel_files = [f for f in os.listdir(directory) if f.endswith(".xlsx")]

    # Initialisiere das neueste Datum als None und den neuesten Dateinamen als leer
    latest_date = None
    latest_file = ""

    # Iteriere über alle Excel-Dateien und finde die neueste
    for file in excel_files:
        # Extrahiere das Datum aus dem Dateinamen und konvertiere es
        date_str = file.split()[0]
        datetime_obj = datetime.strptime(date_str, "%y%m%d")
        # Überprüfe, ob das aktuelle Datum neuer ist als das bisherige neueste Datum
        if latest_date is None or datetime_obj > latest_date:
            latest_date = datetime_obj
            latest_file = file
    df = pd.read_excel(os.path.join(directory, latest_file), sheet_name=sheet_name)
    return df


def transform_the_file(df):
    # Zeile 2023 löschen
    df = df[df["Fahrgäste (Einsteiger*innen)"] > 2023]
    # DataFrame umformen (melt)
    value_vars = df.columns[1:]
    df = df.melt(id_vars="Fahrgäste (Einsteiger*innen)", value_vars=value_vars)
    df.columns = [
        "Year",
        "Month",
        "Fahrgäste (Einsteiger*innen)",
    ]  # Spaltennamen aktualisieren

    month_mapping = {
        "Januar": "01",
        "Februar": "02",
        "März": "03",
        "April": "04",
        "Mai": "05",
        "Juni": "06",
        "Juli": "07",
        "August": "08",
        "September": "09",
        "Oktober": "10",
        "November": "11",
        "Dezember": "12",
    }

    df["Month"] = df["Month"].map(month_mapping)

    df = df.sort_values(["Year", "Month"])
    df.insert(2, "Granularität", "Monat")
    df.insert(
        3,
        "Startdatum Kalenderwoche/Monat",
        df["Year"].astype(str) + "-" + df["Month"] + "-01",
    )

    df["Kalenderwoche"] = ""
    df["Datum der Monatswerte"] = df["Year"].astype(str) + "-" + df["Month"] + "-01"

    df = df.drop(["Year", "Month"], axis=1)

    # Zeilen mit NaN-Werten löschen
    df = df.dropna()
    return df


def save_the_file(df, directory):
    # transform the file to csv and save it
    df.to_csv(directory, index=0)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f"Executing {__file__}...")
    main()
    logging.info("Job successful!")
