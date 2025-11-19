import logging
import os
import shutil

import common
import pandas as pd

DATA_ORIG_PATH = "data_orig"


def sanitize_filename(name: str) -> str:
    transl_table = str.maketrans(
        {
            "ä": "ae",
            "Ä": "Ae",
            "ö": "oe",
            "Ö": "Oe",
            "ü": "ue",
            "Ü": "Ue",
            "ß": "ss",
        }
    )
    name = name.translate(transl_table)
    name = name.replace(" ", "_")
    allowed = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-"
    name = "".join(c for c in name if c in allowed)
    return name


def process_excel_file():
    excel_filename = "Liste_Gutachten.xlsx"
    excel_file_path = os.path.join(DATA_ORIG_PATH, excel_filename)
    if not os.path.exists(excel_file_path):
        raise FileNotFoundError(f"The file '{excel_filename}' does not exist in the directory '{DATA_ORIG_PATH}'.")

    df = pd.read_excel(excel_file_path)
    df["Dateiname"] = df["Dateiname"].astype(str)
    # Neue Spalte: Dateiname wie er auf dem FTP erscheinen soll
    df["Dateiname_ftp"] = df["Dateiname"].apply(sanitize_filename)
    base_url = "https://data-bs.ch/stata/staka/gutachten/"
    df["URL_Datei"] = base_url + df["Dateiname_ftp"]

    # Check: existieren alle lokalen Dateien mit Originalnamen?
    files_in_data_orig = set(os.listdir(DATA_ORIG_PATH))
    listed_files = set(df["Dateiname"])
    unlisted_files = files_in_data_orig - listed_files - {".gitkeep", "Liste_Gutachten.xlsx"}
    if unlisted_files:
        raise ValueError(f"The following files are in 'data_orig' but not in 'Liste_Gutachten': {unlisted_files}")
    missing_files = listed_files - files_in_data_orig
    if missing_files:
        raise ValueError(
            f"The following files are listed in 'Liste_Gutachten' but do not exist in 'data_orig': {missing_files}"
        )

    logging.info("All files in 'data_orig' are listed in 'Liste_Gutachten' and vice versa.")
    return df


def upload_files_to_ftp(df: pd.DataFrame):
    for orig_name, ftp_name in zip(df["Dateiname"], df["Dateiname_ftp"]):
        local_file_path = os.path.join(DATA_ORIG_PATH, orig_name)
        local_file_path_sanitized = os.path.join("data", ftp_name)
        shutil.copy2(local_file_path, local_file_path_sanitized)
        remote_dir = "staka/gutachten/"
        common.upload_ftp(local_file_path_sanitized, remote_path=remote_dir)
        logging.info(f"Uploaded {orig_name} as {ftp_name} to FTP at {remote_dir}")

    csv_filename = "100489_gutachten.csv"
    csv_file_path = os.path.join("data", csv_filename)
    df = df.drop(columns=["Dateiname_ftp"])
    df.to_csv(csv_file_path, index=False)
    common.update_ftp_and_odsp(csv_file_path, "staka/gutachten/", dataset_id="100489")


def main():
    df = process_excel_file()
    upload_files_to_ftp(df)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
    logging.info("Job successful.")
