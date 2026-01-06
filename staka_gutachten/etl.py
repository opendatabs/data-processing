import logging
import os
import shutil
from pathlib import Path

import common
import pandas as pd

DATA_ORIG_PATH = "data_orig"


def sanitize_filename(name: str) -> str:
    transl_table = str.maketrans({"ä": "ae", "Ä": "Ae", "ö": "oe", "Ö": "Oe", "ü": "ue", "Ü": "Ue", "ß": "ss"})
    name = name.translate(transl_table).replace(" ", "_")
    allowed = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-"
    return "".join(c for c in name if c in allowed)


def process_excel_file():
    excel_filename = "Liste_Gutachten.xlsx"
    excel_file_path = os.path.join(DATA_ORIG_PATH, excel_filename)
    if not os.path.exists(excel_file_path):
        raise FileNotFoundError(f"The file '{excel_filename}' does not exist in the directory '{DATA_ORIG_PATH}'.")

    df = pd.read_excel(excel_file_path)
    df["Dateiname"] = df["Dateiname"].astype(str)
    # Neue Spalte: Dateiname wie er auf dem FTP erscheinen soll
    df["Dateiname_ftp"] = df["Dateiname"].apply(sanitize_filename)

    # Ensure PDFs keep / get the .pdf suffix in the FTP name
    def ensure_pdf_suffix(orig_name: str, ftp_name: str) -> str:
        if Path(orig_name).suffix.lower() == ".pdf" and Path(ftp_name).suffix.lower() != ".pdf":
            return str(Path(ftp_name).with_suffix(".pdf"))
        return ftp_name

    df["Dateiname_ftp"] = [
        ensure_pdf_suffix(o, f) for o, f in zip(df["Dateiname"], df["Dateiname_ftp"])
    ]

    base_url = "https://data-bs.ch/stata/staka/gutachten/"
    gate_url = base_url + "index.html?file="
    df["URL_Datei"] = gate_url + df["Dateiname_ftp"]

    # Check: existieren alle lokalen Dateien mit Originalnamen?
    files_in_data_orig = set(os.listdir(DATA_ORIG_PATH))
    listed_files = set(df["Dateiname"])
    unlisted_files = files_in_data_orig - listed_files - {".gitkeep", "Liste_Gutachten.xlsx", "DESKTOP.INI"}
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
    remote_dir = "staka/gutachten/"
    os.makedirs("data", exist_ok=True)

    for orig_name, ftp_name in zip(df["Dateiname"], df["Dateiname_ftp"]):
        src_path = os.path.join(DATA_ORIG_PATH, orig_name)
        dst_path = os.path.join("data", ftp_name)

        shutil.copy2(src_path, dst_path)

        common.upload_ftp(dst_path, remote_path=remote_dir)
        logging.info(f"Uploaded {orig_name} as {ftp_name} to FTP at {remote_dir}")

    csv_filename = "100489_gutachten.csv"
    csv_file_path = os.path.join("data", csv_filename)
    df_out = df.drop(columns=["Dateiname_ftp"])
    df_out.to_csv(csv_file_path, index=False)
    common.update_ftp_and_odsp(csv_file_path, remote_dir, dataset_id="100489")


def main():
    df = process_excel_file()
    upload_files_to_ftp(df)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
    logging.info("Job successful.")
