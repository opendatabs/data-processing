import logging
import os

import common
import create_ics
import pandas as pd


def main():
    excel_path = os.path.join("data_orig", "Frei- und Feiertage.xlsx")
    xlsx = pd.ExcelFile(excel_path)
    sheet_names = xlsx.sheet_names
    df = pd.concat(
        [extract_relevant_rows(sheet, excel_path) for sheet in sheet_names if sheet != "Vorlage"],
        ignore_index=True,
    )
    df.columns = ["Wochentag", "Datum", "Bezeichnung", "Anzahl_Tage"]
    # Give Wochentag german names Montag etc
    df["Wochentag"] = df["Datum"].apply(lambda x: x.strftime("%A") if pd.notna(x) else None)
    df["Wochentag"] = df["Wochentag"].replace(
        {
            "Monday": "Montag",
            "Tuesday": "Dienstag",
            "Wednesday": "Mittwoch",
            "Thursday": "Donnerstag",
            "Friday": "Freitag",
            "Saturday": "Samstag",
            "Sunday": "Sonntag",
        }
    )
    path_export = os.path.join("data", "100448_frei-und-feiertage.csv")
    df.to_csv(path_export, index=False)
    common.update_ftp_and_odsp(path_export, "frei-und-feiertage", "100448")
    update_ics_file_on_ftp_server()


def extract_relevant_rows(sheet_name, excel_path):
    df = pd.read_excel(excel_path, sheet_name=sheet_name, header=None, usecols="B:E")
    start_row = 4

    def is_row_empty(row):
        return all((pd.isna(x) or str(x).strip() == "") for x in row)

    for i in range(start_row, len(df)):
        if is_row_empty(df.iloc[i]):
            return df.iloc[start_row:i]
    return df.iloc[start_row:]


def update_ics_file_on_ftp_server() -> None:
    # Generate ICS file using create_ics.py
    logging.info("Generating ICS file...")
    ics_file_name = create_ics.main()
    logging.info(f"ICS file generation completed successfully: {ics_file_name}")

    # Upload the ICS file to FTP server
    if os.path.exists(ics_file_name):
        logging.info("Uploading ICS file to FTP server...")
        remote_path = "frei-und-feiertage"
        try:
            common.upload_ftp(
                filename=ics_file_name,
                remote_path=remote_path,
            )
            logging.info(f"ICS file uploaded successfully to FTP server in folder '{remote_path}'")
        except Exception as e:
            logging.error(f"Error uploading ICS file to FTP server: {str(e)}")
    else:
        logging.error(f"ICS file not found at path: {ics_file_name}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f"Executing {__file__}...")
    main()
    logging.info("Job completed successfully!")
