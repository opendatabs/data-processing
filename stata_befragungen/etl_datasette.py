import logging
import os
import sqlite3

import common
import common.change_tracking as ct
import pandas as pd


def main():
    datasets = [
        {
            "data_file": "data/55plus/2023/Daten_Befragung_55_plus_2023.csv",
            "var_file": "data/55plus/2023/Variablen_Befragung_55_plus_2023.csv",
            "export_folder": "datasette",
            "export_file": "Befragung_55plus_2023.db",
            "ftp_folder": "55plus",
        },
        {
            "data_file": "data/55plus/2019/Daten_Befragung_55_plus_2019.csv",
            "var_file": "data/55plus/2019/Variablen_Befragung_55_plus_2019.csv",
            "export_folder": "datasette",
            "export_file": "Befragung_55plus_2019.db",
            "ftp_folder": "55plus",
        },
        {
            "data_file": "data/55plus/2015/Daten_Befragung_55_plus_2015.csv",
            "var_file": "data/55plus/2015/Variablen_Befragung_55_plus_2015.csv",
            "export_folder": "datasette",
            "export_file": "Befragung_55plus_2015.db",
            "ftp_folder": "55plus",
        },
        {
            "data_file": "data/55plus/2011/Daten_Befragung_55_plus_2011.csv",
            "var_file": "data/55plus/2011/Variablen_Befragung_55_plus_2011.csv",
            "export_folder": "datasette",
            "export_file": "Befragung_55plus_2011.db",
            "ftp_folder": "55plus",
        },
    ]

    for ds in datasets:
        convert_to_sqlite(
            data_file=ds["data_file"],
            var_file=ds["var_file"],
            export_folder=ds["export_folder"],
            export_file=ds["export_file"],
            ftp_folder=ds["ftp_folder"],
        )


def convert_to_sqlite(data_file, var_file, export_folder, export_file, ftp_folder):
    if ct.has_changed(data_file):
        # Load the data
        data = pd.read_csv(data_file)

        # Create a connection
        export_path = os.path.join("data", export_folder, export_file)
        conn = sqlite3.connect(export_path)

        # Write the data to the database
        data.to_sql("Antworten", con=conn, if_exists="replace", index=False)

        # TODO: Use var_file to create descriptions of columns

        # Create indices for every column except weight and year
        columns_to_index = [col for col in data.columns if col not in ["weight", "Jahr", "ID"]]
        common.create_indices(conn, "Antworten", columns_to_index)

        # Upload to ftp
        common.upload_ftp(
            export_path,
            remote_path=f"befragungen/{ftp_folder}",
        )

        conn.close()
        ct.update_hash_file(data_file)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f"Executing {__file__}...")
    main()
    logging.info("Job successfully completed!")
