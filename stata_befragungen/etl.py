import io
import logging
import os
import shutil
import unicodedata

import common
import common.change_tracking as ct
import pandas as pd
from charset_normalizer import from_path


def main():
    datasets = [
        {
            "data_file": os.path.join("data_orig", "55plus/2011-2023/Daten_Befragung_55_plus_alle_jahre.csv"),
            "var_file": os.path.join("data_orig", "55plus/2011-2023/Variablen_Befragung_55_plus_alle Jahre.csv"),
            "export_folder": "55plus",
            "export_file": "Befragung_55_plus_alle_jahre.csv",
            "ftp_folder": "55plus",
            "jahr": None,
            "ods_id": "100185",
        },
        {
            "data_file": os.path.join("data_orig", "55plus/2023/datensatz_55plus_2023_OGD_TEXT.csv"),
            "var_file": os.path.join("data_orig", "55plus/2023/Variablen_55plus_2023_OGD.csv"),
            "export_folder": "55plus/2023",
            "export_file": "Befragung_55_plus_2023.csv",
            "ftp_folder": "55plus",
            "jahr": 2023,
            "ods_id": "100412",
        },
        {
            "data_file": os.path.join("data_orig", "55plus/2011/Daten_Befragung_55_plus_2019.csv"),
            "var_file": os.path.join("data_orig", "55plus/2011/Variablen_Befragung_55_plus_2019.csv"),
            "export_folder": "55plus/2019",
            "export_file": "Befragung_55_plus_2019.csv",
            "ftp_folder": "55plus",
            "jahr": 2019,
            "ods_id": "100203",
        },
        {
            "data_file": os.path.join("data_orig", "55plus/2011/Daten_Befragung_55_plus_2015.csv"),
            "var_file": os.path.join("data_orig", "55plus/2011/Variablen_Befragung_55_plus_2015.csv"),
            "export_folder": "55plus/2015",
            "export_file": "Befragung_55_plus_2015.csv",
            "ftp_folder": "55plus",
            "jahr": 2015,
            "ods_id": "100204",
        },
        {
            "data_file": os.path.join("data_orig", "55plus/2011/Daten_Befragung_55_plus_2011.csv"),
            "var_file": os.path.join("data_orig", "55plus/2011/Variablen_Befragung_55_plus_2011.csv"),
            "export_folder": "55plus/2011",
            "export_file": "Befragung_55_plus_2011.csv",
            "ftp_folder": "55plus",
            "jahr": 2011,
            "ods_id": "100205",
        },
    ]
    for ds in datasets:
        process_single_file(
            data_file=ds["data_file"],
            var_file=ds["var_file"],
            data_path="data",
            export_folder=ds["export_folder"],
            export_file=ds["export_file"],
            ftp_folder=ds["ftp_folder"],
            ods_id=ds["ods_id"],
            jahr=ds["jahr"],
        )
        publish_zip_from_folder(os.path.join("data", ds["export_folder"]), ds["ftp_folder"])


def publish_zip_from_folder(dir_to_zip, ftp_folder):
    logging.info(f'Creating zip archive of data in folder "{dir_to_zip}" for the web...')
    zip_name = f"{dir_to_zip}.zip"
    shutil.make_archive(dir_to_zip, "zip", dir_to_zip)
    if ct.has_changed(zip_name):
        common.upload_ftp(zip_name, remote_path=f"befragungen/{ftp_folder}")
        ct.update_hash_file(zip_name)


def process_single_file(
    data_file,
    var_file,
    data_path,
    export_folder,
    export_file,
    ftp_folder,
    ods_id,
    jahr=None,
):
    logging.info(f"Processing survey with data file {data_file}...")
    enc = get_encoding(data_file)
    logging.info("Normalizing unicode data to get rid of &nbsp; (\\xa0)...")
    with open(data_file, "r", encoding=enc) as f:
        text = f.read()
        clean_text = unicodedata.normalize("NFKD", text)
    df_data = pd.read_csv(io.StringIO(clean_text), encoding=enc, sep=";", engine="python")
    df_data.to_csv(os.path.join(data_path, export_folder, f"Daten_{export_file}"), index=False)
    if jahr:
        if "Jahr" not in df_data:
            logging.info(f"Re-setting column Jahr to {jahr}...")
            df_data["Jahr"] = jahr
    else:
        logging.info("Jahresübergreifend: Filter out questions which just appear in one year...")
        # TODO: Filter out questions which just appear in one year
    df_data_long = df_data.melt(id_vars=["ID", "Jahr"])

    enc = get_encoding(var_file)
    df_var = pd.read_csv(var_file, encoding=enc, sep=";", engine="python")
    df_var.to_csv(os.path.join(data_path, export_folder, f"Variablen_{export_file}"), index=False)
    df_merge = df_data_long.merge(df_var, how="left", left_on="variable", right_on="FrageName")
    df_merge = df_merge[df_merge["value"] != "**OTHER**"]
    df_export = df_merge[
        [
            "Jahr",
            "ID",
            "FrageName",
            "FrageLabel",
            "FrageLabel_Fortsetzung",
            "FrageLabel_Anmerkung",
            "value",
        ]
    ].rename(columns={"value": "Antwort"})
    export_path = os.path.join(data_path, export_folder, export_file)
    df_export.to_csv(export_path, index=False)
    common.update_ftp_and_odsp(export_path, f"befragungen/{ftp_folder}", ods_id)


def get_encoding(filename):
    logging.info("Retrieving encoding...")
    result = from_path(filename)
    enc = result.best().encoding
    logging.info(f"Retrieved encoding {enc}...")
    return enc


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f"Executing {__file__}...")
    main()
    logging.info("Job successfully completed!")
