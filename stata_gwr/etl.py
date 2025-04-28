import logging
import os
import zipfile

import common
import common.change_tracking as ct
import pandas as pd


def main():
    GEB = {
        "filename": "gebaeude_batiment_edificio{}.csv",
        "to_decode": [
            "GKSCE",
            "GSTAT",
            "GKAT",
            "GKLAS",
            "GBAUP",
            "GVOLNORM",
            "GVOLSCE",
            "GSCHUTZR",
            "GWAERZH1",
            "GENH1",
            "GWAERSCEH1",
            "GWAERZH2",
            "GENH2",
            "GWAERSCEH2",
            "GWAERZW1",
            "GENW1",
            "GWAERSCEW1",
            "GWAERZW2",
            "GENW2",
            "GWAERSCEW2",
        ],
    }
    DOM = {
        "filename": "eingang_entree_entrata{}.csv",
        "to_decode": ["STRSP", "STROFFIZIEL", "DOFFADR"],
    }
    WHG = {
        "filename": "wohnung_logement_abitazione{}.csv",
        "to_decode": ["WSTWK", "WMEHRG", "WSTAT", "WKCHE"],
    }

    all_entities = [GEB, DOM, WHG]

    DECODED_COL_APPENDIX = "_DECODED"
    DECODED_FILE_APPENDIX = "_bs"  # with empty string, this overwrites original files. Use 'decoded' or similar to create new files

    r = common.requests_get("https://public.madd.bfs.admin.ch/bs.zip")
    r.raise_for_status()
    data_orig_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)), "data_orig"
    )
    zip_folder = "bs"
    zip_file_path = os.path.join(data_orig_path, f"{zip_folder}.zip")
    with open(zip_file_path, "wb") as f:
        f.write(r.content)
    if ct.has_changed(zip_file_path):
        with zipfile.ZipFile(zip_file_path) as z:
            z.extractall(os.path.join(data_orig_path, zip_folder))
        codes = pd.read_table(
            os.path.join(data_orig_path, zip_folder, "kodes_codes_codici.csv"),
            dtype=str,
        )
        code_map = pd.Series(codes.CODTXTLD.values, index=codes.CECODID).to_dict()
        logging.info("Adding decoded columns to datasets...")
        data_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "data")
        exported_files = []
        for entity in all_entities:
            ent = pd.read_table(
                os.path.join(data_orig_path, zip_folder, entity["filename"].format("")),
                dtype=str,
            )
            for var in entity["to_decode"]:
                ent.insert(ent.columns.get_loc(var) + 1, var + DECODED_COL_APPENDIX, "")
                ent[var + DECODED_COL_APPENDIX] = ent[var].map(code_map)
            export_file_path = os.path.join(
                data_path, entity["filename"].format(DECODED_FILE_APPENDIX)
            )
            ent.to_csv(export_file_path, index=False, sep="\t")
            exported_files.append(export_file_path)
        for filename in os.listdir(data_path):
            file_path = os.path.join(data_path, filename)
            if (
                os.path.isfile(file_path)
                and ".csv" in file_path
                and ct.has_changed(file_path)
            ):
                common.upload_ftp(file_path, remote_path="gwr/opendata_export")
        ods_ids = ["100230", "100231", "100232"]
        for ods_id in ods_ids:
            common.publish_ods_dataset_by_id(ods_id)
        ct.update_hash_file(zip_file_path)
    pass


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f"Executing {__file__}...")
    main()
    logging.info("Job successful!")
