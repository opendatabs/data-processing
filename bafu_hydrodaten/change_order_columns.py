import os
import pathlib

import common
import pandas as pd

from bafu_hydrodaten import credentials

# get the data from ftp_archive_rhein_backup
ftp_path_backup = credentials.ftp_archive_rhein_backup
local_path_Rhein = os.path.join(pathlib.Path(__file__), "/data/Rhein/backup")
file_pattern_Rhein = "2289_pegel*.csv"


def get_data(
    old_path=ftp_path_backup,
    local_path=local_path_Rhein,
    file_pattern=file_pattern_Rhein,
):
    files = common.download_ftp(
        [],
        credentials.ftp_server,
        credentials.ftp_user,
        credentials.ftp_pass,
        old_path,
        local_path,
        file_pattern,
    )
    return files


# set column order
def set_column_order(
    files,
    columns,
    local_export_path=os.path.join(
        pathlib.Path(__file__).parent,
        "/data/Rhein/new_archive/2289_pegel_abfluss_bis_2023-04-19_00:50:00+01:00.csv",
    ),
):
    df = pd.DataFrame(columns=columns)
    for file in files:
        df_test = pd.read_csv(file["local_file"])
        df = pd.concat([df, df_test])
    df.to_csv(local_export_path, columns=columns, index=False)


# upload new csv
def upload_to_ftp(local_export_path, new_path=credentials.ftp_archive_rhein):
    common.upload_ftp(
        local_export_path,
        credentials.ftp_server,
        credentials.ftp_user,
        credentials.ftp_pass,
        new_path,
    )


def change_order_columns(old_path, new_path, local_path, file_pattern, river_id, columns):
    local_export_path = os.path.join(
        pathlib.Path(__file__).parent,
        f"data/new_archive/{river_id}/{river_id}_bis_2023-04-26.csv",
    )
    files = get_data(old_path=old_path, local_path=local_path, file_pattern=file_pattern)
    set_column_order(files, columns, local_export_path)
    # upload_to_ftp(new_path, local_export_path)


river_ids = ["2106", "2199", "2615"]
for river_id in river_ids:
    ftp_remote_dir = credentials.ftp_remote_dir.replace("river_id", river_id)
    if river_id == "2106":
        columns = [
            "timestamp",
            "pegel",
            "abfluss",
            "temperatur",
            "datum",
            "zeit",
            "intervall",
        ]
    elif river_id == "2199" or river_id == "2289":
        columns = ["timestamp", "pegel", "abfluss", "datum", "zeit", "intervall"]
    elif river_id == "2615":
        columns = ["timestamp", "pegel", "datum", "zeit", "intervall"]
    change_order_columns(
        old_path=ftp_remote_dir,
        new_path=ftp_remote_dir,
        local_path=os.path.join(pathlib.Path(__file__).parent, f"data/backup/{river_id}"),
        file_pattern=f"{river_id}_pegel*.csv",
        river_id=river_id,
        columns=columns,
    )
