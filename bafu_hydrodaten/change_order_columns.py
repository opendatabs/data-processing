import pandas as pd
import common
from bafu_hydrodaten import credentials
import os
import pathlib

# get the data from ftp_archive_rhein_backup
ftp_path_backup = credentials.ftp_archive_rhein_backup
local_path_Rhein = os.path.join(pathlib.Path(__file__), '/data/Rhein/backup')
file_pattern_Rhein = '2289_pegel*.csv'

def get_data(old_path=ftp_path_backup, local_path=local_path_Rhein, file_pattern=file_pattern_Rhein):
    files = common.download_ftp([], credentials.ftp_server, credentials.ftp_user,
                        credentials.ftp_pass, old_path, local_path, file_pattern)
    return files


# set column order: datum,zeit,intervall,pegel,timestamp,abfluss
def set_column_order(files,  local_export_path=os.path.join(pathlib.Path(__file__),  '/data/Rhein/new_archive/2289_pegel_abfluss_'
                                                              'bis_2023-04-19_00:50:00+01:00.csv')):
    df = pd.DataFrame(columns=['timestamp', 'pegel', 'abfluss', 'datum', 'zeit', 'intervall'])
    for file in files:
        df_test = pd.read_csv(file['local_file'])
        df = pd.concat([df, df_test])

    df.to_csv(local_export_path, columns=['timestamp', 'pegel', 'abfluss', 'datum', 'zeit', 'intervall'], index=False)


# upload new csv to ftp_archive_rhein
def upload_to_ftp(local_export_path, new_path=credentials.ftp_archive_rhein):
    common.upload_ftp(local_export_path, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, new_path)

def change_order_columns(old_path, new_path, local_path, file_pattern):
    local_export_path = ''
    files = get_data(old_path=old_path, local_path=local_path, file_pattern=file_pattern)
    set_column_order(files, local_export_path)
    upload_to_ftp(new_path, local_export_path)
