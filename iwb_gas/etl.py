import logging
import pandas as pd
import datetime
import os
import pathlib
import ods_publish.etl_id as odsp
import common
from iwb_gas import credentials
from common import change_tracking as ct


def main():
    path_def = os.path.join(pathlib.Path(__file__).parents[1], 'iwb_gas/data/gas/def')
    list_files = common.download_ftp([], credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass,
                                     'gas', path_def, '*_DEF_????????.csv')
    # Add data from the "raw files" for the dates which are not yet in a monthly file
    # Take every RAW file from the current and the last month and drop duplicate lines later
    today = datetime.date.today()
    this_month = today.strftime("%Y%m")
    list_files += common.download_ftp([], credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass,
                                          'gas', path_def, f'*_RAW_{this_month}??.csv')
    first = today.replace(day=1)
    last_month = (first - datetime.timedelta(days=1)).strftime("%Y%m")
    list_files += common.download_ftp([], credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass,
                                          'gas', path_def, f'*_RAW_{last_month}??.csv')
    df = pd.DataFrame()
    for file in list_files:
        path = file['local_file']
        df_file = pd.read_csv(path, skiprows=5, sep=';')
        df = pd.concat([df, df_file], ignore_index=True)
    df['Date'] = pd.to_datetime(df['Date'], format='%d.%m.%Y')
    # to do: fix timezone
    df['Timestamp'] = df['Date'].astype(str) + ' ' + df['Time'].astype(str)
    df = df.drop_duplicates(subset=['Timestamp'])
    df['year'] = df['Date'].dt.year
    df['month'] = df['Date'] .dt.month
    df['day'] = df['Date'].dt.day
    path_export = os.path.join(pathlib.Path(__file__).parents[1], 'iwb_gas/data/export/100304.csv')
    df.to_csv(path_export, index=False)
    if ct.has_changed(path_export):
        common.upload_ftp(path_export, credentials.ftp_server_export, credentials.ftp_user_export, credentials.ftp_pass_export,
                              'iwb/gas')
        odsp.publish_ods_dataset_by_id('100304')
        ct.update_hash_file(path_export)

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info('Job successful!')
