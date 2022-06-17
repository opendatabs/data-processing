import logging
import os
import pathlib
import zoneinfo
import zoneinfo
import pandas as pd
import xlrd
import ods_publish.etl_id as odsp
import common
from ibs_parkhaus_bewegungen import credentials
from common import change_tracking as ct


def main():
    dfs = []
    for entry in credentials.data_files:
        file_name = os.path.join(credentials.data_file_root, entry['file_name'])
        logging.info(f'Parsing file {file_name}...')
        df = pd.read_excel(file_name, skiprows=7, usecols='A:D', header=None, names=['title', 'timestamp_text', 'einfahrten', 'ausfahrten'])
        df = df.dropna(subset=['ausfahrten'])
        df['timestamp'] = pd.to_datetime(df.timestamp_text, format='%Y-%m-%d %H:%M:%S').dt.tz_localize(tz='Europe/Zurich', ambiguous=True).dt.tz_convert('UTC')
        logging.info(f'Adding rows with no data...')
        df = df.set_index('timestamp').asfreq('1H').reset_index()
        df[['einfahrten', 'ausfahrten']] = df[['einfahrten', 'ausfahrten']].fillna(0)
        df.title = entry['title']
        dfs.append(df)
    all_df = pd.concat(dfs)
    all_df = all_df.convert_dtypes()
    export_filename = os.path.join(pathlib.Path(__file__).parent, 'data', 'parkhaus_bewegungen.csv')
    all_df.to_csv(export_filename, index=False)
    if ct.has_changed(export_filename, do_update_hash_file=False):
        common.upload_ftp(export_filename, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, credentials.ftp_path)
        odsp.publish_ods_dataset_by_id('100198')
        ct.update_hash_file(export_filename)
    pass


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info(f'Job successfully completed!')
