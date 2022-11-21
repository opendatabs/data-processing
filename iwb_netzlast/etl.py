import logging
import pandas as pd
import os
import pathlib
import openpyxl
import ods_publish.etl_id as odsp
import common
from iwb_netzlast import credentials
from common import change_tracking as ct


def create_timestamp(df):
    return df['Ab-Datum'].dt.to_period('d').astype(str) + 'T' + df['Ab-Zeit'].astype(str)


def main():
    logging.info(f'Processing historical data 2012-2020...')
    hist_dfs = []
    for year in range(2012, 2021):
        logging.info(f'Processing year {year}...')
        file_hist = os.path.join(pathlib.Path(__file__).parent, 'data_orig', f'Stadtlast_IDS_{year}.xls')
        df_hist = pd.read_excel(file_hist, skiprows=22, usecols='B,E')
        hist_dfs.append(df_hist)
    df_history = pd.concat(hist_dfs).rename(columns={'Zeitstempel': 'timestamp', 'Werte (kWh)': 'netzlast_kwh'})
    # In these files, timestamps are at the end of a 15-minute interval. Subtract 15 minutes to match to newer files.
    # df_history['timestamp_start'] = df_history['timestamp'] + pd.Timedelta(minutes=-15)
    # Export as string to match newer files
    df_history['timestamp'] = df_history['timestamp'].dt.strftime('%Y-%m-%dT%H:%M:%S')

    logging.info(f'Processing 2nd half of 2020...')
    hist2 = os.path.join(pathlib.Path(__file__).parent, 'data_orig', 'Stadtlast_2020.xlsx')
    df_history2 = pd.read_excel(hist2)
    df_history2['timestamp'] = create_timestamp(df_history2)
    df_history2 = df_history2[['timestamp', 'Profilwert']].rename(columns={'Profilwert': 'netzlast_kwh'})

    logging.info(f'Processing 2021, 2022 (until 2022-09-20), and 2022 (starting 2022-10-01)...')
    files = ['Stadtlast_2021.xlsx', 'Stadtlast_2022.xlsx', 'Stadtlast_16112022.xlsx']
    new_dfs = []
    for file in files:
        logging.info(f'Processing {file}...')
        file_2 = os.path.join(pathlib.Path(__file__).parent, 'data_orig', file)
        df2 = pd.read_excel(file_2)
        df2['timestamp'] = create_timestamp(df2)
        df_update = df2[['timestamp', 'Stadtlast']].rename(columns={'Stadtlast': 'netzlast_kwh'})
        new_dfs.append(df_update)
    latest_dfs = pd.concat(new_dfs)

    df_export = (pd.concat([df_history, df_history2, latest_dfs])
                 .dropna(subset=['netzlast_kwh'])
                 .reset_index(drop=True)
                 .rename(columns={'timestamp': 'timestamp_interval_start_raw_text'}))
    df_export['timestamp_interval_start'] = pd.to_datetime(df_export.timestamp_interval_start_raw_text, format='%Y-%m-%dT%H:%M:%S').dt.tz_localize('Europe/Zurich', ambiguous='infer')  # , nonexistent='shift_forward')
    df_export['timestamp_interval_start_text'] = df_export['timestamp_interval_start'].dt.strftime('%Y-%m-%dT%H:%M:%S%z')
    export_filename = os.path.join(os.path.dirname(__file__), 'data', 'netzlast.csv')
    df_export[['timestamp_interval_start', 'netzlast_kwh', 'timestamp_interval_start_text']].to_csv(export_filename, index=False, sep=';')
    if ct.has_changed(export_filename):
        common.upload_ftp(export_filename, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'iwb/netzlast')
        odsp.publish_ods_dataset_by_id('100233')
        ct.update_hash_file(export_filename)
    pass


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info('Job successful!')
