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
    file_1 = os.path.join(pathlib.Path(__file__).parent, 'data_orig', 'Stadtlast_Update_PD.xlsx')
    df = pd.read_excel(file_1)
    df['timestamp_2021'] = create_timestamp(df)
    df['timestamp_2022'] = df['timestamp_2021'].str.replace('2021', '2022')

    df_export = pd.concat(
        [
            df[['timestamp_2021', 'Stadtlast 2021']].rename(columns={'timestamp_2021': 'timestamp', 'Stadtlast 2021': 'netzlast_kwh'}),
            df[['timestamp_2022', 'Stadtlast 2022']].rename(columns={'timestamp_2022': 'timestamp', 'Stadtlast 2022': 'netzlast_kwh'}),
         ]
    )

    file_2 = os.path.join(pathlib.Path(__file__).parent, 'data_orig', 'Stadtlast_25102022.xlsx')
    df2 = pd.read_excel(file_2)
    df2['timestamp'] = create_timestamp(df2)
    df_update = df2[['timestamp', 'Stadtlast']].rename(columns={'Stadtlast': 'netzlast_kwh'})
    df_export = pd.concat([df_export, df_update]).dropna(subset=['netzlast_kwh']).reset_index(drop=True)

    export_filename = os.path.join(os.path.dirname(__file__), 'data', 'netzlast.csv')
    df_export.to_csv(export_filename, index=False, sep=';')
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
