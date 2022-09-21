import os
import pandas as pd
import logging
import common.change_tracking as ct
import ods_publish.etl_id as odsp
import common
from stata_parzellen import credentials


def main():
    CURR_DIR = os.path.dirname(os.path.realpath(__file__))
    parzellen_data_file = os.path.join(CURR_DIR, 'data_orig', 'Liegenschaften_Parzellen.csv')
    if ct.has_changed(parzellen_data_file):
        logging.info(f'Reading data from 4 datasets...')
        df = pd.read_csv(parzellen_data_file)
        df_wohnviertel = common.pandas_read_csv('https://data.bs.ch/explore/dataset/100042/download/?format=csv', sep=';')[['wov_id', 'wov_label', 'wov_name', 'gemeinde_name']]
        df_bezirk = common.pandas_read_csv('https://data.bs.ch/explore/dataset/100039/download/?format=csv', sep=';')[['bez_id', 'bez_label', 'bez_name']]
        df_block = common.pandas_read_csv('https://data.bs.ch/explore/dataset/100040/download/?format=csv', sep=';')[['blo_id', 'blo_label']]
        logging.info(f'Merging datasets...')
        df_export = (df.merge(df_wohnviertel, left_on='WOV_ID', right_on='wov_id', how='left')
                     .merge(df_bezirk, left_on='BEZ_ID', right_on='bez_id', how='left')
                     .merge(df_block, left_on='BLO_ID', right_on='blo_id', how='left')
                     .drop(columns=['wov_id']))
        logging.info(f'Exporting, uploading and updating data in ODS...')
        export_filename = os.path.join(CURR_DIR, 'data', 'Liegenschaften_Parzellen_Names.csv')
        df_export.to_csv(export_filename, index=False)
        common.upload_ftp(export_filename, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'gva/parzellen')
        odsp.publish_ods_dataset_by_id('100202')
        ct.update_hash_file(parzellen_data_file)
    pass


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info('Job successful!')
