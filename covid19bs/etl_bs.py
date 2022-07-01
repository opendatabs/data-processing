import logging
from covid19bs import credentials
import pandas as pd
import os
import common
import numpy as np
from common import change_tracking as ct
import ods_publish.etl_id as odsp


def main(): 
    logging.info(f'Starting processing python script {__file__}...')
    pub_file = os.path.join(credentials.path_orig, credentials.filename_pub_date)
    test_file = os.path.join(credentials.path_orig, credentials.filename_test_date)
    pub_file_changed = ct.has_changed(pub_file)
    test_file_changed = ct.has_changed(test_file)
    if pub_file_changed or test_file_changed:
        logging.info(f'Reading data from {pub_file}...')
        df_pubdate = pd.read_csv(pub_file, sep=';')
        logging.info(f'Renaming columns to match openZH dataset...')
        df_pubdate = df_pubdate.rename(columns={
            'datum': 'date',
            'meldezeit': 'time',
            'publizierte_neue_faelle_kum': 'ncumul_conf',
            'publizierte_neue_faelle': 'ndiff_conf',
            'hospitalisierte_bs': 'current_hosp_resident',
            'hospitalisierte_icu': 'current_icu',
            'hospitalisierte_total': 'current_hosp'
            })
        logging.info(f'Replacing missing times with default value of 00:00...')
        df_pubdate.time = df_pubdate.time.replace('None', '00:00')

        logging.info(f'Reading data from {test_file}...')
        df_testdate = pd.read_csv(test_file, sep=';')
        logging.info(f'Renaming columns to match openZH dataset...')
        df_testdate = df_testdate.rename(columns={
            'datum': 'test_date',
            'erholt_bs': 'ncumul_released',
            'gestorbene_bs_kum': 'ncumul_deceased',
            'isoliert_bs': 'current_isolated',
            'diff_erholt_bs': 'ndiff_released',
            'diff_gestorbene_bs': 'ndiff_deceased',
            'quarantaene_bs': 'current_quarantined_total',
            'quarantaene_reise_bs': 'current_quarantined_riskareatravel',
            'quarantaene_kontakt_bs': 'current_quarantined'
        })
        logging.info(f'Calculating pub date...')
        df_testdate['date'] = (pd.to_datetime(df_testdate['test_date']) + pd.Timedelta(days=1)).dt.strftime('%Y-%m-%d')

        manual_data_file = os.path.join(credentials.path_orig, credentials.filename_conf_non_resident)
        logging.info(f'Reading data from {manual_data_file}...')
        df_manual = pd.read_csv(manual_data_file)
        latest_manual_date = df_manual['date'].max()

        logging.info(f'Joining test and pub datasets...')
        df_merged = pd.merge(df_pubdate, df_testdate, on=['date'], how='outer')
        logging.info(f'Deleting rows to be filled by manual data file...')
        df_trunc = df_merged[df_merged['date'] > latest_manual_date]
        logging.info(f'Setting source of auto-generated data file...')
        df_trunc['source'] = 'https://www.gesundheit.bs.ch'

        logging.info(f'Appending generated to manual data file...')
        df_append = df_manual.append(df_trunc)

        logging.info(f'Calculating columns...')
        df_append['abbreviation_canton_and_fl'] = 'BS'
        df_append['current_hosp_non_resident'] = df_append['current_hosp'] - df_append['current_hosp_resident']
        # values for some columns are currently not available
        df_append['ncumul_tested'] = np.nan
        df_append['new_hosp'] = np.nan
        df_append['current_vent'] = np.nan


        logging.info('Calculating differences between current and previous row...')
        df_diff = df_append[[#'ncumul_conf', 'ncumul_released', 'ncumul_deceased', 'current_hosp',
                             'ncumul_confirmed_non_resident']].diff()
        #df_append['ndiff_conf'] = df_diff.ncumul_conf
        #df_append['ndiff_released'] = df_diff.ncumul_released
        #df_append['ndiff_deceased'] = df_diff.ncumul_deceased
        df_append['ndiff_confirmed_non_resident'] = df_diff.ncumul_confirmed_non_resident

        logging.info(f'Change column order and keeping only necessary columns...')
        df_append = df_append[['date', 'time', 'abbreviation_canton_and_fl', 'ncumul_tested', 'ncumul_conf', 'new_hosp', 'current_hosp',
                'current_icu', 'current_vent', 'ncumul_released', 'ncumul_deceased', 'source', 'current_isolated',
                'current_quarantined', 'ncumul_confirmed_non_resident', 'current_hosp_non_resident',
                'current_quarantined_riskareatravel', 'current_quarantined_total',
                'current_hosp_resident', 'ndiff_conf', 'ndiff_released', 'ndiff_deceased', 'ndiff_confirmed_non_resident', 'test_date']]

        logging.info(f'Removing test_date column for the moment...')
        df_append = df_append.drop(columns=['test_date'])

        # export_filename = os.path.join(credentials.path, credentials.filename).replace('.csv', f'_{latest_date}.csv')
        export_filename = os.path.join(credentials.path, credentials.filename)
        logging.info(f'Exporting csv to {export_filename}')
        df_append.to_csv(export_filename, index=False)
        if ct.has_changed(export_filename):
            common.upload_ftp(export_filename, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'covid19bs/auto_generated')
            odsp.publish_ods_dataset_by_id('100073')
            ct.update_hash_file(export_filename)

        if pub_file_changed:
            ct.update_hash_file(pub_file)
        if test_file_changed:
            ct.update_hash_file(test_file)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info('Job successful!')
