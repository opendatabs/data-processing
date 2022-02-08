import glob
import logging
import os
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import common
import common.change_tracking as ct
import ods_publish.etl_id as odsp
import pandas as pd
from staka_abstimmungen import credentials
from staka_abstimmungen.src.etl_details import calculate_details
from staka_abstimmungen.src.etl_kennzahlen import calculate_kennzahlen


def main():
    logging.info(f'Reading control.csv...')
    df = pd.read_csv(os.path.join(credentials.path, 'control.csv'), sep=';', parse_dates=['Embargo_Test', 'Embargo_Live', 'Ignore_changes_after'])

    active_abst = df.query('Active == True')
    active_active_size = active_abst.Active.size
    if active_active_size == 1:
        logging.info(f'Found {active_active_size} active Abstimmung.')
        for column in ['Embargo_Test', 'Embargo_Live', 'Ignore_changes_after']:
            active_abst[column] = active_abst[column].dt.tz_localize('Europe/Zurich')
        now_in_switzerland = datetime.now(timezone.utc).astimezone(ZoneInfo('Europe/Zurich'))

        process_test = active_abst.Embargo_Test[0] <= now_in_switzerland < active_abst.Ignore_changes_after[0]
        process_live = active_abst.Embargo_Live[0] <= now_in_switzerland < active_abst.Ignore_changes_after[0]
        logging.info(f'We are currently within active time period for Test: {process_test} / for Live: {process_live}.')

        if process_test or process_live:
            data_files = get_latest_data_files()
            abst_datum_string = active_abst.Abstimmungs_datum[0].replace('-', '')
            active_files = [f for f in data_files if abst_datum_string in f]
            logging.info(f'We have {len(active_files)} data files for the current Abstimmung: {active_files}. ')

            data_files_changed = False
            for file in active_files:
                if ct.has_changed(os.path.join(credentials.path, file), do_update_hash_file=False):
                    data_files_changed = True
            logging.info(f'Are there any changes in the active data files? {data_files_changed}.')

            if data_files_changed:
                details_abst_date, de_details = calculate_details(active_files)
                details_export_file_name = os.path.join(credentials.path, 'data-processing-output', f'Abstimmungen_Details_{details_abst_date}.csv')
                details_changed = upload_ftp_if_changed(de_details, details_export_file_name)

                kennz_abst_date, df_kennz = calculate_kennzahlen(active_files)
                kennz_file_name = os.path.join(credentials.path, 'data-processing-output', f'Abstimmungen_{kennz_abst_date}.csv')
                kennz_changed = upload_ftp_if_changed(df_kennz, kennz_file_name)

                # todo: Create live datasets in ODS as a copy of the test datasets if they do not exist yet.
                # todo: Use ods realtime push instead of FTP pull.
                logging.info(f'Publishing ODS datasets for Kennzahlen and Details if the export file has changed, and it is time to publish...')
                if process_live:
                    if kennz_changed:
                        odsp.publish_ods_dataset_by_id(active_abst.ODS_id_Kennzahlen_Live[0])
                    if details_changed:
                        odsp.publish_ods_dataset_by_id(active_abst.ODS_id_Details_Live[0])
                if process_test:
                    if kennz_changed:
                        odsp.publish_ods_dataset_by_id(active_abst.ODS_id_Kennzahlen_Test[0])
                    if details_changed:
                        odsp.publish_ods_dataset_by_id(active_abst.ODS_id_Details_Test[0])

                vorlage_in_filename = [f for f in active_files if 'Vorlage' in f]
                logging.info(f'Number of data files with "Vorlage" in the filename: {len(vorlage_in_filename)}. If 0: setting live ods datasets to public...')
                if process_live and len(vorlage_in_filename) == 0:
                    r_kennz = odsp.ods_set_general_access_policy(active_abst.ODS_id_Kennzahlen_Live[0], 'domain')
                    r_details = odsp.ods_set_general_access_policy(active_abst.ODS_id_Details_Live[0], 'domain')
                    # todo: Maybe send email upon change of general access policy?

                logging.info(f'Checking if it is time to update hash files...')
                if process_test and process_live:
                    for file in active_files:
                        ct.update_hash_file(os.path.join(credentials.path, file))
                else:
                    logging.info('No, it is not time to update has files.')

    elif active_active_size == 0:
        logging.info(f'No active Abstimmung, nothing to do for the moment. ')
    elif active_active_size > 1:
        raise NotImplementedError('Only one Abstimmung must be active at any time!')

    logging.info(f'Job Successful!')


def upload_ftp_if_changed(df, file_name):
    print(f'Exporting to {file_name}...')
    df.to_csv(file_name, index=False)
    has_changed = ct.has_changed(file_name, do_update_hash_file=False)
    if has_changed:
        common.upload_ftp(file_name, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'wahlen_abstimmungen/abstimmungen')
    return has_changed


def get_latest_data_files():
    data_file_names = []
    for pattern in ['*_EID_????????*.xlsx', '*_KAN_????????*.xlsx']:
        file_list = glob.glob(os.path.join(credentials.path, pattern))
        if len(file_list) > 0:
            latest_file = max(file_list, key=os.path.getmtime)
            data_file_names.append(os.path.basename(latest_file))
    return data_file_names


if __name__ == "__main__":
    print(f'Executing {__file__}...')
    main()
