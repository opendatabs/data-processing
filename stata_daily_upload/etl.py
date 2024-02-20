import logging
import common
import os
import json
from stata_daily_upload import credentials
import common.change_tracking as ct
import ods_publish.etl_id as odsp
import datetime


def main():
    # Open the JSON file where the uploads are saved
    path_uploads = os.path.join(credentials.path_work, 'StatA', 'harvesters', 'StatA', 'stata_daily_uploads.json')
    with open(path_uploads, 'r') as jsonfile:
        uploads = json.load(jsonfile)

    if ct.has_changed(path_uploads):
        logging.info('Uploads have changed. Upload to FTP...')
        path_archive = os.path.join(credentials.path_work, 'StatA', 'harvesters', 'StatA', 'archive',
                                    f'stata_daily_uploads_{datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.json')
        with open(path_archive, 'w') as jsonfile:
            json.dump(uploads, jsonfile)
        ct.update_hash_file(path_uploads)

    # TODO: Refactor
    file_not_found_errors = []
    for upload in uploads:
        file_property = upload['file']
        try:
            changed = False
            if isinstance(file_property, list):
                for file in file_property:
                    file_path = os.path.join(credentials.path_work, file)
                    if (not upload.get('embargo')) or (upload.get('embargo') and common.is_embargo_over(file_path)):
                        if ct.has_changed(file_path, method='modification_date'):
                            changed = True
                            ct.update_mod_timestamp_file(file_path)
                            common.upload_ftp(file_path, credentials.ftp_server, credentials.ftp_user,
                                              credentials.ftp_pass, upload['dest_dir'])
                    if upload.get('make_public_embargo') and common.is_embargo_over(file_path):
                        ods_id_property = upload['ods_id']
                        if isinstance(ods_id_property, list):
                            for single_ods_id in ods_id_property:
                                odsp.ods_set_general_access_policy(single_ods_id, 'domain', True)
                        else:
                            odsp.ods_set_general_access_policy(ods_id_property, 'domain', True)

            else:
                file_path = os.path.join(credentials.path_work, upload['file'])
                if (not upload.get('embargo')) or (upload.get('embargo') and common.is_embargo_over(file_path)):
                    if ct.has_changed(file_path, method='modification_date'):
                        changed = True
                        ct.update_mod_timestamp_file(file_path)
                        common.upload_ftp(file_path, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, upload['dest_dir'])
                if upload.get('make_public_embargo') and common.is_embargo_over(file_path):
                    ods_id_property = upload['ods_id']
                    if isinstance(ods_id_property, list):
                        for single_ods_id in ods_id_property:
                            odsp.ods_set_general_access_policy(single_ods_id, 'domain', True)
                    else:
                        odsp.ods_set_general_access_policy(ods_id_property, 'domain', True)
            if changed:
                ods_id_property = upload['ods_id']
                if isinstance(ods_id_property, list):
                    for single_ods_id in ods_id_property:
                        odsp.publish_ods_dataset_by_id(single_ods_id)
                else:
                    odsp.publish_ods_dataset_by_id(ods_id_property)

        except FileNotFoundError as e:
            file_not_found_errors.append(e)
    error_count = len(file_not_found_errors)
    if error_count > 0:
        for e in file_not_found_errors:
            logging.exception(e)
        raise FileNotFoundError(f'{error_count} FileNotFoundErrors have been raised!')
    print('Job successful!')


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
