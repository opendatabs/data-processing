import logging
import common
import os
from covid19bs import credentials
import common.change_tracking as ct
import ods_publish.etl_id as odsp


def main():
    uploads = [{'src_dir': credentials.path_orig, 'file': 'faelle_minderjaehrige_3j_klassen.csv', 'dest_dir': 'covid19bs', 'ods_id': '100152'}
               ]
    upload_publish_if_changed(uploads)
    print('Job successful!')


def upload_publish_if_changed(uploads):
    for upload in uploads:
        file_path = os.path.join(upload['src_dir'], upload['file'])
        if ct.has_changed(file_path, do_update_hash_file=False):
            common.upload_ftp(file_path, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, upload['dest_dir'])
            odsp.publish_ods_dataset_by_id(upload['ods_id'])
            ct.update_hash_file(file_path)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
