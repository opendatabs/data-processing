from datetime import datetime
import os
import common
import logging
from stadtreinigung_sauberkeitsindex import credentials
from common import credentials as common_cred
from requests.auth import HTTPBasicAuth
import ods_publish.etl_id as odsp
from common import change_tracking as ct


def main():
    r = common.requests_get(url=credentials.url, auth=HTTPBasicAuth(credentials.user, credentials.pw))
    if len(r.text) == 0:
        logging.error('No data retrieved from API!')
        raise RuntimeError('No data retrieved from API.')
    else:
        curr_dir = os.path.dirname(os.path.realpath(__file__))
        export_filename = f"{curr_dir}/data/data-{datetime.now().strftime('%Y-%m')}.csv"
        with open(export_filename, 'w') as file:
            file.write(r.text)
        if ct.has_changed(export_filename):
            common.upload_ftp(export_filename, common_cred.ftp_server, common_cred.ftp_user, common_cred.ftp_pass,
                              'stadtreinigung/sauberkeitsindex')
            odsp.publish_ods_dataset_by_id('')
            ct.update_hash_file(export_filename)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info('Job successful!')
