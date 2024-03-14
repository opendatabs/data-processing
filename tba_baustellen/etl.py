import common
import logging
from tba_baustellen import credentials
from requests.auth import HTTPBasicAuth
import os
from datetime import datetime
from common import credentials as common_cred
from requests.auth import HTTPBasicAuth
import ods_publish.etl_id as odsp
from common import change_tracking as ct
import pandas as pd



def main():
    r = common.requests_get(url=credentials.url, auth=HTTPBasicAuth(credentials.user, credentials.pw))
    if len(r.text) == 0:
        logging.error('No data retrieved from API!')
        raise RuntimeError('No data retrieved from API.')
    else:
        df = pd.read_json(r.text)
        df_export = df[['id', 'projekt_name', 'projekt_beschrieb', 'projekt_info', 'projekt_link', 'datum_von', 'datum_bis', 'geometry']]
        curr_dir = os.path.dirname(os.path.realpath(__file__))
        export_filename = f"{curr_dir}/data/baustellen.csv"
        df_export.to_csv(export_filename, index=False)
        if ct.has_changed(export_filename):
            common.upload_ftp(export_filename, common_cred.ftp_server, common_cred.ftp_user, common_cred.ftp_pass,
                              'tba/baustellen')
            odsp.publish_ods_dataset_by_id('100359')
            ct.update_hash_file(export_filename)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info('Job successful!')