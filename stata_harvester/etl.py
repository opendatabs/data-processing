import os
import logging

import common
import common.change_tracking as ct
from dotenv import load_dotenv

load_dotenv()

DATA_ORIG_PATH = os.getenv("DATA_ORIG_PATH")

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    logging.info(f'Checking if harvester file has changed...')
    if ct.has_changed(os.path.join(DATA_ORIG_PATH, 'OpendataSoft_Export_Stata.csv')):
        logging.info('File has changed, uploading to FTP...')
        for file in os.listdir(DATA_ORIG_PATH):
            common.upload_ftp(os.path.join(DATA_ORIG_PATH, file), os.getenv("FTP_SERVER"), os.getenv("FTP_USER"), os.getenv("FTP_PASS"), 
                              'harvesters/stata/ftp-csv')
        ct.update_hash_file(os.path.join(DATA_ORIG_PATH, 'OpendataSoft_Export_Stata.csv'))
        # Run ods_harvest/etl.py
        logging.info('Running ods_harvest/etl.py...')
        os.system('python3 ../ods_harvest/etl.py stata-ftp-csv')
    else:
        logging.info('File has not changed, skipping upload...')
    logging.info('Job successful!')
