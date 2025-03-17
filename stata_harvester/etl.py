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
    files_changed = False
    for file in os.listdir(DATA_ORIG_PATH):
        file_path = os.path.join(DATA_ORIG_PATH, file)
        if ct.has_changed(file_path):
            files_changed = True
            logging.info(f'File {file} has changed, uploading to FTP...')
            common.upload_ftp(file_path, os.getenv("FTP_SERVER"), os.getenv("FTP_USER"), os.getenv("FTP_PASS"), 'harvesters/stata/ftp-csv')
            ct.update_hash_file(file_path)
    
    if files_changed:
        # Run ods_harvest/etl.py
        logging.info('Running ods_harvest/etl.py...')
        os.system('python3 ../ods_harvest/etl.py stata-ftp-csv')
    else:
        logging.info('No files changed, skipping upload...')
    logging.info('Job successful!')
