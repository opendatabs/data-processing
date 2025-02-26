import os
import logging

import common
from dotenv import load_dotenv

load_dotenv()

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    for file in os.listdir(os.getenv("DATA_ORIG_PATH")):
        common.upload_ftp(file, os.getenv("FTP_SERVER"), os.getenv("FTP_USER"), os.getenv("FTP_PASS"), 'harvesters/stata/ftp-csv')
    logging.info('Job successful!')
