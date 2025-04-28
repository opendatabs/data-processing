import logging
import os
import sys

import common
import common.change_tracking as ct
from common import FTP_SERVER, FTP_USER, FTP_PASS

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f"Executing {__file__}...")
    logging.info("Checking if harvester file has changed...")
    files_changed = False
    for file in os.listdir("data_orig"):
        file_path = os.path.join("data_orig", file)
        if ct.has_changed(file_path):
            files_changed = True
            logging.info(f"File {file} has changed, uploading to FTP...")
            common.upload_ftp(
                file_path,
                FTP_SERVER,
                FTP_USER,
                FTP_PASS,
                "harvesters/stata/ftp-csv",
            )
            ct.update_hash_file(file_path)

    if files_changed:
        # Run ods_harvest/etl.py
        sys.exit(0)
    else:
        sys.exit(99)
