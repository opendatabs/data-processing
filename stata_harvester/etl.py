import logging
import os

import common
import common.change_tracking as ct
from dotenv import load_dotenv

load_dotenv()

FTP_SERVER = os.getenv("FTP_SERVER")
FTP_USER = os.getenv("FTP_USER_01")
FTP_PASS = os.getenv("FTP_PASS_01")

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

    if not files_changed:
        logging.info("NO_FILES_CHANGED")
    else:
        logging.info("FILES_CHANGED")
