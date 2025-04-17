import logging
import os

import common
from dotenv import load_dotenv

load_dotenv()
FTP_SERVER = os.getenv("FTP_SERVER")
FTP_USER = os.getenv("FTP_USER")
FTP_PASS = os.getenv("FTP_PASS")


def main():
    remote_path = "roh"
    pattern = "BS_Grundwasser_odProc_*.csv"
    listing = common.download_ftp(
        [],
        FTP_SERVER,
        FTP_USER,
        FTP_PASS,
        remote_path,
        "data_orig",
        pattern,
        list_only=True,
    )
    for file in listing:
        from_file = os.path.join(file["remote_path"], file["remote_file"])
        to_name = file["remote_file"].replace("_odProc_", "_odExp_")
        common.rename_ftp(from_file, to_name, FTP_SERVER, FTP_USER, FTP_PASS)
    pass


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f"Executing {__file__}...")
    main()
    logging.info("Job successful!")
