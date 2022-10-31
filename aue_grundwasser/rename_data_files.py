import os

import common
from aue_grundwasser import credentials
import logging


def main():
    remote_path = 'roh'
    pattern = 'BS_Grundwasser_odProc_*.csv'
    listing = common.download_ftp([], credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, remote_path, credentials.data_orig_path, pattern, list_only=True)
    for file in listing:
        from_file = os.path.join(file['remote_path'], file['remote_file'])
        to_name = file['remote_file'].replace('_odProc_', '_odExp_')
        common.rename_ftp(from_file, to_name, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass)
    pass


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info('Job successful!')
