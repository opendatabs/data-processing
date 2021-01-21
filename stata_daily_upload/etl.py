import common
import os
from stata_daily_upload import credentials

uploads = [{'file': 'Bevoelkerung/sterbefaelle.csv', 'dest_dir': 'bevoelkerung'},
           {'file': 'Bevoelkerung/geburten_nach_datum.csv', 'dest_dir': 'bevoelkerung'},
           {'file': 'Bevoelkerung/geburten_nach_monat.csv', 'dest_dir': 'bevoelkerung'},
           {'file': 'Tourismus/tourismus-daily.csv', 'dest_dir': 'tourismus'},
           {'file': 'Tourismus/tourismus-monthly.csv', 'dest_dir': 'tourismus'}]


@common.retry(BrokenPipeError, tries=10, delay=10, backoff=1)
def upload_ftp(file, dest_dir):
    common.upload_ftp(os.path.join(credentials.path_work, file), credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, dest_dir)


for upload in uploads:
    upload_ftp(upload['file'], upload['dest_dir'])

print('Job successful!')
