import common
import os
from stata_daily_upload import credentials

uploads = [{'file': 'Bevoelkerung/sterbefaelle.csv', 'dest_dir': 'bevoelkerung'},
           {'file': 'Bevoelkerung/geburten_nach_datum.csv', 'dest_dir': 'bevoelkerung'},
           {'file': 'Bevoelkerung/geburten_nach_monat.csv', 'dest_dir': 'bevoelkerung'},
           {'file': 'Tourismus/tourismus-daily.csv', 'dest_dir': 'tourismus'},
           {'file': 'Tourismus/tourismus-monthly.csv', 'dest_dir': 'tourismus'},
           {'file': 'Veranstaltung/veranstaltungen.csv', 'dest_dir': 'veranstaltungen'},]

for upload in uploads:
    common.upload_ftp(os.path.join(credentials.path_work, upload['file']), credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, upload['dest_dir'])

print('Job successful!')
