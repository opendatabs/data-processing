import common
import os
from stata_daily_upload import credentials

uploads = [{'file': 'sterbefaelle.csv', 'dest_dir': 'bevoelkerung'},
           {'file': 'geburten_nach_datum.csv', 'dest_dir': 'bevoelkerung'},
           {'file': 'geburten_nach_monat.csv', 'dest_dir': 'bevoelkerung'}]
for upload in uploads:
    common.upload_ftp(os.path.join(credentials.path_work, upload['file']), credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, upload['dest_dir'])

print('Job successful!')