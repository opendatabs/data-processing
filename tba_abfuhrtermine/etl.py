import common
import os
from tba_abfuhrtermine import credentials

file_path = os.path.join(credentials.path, credentials.filename)
print(f'Uploading {file_path} to FTP server...')
common.upload_ftp(file_path, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'tba/abfuhrtermine')
print('Job successful!')