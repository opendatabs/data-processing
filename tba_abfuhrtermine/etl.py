import common
import os
from tba_abfuhrtermine import credentials

common.upload_ftp(os.path.join(credentials.path, credentials.filename), credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'tba/abfuhrtermine')
print('Job successful!')