from md_covid19cases import credentials
import common
import os

filename = os.path.join(credentials.path, credentials.filename_faelle_details)
print(f'Uploading data to FTP server...')
common.upload_ftp(filename, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'md/covid19_cases')
print('Job successful!')
