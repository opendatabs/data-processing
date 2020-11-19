from md_covid19cases import credentials
import common
import os
import pandas as pd

filename = os.path.join(credentials.path, credentials.filename)
# print(f'Reading {filename} into data frame...')
# df = pd.read_csv(filename, sep=';')
# print(f'Retaining only necessary columns...')
# df = df[['pers_alter', 'geschlecht', 'test_datum']]
# export_filename = os.path.join(credentials.path, credentials.export_filename)
# print(f'Exporting data to {export_filename}...')
# df.to_csv(export_filename, index=False)
print(f'Uploading data to FTP server...')
common.upload_ftp(filename, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'md/covid19_cases')
print('Job successful!')