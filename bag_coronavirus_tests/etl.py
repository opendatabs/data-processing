from bag_coronavirus_tests import credentials
import os
import common
import glob

search_string = os.path.join(credentials.path, '*CovidTests_BS.xlsx')
print(f'Searching for files matching "{search_string}"...')
files = glob.glob(search_string)
print(f'Found {len(files)} matching files. Uploading to FTP server...')
for file in files:
    common.upload_ftp(os.path.join(credentials.path, file), credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'BAG_Coronavirus_Tests')

print(f'Job successful!')
