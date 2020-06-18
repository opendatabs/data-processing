from ftplib import FTP
import common
from bafu_hydrodaten import credentials

print(f'Connecting to FTP Server to read data...')
ftp = FTP(credentials.ftp_read_server, credentials.ftp_read_user, credentials.ftp_read_pass)
print(f'Changing to remote dir {credentials.ftp_read_remote_path}...')
ftp.cwd(credentials.ftp_read_remote_path)
files = [credentials.abfluss_file, credentials.pegel_file]
local_files = []
for file in files:
    local_file = f'bafu_hydrodaten/data/{file}'
    local_files.append(local_file)
    print(f'Retrieving file {local_file}...')
    with open(local_file, 'wb') as f:
        ftp.retrbinary(f"RETR {file}", f.write)
ftp.quit()

for local_file in local_files:
    common.upload_ftp(local_file, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, credentials.ftp_remote_dir)
print('Job successful!')
