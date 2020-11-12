from ftplib import FTP
import requests
import os


# Upload file to FTP Server
def upload_ftp(filename, server, user, password, remote_path):
    print("Uploading " + filename + " to FTP server directory " + remote_path + '...')
    # change to desired directory first
    curr_dir = os.getcwd()
    rel_path, filename_no_path = os.path.split(filename)
    if rel_path != '':
        os.chdir(rel_path)
    ftp = FTP(server)
    ftp.login(user, password)
    ftp.cwd(remote_path)
    with open(filename_no_path, 'rb') as f:
        ftp.storbinary('STOR %s' % filename_no_path, f)
    ftp.quit()
    os.chdir(curr_dir)
    return


# Download files from FTP server
def download_ftp(files, server, user, password, remote_path, local_path):
    print(f'Connecting to FTP Server "{server}" in path "{remote_path}" to download file(s) "{files}" to local path "{local_path}"...')
    ftp = FTP(server, user, password)
    ftp.cwd(remote_path)
    local_files = []
    for file in files:
        local_file = os.path.join(local_path, file)
        local_files.append(local_file)
        print(f'Retrieving file {local_file}...')
        with open(local_file, 'wb') as f:
            ftp.retrbinary(f"RETR {file}", f.write)
    ftp.quit()
    return


# Tell Opendatasoft to (re-)publish datasets
# How to get the dataset_uid from ODS:
# curl --proxy https://USER:PASSWORD@PROXYSERVER:PORT -i https://data.bs.ch/api/management/v2/datasets/?where=datasetid='100001' -u username@bs.ch:password123
def publish_ods_dataset(dataset_uid, creds):
    print("Telling OpenDataSoft to reload dataset " + dataset_uid + '...')
    response = requests.put('https://basel-stadt.opendatasoft.com/api/management/v2/datasets/' + dataset_uid + '/publish', params={'apikey': creds.api_key}, proxies={'https': creds.proxy})
    if response.status_code == 200:
        print('ODS publish command successful.')
    elif response.status_code == 400:
        print('ODS publish command returned http error 400, but experience shows that publishing works anyway.')
        print(response)
    else:
        print('Problem with OpenDataSoft Management API: ')
        print(response)
        raise RuntimeError('Problem with OpenDataSoft Management API: ' + response)