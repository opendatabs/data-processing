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


# Tell Opendatasoft to (re-)publish datasets
def publish_ods_dataset(dataset_uid, creds):
    print("Telling OpenDataSoft to reload dataset " + dataset_uid + '...')
    response = requests.put('https://basel-stadt.opendatasoft.com/api/management/v2/datasets/' + dataset_uid + '/publish', params={'apikey': creds.api_key}, proxies={'https': creds.proxy})
    if response.status_code == 200:
        print('ODS publish command successful.')
    else:
        print('Problem with OpenDataSoft Management API: ')
        print(response)
        raise RuntimeError('Problem with OpenDataSoft Management API: ' + response)