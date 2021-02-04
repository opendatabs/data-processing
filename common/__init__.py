import requests
import os
import ftplib
import time
import urllib3
from functools import wraps

weekdays_german = ['Montag', 'Dienstag', 'Mittwoch', 'Donnerstag', 'Freitag', 'Samstag', 'Sonntag']
http_errors_to_handle = ConnectionResetError, urllib3.exceptions.MaxRetryError, requests.exceptions.ProxyError
ftp_errors_to_handle = ftplib.error_temp, ftplib.error_perm, BrokenPipeError, ConnectionResetError, EOFError, FileNotFoundError


# Source: https://github.com/saltycrane/retry-decorator/blob/master/retry_decorator.py
# BSD license: https://github.com/saltycrane/retry-decorator/blob/master/LICENSE
def retry(ExceptionToCheck, tries=4, delay=3, backoff=2, logger=None):
    """Retry calling the decorated function using an exponential backoff.

    http://www.saltycrane.com/blog/2009/11/trying-out-retry-decorator-python/
    original from: http://wiki.python.org/moin/PythonDecoratorLibrary#Retry

    :param ExceptionToCheck: the exception to check. may be a tuple of
        exceptions to check
    :type ExceptionToCheck: Exception or tuple
    :param tries: number of times to try (not retry) before giving up
    :type tries: int
    :param delay: initial delay between retries in seconds
    :type delay: int
    :param backoff: backoff multiplier e.g. value of 2 will double the delay
        each retry
    :type backoff: int
    :param logger: logger to use. If None, print
    :type logger: logging.Logger instance
    """
    def deco_retry(f):

        @wraps(f)
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return f(*args, **kwargs)
                except ExceptionToCheck as e:
                    msg = "%s, Retrying in %d seconds..." % (str(e), mdelay)
                    if logger:
                        logger.warning(msg)
                    else:
                        print(msg)
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            return f(*args, **kwargs)

        return f_retry  # true decorator

    return deco_retry


@retry(http_errors_to_handle, tries=6, delay=5, backoff=1)
def requests_get(*args, **kwargs):
    return requests.get(*args, **kwargs)


@retry(http_errors_to_handle, tries=6, delay=5, backoff=1)
def requests_post(*args, **kwargs):
    return requests.post(*args, **kwargs)


# Upload file to FTP Server
# Retry with some delay in between if any explicitly defined error is raised
@retry(ftp_errors_to_handle, tries=6, delay=10, backoff=1)
def upload_ftp(filename, server, user, password, remote_path):
    print("Uploading " + filename + " to FTP server directory " + remote_path + '...')
    # change to desired directory first
    curr_dir = os.getcwd()
    rel_path, filename_no_path = os.path.split(filename)
    if rel_path != '':
        os.chdir(rel_path)
    ftp = ftplib.FTP(server)
    ftp.login(user, password)
    ftp.cwd(remote_path)
    with open(filename_no_path, 'rb') as f:
        ftp.storbinary('STOR %s' % filename_no_path, f)
    ftp.quit()
    os.chdir(curr_dir)
    return


# Download files from FTP server
# Retry with some delay in between if any explicitly defined error is raised
@retry(ftp_errors_to_handle, tries=6, delay=10, backoff=1)
def download_ftp(files, server, user, password, remote_path, local_path):
    print(f'Connecting to FTP Server "{server}" in path "{remote_path}" to download file(s) "{files}" to local path "{local_path}"...')
    ftp = ftplib.FTP(server, user, password)
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

# Retry with some delay in between if any explicitly defined error is raised
@retry(http_errors_to_handle, tries=6, delay=10, backoff=1)
def publish_ods_dataset(dataset_uid, creds):
    print("Telling OpenDataSoft to reload dataset " + dataset_uid + '...')
    response = requests.put('https://data.bs.ch/api/management/v2/datasets/' + dataset_uid + '/publish', params={'apikey': creds.api_key}, proxies={'https': creds.proxy})
    if not response.ok:
        print(f'Received http error {response.status_code}:')
        print(f'Error message: {response.text}')
        response.raise_for_status()

    # if response.status_code == 200:
    #     print('ODS publish command successful.')
    # elif response.status_code == 400:
    #     print('ODS publish command returned http error 400, but experience shows that publishing works anyway.')
    #     print(response)
    # else:
    #     print('Problem with OpenDataSoft Management API: ')
    #     print(response)
    #     raise RuntimeError('Problem with OpenDataSoft Management API: ' + response)


def get_ods_uid_by_id(ods_id, creds):
    print(f'Retrieving ods uid for ods id {id}...')
    response = requests_get(url=f'https://data.bs.ch/api/management/v2/datasets/?where=datasetid={ods_id}', auth=(creds.user_name, creds.password), proxies={'https': creds.proxy})
    if not response.ok:
        print(f'Received http error {response.status_code}:')
        print(f'Error message: {response.text}')
        response.raise_for_status()
    return response.json()['datasets'][0]['dataset_uid']