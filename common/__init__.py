import requests
import os
import ftplib
import time
import urllib3
import ssl
from functools import wraps
import pandas as pd
import fnmatch
import logging
from datetime import datetime, timedelta, timezone
# see https://pypi.org/project/backports.zoneinfo/
try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports import zoneinfo


weekdays_german = ['Montag', 'Dienstag', 'Mittwoch', 'Donnerstag', 'Freitag', 'Samstag', 'Sonntag']
http_errors_to_handle = ConnectionResetError, urllib3.exceptions.MaxRetryError, requests.exceptions.ProxyError, requests.exceptions.HTTPError, ssl.SSLCertVerificationError
ftp_errors_to_handle = ftplib.error_temp, ftplib.error_perm, BrokenPipeError, ConnectionResetError, ConnectionRefusedError, EOFError, FileNotFoundError


# Source: https://github.com/saltycrane/retry-decorator/blob/master/retry_decorator.py
# BSD license: https://github.com/saltycrane/retry-decorator/blob/master/LICENSE
def retry(ExceptionToCheck, tries=4, delay=3, backoff=2, logger=None):
    """Retry calling the decorated function using an exponential backoff.

    https://www.saltycrane.com/blog/2009/11/trying-out-retry-decorator-python/
    original from: https://wiki.python.org/moin/PythonDecoratorLibrary#Retry

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
    r = requests.get(*args, **kwargs)
    r.raise_for_status()
    return r


@retry(http_errors_to_handle, tries=6, delay=5, backoff=1)
def requests_post(*args, **kwargs):
    r = requests.post(*args, **kwargs)
    r.raise_for_status()
    return r


@retry(http_errors_to_handle, tries=6, delay=5, backoff=1)
def requests_patch(*args, **kwargs):
    r = requests.patch(*args, **kwargs)
    r.raise_for_status()
    return r


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
def download_ftp(files: list, server: str, user: str, password: str, remote_path: str, local_path: str, pattern: str):
    print(f'Connecting to FTP Server "{server}" in path "{remote_path}" to download file(s) "{files}" or pattern "{pattern}" to local path "{local_path}"...')
    ftp = ftplib.FTP(server, user, password)
    ftp.cwd(remote_path)
    files_to_download = []
    if len(files) > 0:
        files_to_download = files
    elif len(pattern) > 0:
        logging.info(f'Filtering list of files using pattern...')
        files_to_download = fnmatch.filter(ftp.nlst(), pattern)

    local_files = []
    for file in files_to_download:
        local_file = os.path.join(local_path, file)
        local_files.append(local_file)
        print(f'Retrieving file {local_file}...')
        with open(local_file, 'wb') as f:
            ftp.retrbinary(f"RETR {file}", f.write)
    ftp.quit()
    return


@retry(ftp_errors_to_handle, tries=6, delay=2, backoff=1)
def ensure_ftp_dir(server, user, password, folder):
    print(f'Connecting to FTP server {server} to make sure folder {folder} exists...')
    ftp = ftplib.FTP(server, user, password)
    try:
        ftp.mkd(folder)
    except ftplib.all_errors as e:
        if str(e).split(None, 1)[1] == "Can't create directory: File exists":
            print(f'Folder (or file with same name) exists already, doing nothing. ')
        else:
            raise e
    finally:
        ftp.quit()


# Tell Opendatasoft to (re-)publish datasets
# How to get the dataset_uid from ODS:
# curl --proxy https://USER:PASSWORD@PROXYSERVER:PORT -i https://data.bs.ch/api/management/v2/datasets/?where=datasetid='100001' -u username@bs.ch:password123
# Or without proxy via terminal:
# curl -i "https://data.bs.ch/api/management/v2/datasets/?where=datasetid=100001" -u username@bs.ch:password123

# Retry with some delay in between if any explicitly defined error is raised
@retry(http_errors_to_handle, tries=6, delay=10, backoff=1)
def publish_ods_dataset(dataset_uid, creds):
    print("Telling OpenDataSoft to reload dataset " + dataset_uid + '...')
    response = requests.put('https://data.bs.ch/api/management/v2/datasets/' + dataset_uid + '/publish', params={'apikey': creds.api_key}, proxies={'https': creds.proxy})
    if not response.ok:
        print(f'Received http error {response.status_code}:')
        print(f'Error message: {response.text}')
        r_json = response.json()
        # current_status = r_json['raw_params']['current_status']
        error_key = r_json['error_key']
        status_code = r_json['status_code']
        # if status_code == 400 and (current_status == 'queued' or current_status == 'processing_all_dataset_data'):
        if status_code == 400 and error_key == 'InvalidDatasetStatusPreconditionException':
            print(f'ODS returned status 400 and error_key "{error_key}", thus we presume all is ok.')
        else:
            response.raise_for_status()

        # Received http error 400:
        # Error message: {
        #   "status_code": 400,
        #   "message": "Invalid precondition, dataset status is 'queued' but must be one of 'idle, error, limit_reached'",
        #   "raw_params": {
        #     "authorized_origin_status": "idle, error, limit_reached",
        #     "current_status": "queued"
        #   },
        #   "raw_message": "Invalid precondition, dataset status is '{current_status}' but must be one of '{authorized_origin_status}'",
        #   "error_key": "InvalidDatasetStatusPreconditionException"
        # }


def get_ods_uid_by_id(ods_id, creds):
    print(f'Retrieving ods uid for ods id {id}...')
    response = requests_get(url=f'https://data.bs.ch/api/management/v2/datasets/?where=datasetid="{ods_id}"', auth=(creds.user_name, creds.password), proxies={'https': creds.proxy})
    return response.json()['datasets'][0]['dataset_uid']


@retry(http_errors_to_handle, tries=6, delay=5, backoff=1)
def pandas_read_csv(*args, **kwargs):
    return pd.read_csv(*args, **kwargs)


def is_embargo_over(data_file_path, embargo_file_path=None) -> bool:
    if embargo_file_path is None:
        embargo_file_path = os.path.splitext(data_file_path)[0] + '_embargo.txt'
    with open(embargo_file_path, 'r') as f:
        embargo_datetime_str = f.readline()
        logging.info(f'Read string {embargo_datetime_str} from file {embargo_file_path}')
    embargo_datetime = datetime.fromisoformat(embargo_datetime_str)
    if embargo_datetime.tzinfo is None:
        logging.info(f'Datetime string is not timezone aware ("naive"), adding timezone info "Europe/Zurich"...')
        embargo_datetime = embargo_datetime.replace(tzinfo=ZoneInfo('Europe/Zurich'))
    now_in_switzerland = datetime.now(timezone.utc).astimezone(ZoneInfo('Europe/Zurich'))
    embargo_over = now_in_switzerland > embargo_datetime
    logging.info(f'Embargo over: {embargo_over}')
    return embargo_over


def ods_realtime_push_df(df, url, push_key):
    row_count = len(df)
    if row_count == 0:
        print(f'No rows to push to ODS... ')
    else:
        print(f'Pushing {row_count} rows to ODS realtime API...')
        # Realtime API bootstrap data:
        # {
        #     "id": 0,
        #     "value_updated": "2022-01-14T08:44:56.000Z",
        #     "value_label": "1"
        # }
        payload = df.to_json(orient="records")
        # print(f'Pushing the following data to ODS: {json.dumps(json.loads(payload), indent=4)}')
        # use data=payload here because payload is a string. If it was an object, we'd have to use json=payload.
        r = requests_post(url=url, data=payload, params={'pushkey': push_key})
        return r


def collapse_multilevel_column_names(df: pd.DataFrame, sep='_'):
    # Replace the 2-level column names with a string that concatenates both strings
    df.columns = [sep.join(str(c) for c in col) for col in df.columns.values]
    return df
