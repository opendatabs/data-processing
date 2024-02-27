import io
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
import dateutil
from more_itertools import chunked

import common
from common import credentials
from common import change_tracking
import ods_publish.etl_id as odsp
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
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
    r = requests.get(*args, proxies=credentials.proxies, **kwargs)
    r.raise_for_status()
    return r


@retry(http_errors_to_handle, tries=6, delay=5, backoff=1)
def requests_post(*args, **kwargs):
    r = requests.post(*args, proxies=credentials.proxies, **kwargs)
    r.raise_for_status()
    return r


@retry(http_errors_to_handle, tries=6, delay=5, backoff=1)
def requests_patch(*args, **kwargs):
    r = requests.patch(*args, proxies=credentials.proxies, **kwargs)
    r.raise_for_status()
    return r


@retry(http_errors_to_handle, tries=6, delay=5, backoff=1)
def requests_put(*args, **kwargs):
    r = requests.put(*args, proxies=credentials.proxies, **kwargs)
    r.raise_for_status()
    return r


@retry(http_errors_to_handle, tries=6, delay=5, backoff=1)
def requests_delete(*args, **kwargs):
    r = requests.delete(*args, proxies=credentials.proxies, **kwargs)
    r.raise_for_status()
    return r


# Upload file to FTP Server
# Retry with some delay in between if any explicitly defined error is raised
@retry(ftp_errors_to_handle, tries=6, delay=10, backoff=1)
def upload_ftp(filename, server, user, password, remote_path):
    logging.info("Uploading " + filename + " to FTP server directory " + remote_path + '...')
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


# todo: Refactor this into two methods, one for listing only, one for file downloading - this is a mess
# Download files from FTP server
# Retry with some delay in between if any explicitly defined error is raised
@retry(ftp_errors_to_handle, tries=6, delay=10, backoff=1)
def download_ftp(files: list, server: str, user: str, password: str, remote_path: str, local_path: str, pattern: str, list_only=False) -> list:
    logging.info(f'Connecting to FTP Server "{server}" using user "{user}" in path "{remote_path}" to download file(s) "{files}" or pattern "{pattern}" to local path "{local_path}"...')
    ftp = ftplib.FTP(server, user, password)
    ftp.cwd(remote_path)
    remote_files = []
    extended_list = False
    if len(files) > 0:
        remote_files = files
    elif len(pattern) > 0:
        logging.info(f'Filtering list of files using pattern "{pattern}"...')
        # remote_files = fnmatch.filter(ftp.nlst(), pattern)
        ftp_dir_details = ftp.mlsd()
        remote_files = [i for i in (list(ftp_dir_details)) if fnmatch.fnmatch(i[0], pattern)]
        extended_list = True
    files = []
    if list_only:
        logging.info(f'No download required, just file listing...')
    for remote_file in remote_files:
        local_file_name = os.path.join(local_path, remote_file[0] if extended_list else remote_file)
        remote_file_name = remote_file[0] if extended_list else remote_file
        obj = {'remote_file': remote_file_name, 'remote_path': remote_path, 'local_file': local_file_name}
        if extended_list:
            modified = dateutil.parser.parse(remote_file[1]['modify']).astimezone(ZoneInfo('Europe/Zurich')).isoformat()
            obj['modified_remote'] = modified
        files.append(obj)
        if not list_only:
            logging.info(f'FTP downloading file {local_file_name}...')
            with open(local_file_name, 'wb') as f:
                ftp.retrbinary(f"RETR {remote_file_name}", f.write)
    ftp.quit()
    return files


@retry(ftp_errors_to_handle, tries=6, delay=2, backoff=1)
def ensure_ftp_dir(server, user, password, folder):
    logging.info(f'Connecting to FTP server {server} to make sure folder {folder} exists...')
    ftp = ftplib.FTP(server, user, password)
    try:
        ftp.mkd(folder)
    except ftplib.all_errors as e:
        if str(e).split(None, 1)[1] == "Can't create directory: File exists":
            logging.info(f'Folder (or file with same name) exists already, doing nothing. ')
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
@retry(http_errors_to_handle, tries=6, delay=60, backoff=1)
def publish_ods_dataset(dataset_uid, creds, unpublish_first=False):
    logging.info("Telling OpenDataSoft to reload dataset " + dataset_uid + '...')
    if unpublish_first:
        logging.info('Unpublishing dataset first...')
        unpublish_ods_dataset(dataset_uid, creds)
        while not is_unpublished(dataset_uid, creds):
            logging.info('Waiting 10 seconds before checking if dataset is unpublished...')
            time.sleep(10)
    response = requests_put('https://data.bs.ch/api/management/v2/datasets/' + dataset_uid + '/publish',
                            headers={'Authorization': f'apikey {creds.api_key}'})

    if not response.ok:
        raise_response_error(response)

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


def unpublish_ods_dataset(dataset_uid, creds):
    logging.info("Telling OpenDataSoft to unpublish dataset " + dataset_uid + '...')
    response = common.requests_put('https://data.bs.ch/api/management/v2/datasets/' + dataset_uid + '/unpublish',
                                    headers={'Authorization': f'apikey {creds.api_key}'})
    if not response.ok:
        raise_response_error(response)


def is_unpublished(dataset_uid, creds):
    logging.info("Checking if dataset " + dataset_uid + ' is unpublished...')
    published, name, _ = get_dataset_status(dataset_uid, creds)
    return not published and name == 'idle'


def get_dataset_status(dataset_uid, creds):
    logging.info("Getting status of dataset " + dataset_uid + '...')
    response = common.requests_get('https://data.bs.ch/api/management/v2/datasets/' + dataset_uid + '/status',
                                   headers={'Authorization': f'apikey {creds.api_key}'})
    if not response.ok:
        raise_response_error(response)
    return response.json()['published'], response.json()['name'], response.json()['since']


def raise_response_error(response):
    logging.info(f'Received http error {response.status_code}:')
    logging.info(f'Error message: {response.text}')
    r_json = response.json()
    error_key = r_json['error_key']
    status_code = r_json['status_code']
    if status_code == 400 and error_key == 'InvalidDatasetStatusPreconditionException':
        logging.info(f'ODS returned status 400 and error_key "{error_key}", we raise the error now.')
    response.raise_for_status()


def get_ods_uid_by_id(ods_id, creds):
    logging.info(f'Retrieving ods uid for ods id {ods_id}...')
    response = requests_get(url=f'https://data.bs.ch/api/management/v2/datasets/?where=datasetid="{ods_id}"',
                            headers={'Authorization': f'apikey {creds.api_key}'})
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


def ods_realtime_push_complete_update(df_old, df_new, id_columns, url, columns_to_compare=None, push_key=''):
    ods_realtime_push_new_entries(df_old, df_new, id_columns, url, push_key)
    ods_realtime_push_delete_entries(df_old, df_new, id_columns, url, push_key)
    ods_realtime_push_modified_entries(df_old, df_new, id_columns, url, columns_to_compare, push_key)


def ods_realtime_push_new_entries(df_old, df_new, id_columns, url, push_key=''):
    new_rows = change_tracking.find_new_rows(df_old, df_new, id_columns)
    ods_realtime_push_df(new_rows, url, push_key)


def ods_realtime_push_delete_entries(df_old, df_new, id_columns, url, push_key=''):
    deleted_rows = change_tracking.find_deleted_rows(df_new, df_old, id_columns)
    ods_realtime_push_df(deleted_rows, url, push_key, delete=True)


def ods_realtime_push_modified_entries(df_old, df_new, id_columns, url, columns_to_compare=None, push_key=''):
    _, updated_rows = change_tracking.find_modified_rows(df_old, df_new, id_columns, columns_to_compare)
    ods_realtime_push_df(updated_rows, url, push_key)


def batched_ods_realtime_push(df, url, push_key='', chunk_size=1000):
    logging.info(f'Pushing a dataframe in chunks of size {chunk_size} to ods...')
    df_chunks = chunked(df.index, chunk_size)
    for df_chunk_indexes in df_chunks:
        logging.info(f'Submitting a data chunk to ODS...')
        df_chunk = df.iloc[df_chunk_indexes]
        ods_realtime_push_df(df_chunk, url, push_key)


def ods_realtime_push_df(df, url, push_key='', delete=False):
    if not push_key:
        t = url.partition('?pushkey=')
        url = t[0]
        push_key = t[2]
    # Create delete url if delete is True:
    # https://userguide.opendatasoft.com/l/en/article/fqwi39mrdu-keeping-data-up-to-date#deleting_data_using_the_record_id
    if delete:
        url = url.rsplit('push', 1)[0] + 'delete'
    row_count = len(df)
    if row_count == 0:
        logging.info(f"No rows to {'delete' if delete else 'push'} to ODS... ")
    else:
        logging.info(f"{'Deleting' if delete else 'Pushing'} {row_count} rows to ODS realtime API...")
        payload = df.to_json(orient="records")
        # logging.info(f'Pushing the following data to ODS: {json.dumps(json.loads(payload), indent=4)}')
        # use data=payload here because payload is a string. If it was an object, we'd have to use json=payload.
        # TODO: Fix delete requests, they are throwing 405 Method Not Allowed
        r = requests_post(url=url, data=payload, params={'pushkey': push_key})
        r.raise_for_status()
        return r


def collapse_multilevel_column_names(df: pd.DataFrame, sep='_'):
    # Replace the 2-level column names with a string that concatenates both strings
    df.columns = [sep.join(str(c) for c in col) for col in df.columns.values]
    return df


# copied from: https://towardsdatascience.com/automate-email-with-python-1e755d9c6276
def email_message(subject="Python Notification", text="", img=None, attachment=None):
    # build message contents
    msg = MIMEMultipart()
    msg['Subject'] = subject  # add in the subject
    # msg.attach(MIMEText(text))  # add text contents
    msg.attach(MIMEText(text, 'plain', 'utf-8'))  # add plain text contents

    # check if we have anything given in the img parameter
    if img is not None:
        # if we do, we want to iterate through the images, so let's check that
        # what we have is actually a list
        if type(img) is not list:
            img = [img]  # if it isn't a list, make it one
        # now iterate through our list
        for one_img in img:
            img_data = open(one_img, 'rb').read()  # read the image binary data
            # attach the image data to MIMEMultipart using MIMEImage, we add
            # the given filename use os.basename
            msg.attach(MIMEImage(img_data, name=os.path.basename(one_img)))

    # we do the same for attachments as we did for images
    if attachment is not None:
        if type(attachment) is not list:
            attachment = [attachment]  # if it isn't a list, make it one

        for one_attachment in attachment:
            with open(one_attachment, 'rb') as f:
                # read in the attachment using MIMEApplication
                file = MIMEApplication(
                    f.read(),
                    name=os.path.basename(one_attachment)
                )
            # here we edit the attached file metadata
            file['Content-Disposition'] = f'attachment; filename="{os.path.basename(one_attachment)}"'
            msg.attach(file)  # finally, add the attachment to our message object
    return msg


@retry(ftp_errors_to_handle, tries=6, delay=10, backoff=1)
def rename_ftp(from_name, to_name, server, user, password):
    file = os.path.basename(from_name)
    folder = os.path.dirname(from_name)
    ftp = ftplib.FTP(server, user, password)
    logging.info(f'Changing to remote dir {folder}...')
    ftp.cwd(folder)
    logging.info('Searching for file to rename or move...')
    moved = False
    for remote_file, facts in ftp.mlsd():
        if file == remote_file:
            logging.info(f'Moving file to {to_name}...')
            ftp.rename(file, to_name)
            moved = True
            break
    ftp.quit()
    if not moved:
        logging.error(f'File to rename on FTP not found: {file}...')
        raise FileNotFoundError(file)


def list_directories(folder_path, list_txt_path, ignore_list=None):
    if ignore_list is None:
        ignore_list = []
    directories = [f for f in os.listdir(folder_path) if os.path.isdir(os.path.join(folder_path, f))]
    with open(list_txt_path, 'w') as file:
        for item in directories:
            file.write(item + '\n')
    directories_list = [x for x in directories if x not in ignore_list]  # List folders that should be ignored here
    directories_list.sort()
    return directories_list


def list_files(folder_path, list_txt_path, ignore_list=None, recursive=False):
    if ignore_list is None:
        ignore_list = []
    files = []
    if recursive:
        for root, directories, filenames in os.walk(folder_path):
            for filename in filenames:
                files.append(os.path.join(root, filename))
    else:
        files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))]
    with open(list_txt_path, 'w') as file:
        for item in files:
            file.write(item + '\n')
    files_list = [x for x in files if x not in ignore_list]  # List files that should be ignored here
    files_list.sort()
    return files_list


def get_text_from_url(url):
    req = requests_get(url)
    req.raise_for_status()
    return io.StringIO(req.text)


def update_ftp_and_odsp(path_export: str, folder_name: str, dataset_id: str) -> None:
    """
    Updates a dataset by uploading it to an FTP server and publishing it into data.bs.ch.

    This function performs the following steps:
    1. Checks if the content of the dataset at the specified path has changed.
    2. If changes are detected, uploads the dataset to an FTP server using provided credentials.
    3. Publishes the dataset into data.bs.ch using the provided dataset ID.
    4. Updates the hash file to reflect the current state of the dataset.

    Args:
        path_export (str): The file path to the dataset that needs to be updated.
        folder_name (str): The name of the folder on the FTP server where the dataset should be uploaded.
        dataset_id (str): The ID of the dataset to be published on data.bs.ch.
    """
    if change_tracking.has_changed(path_export):
        common.upload_ftp(path_export, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, folder_name)
        odsp.publish_ods_dataset_by_id(dataset_id)
        change_tracking.update_hash_file(path_export)