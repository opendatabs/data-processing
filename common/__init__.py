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
from common import credentials
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
    logging.info(f'Connecting to FTP Server "{server}" in path "{remote_path}" to download file(s) "{files}" or pattern "{pattern}" to local path "{local_path}"...')
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
def publish_ods_dataset(dataset_uid, creds):
    logging.info("Telling OpenDataSoft to reload dataset " + dataset_uid + '...')
    response = requests.put('https://data.bs.ch/api/management/v2/datasets/' + dataset_uid + '/publish', params={'apikey': creds.api_key}, proxies=credentials.proxies)
    if not response.ok:
        logging.info(f'Received http error {response.status_code}:')
        logging.info(f'Error message: {response.text}')
        r_json = response.json()
        # current_status = r_json['raw_params']['current_status']
        error_key = r_json['error_key']
        status_code = r_json['status_code']
        if status_code == 400 and error_key == 'InvalidDatasetStatusPreconditionException':
            logging.info(f'ODS returned status 400 and error_key "{error_key}", we raise the error now.')
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
    logging.info(f'Retrieving ods uid for ods id {ods_id}...')
    response = requests_get(url=f'https://data.bs.ch/api/management/v2/datasets/?where=datasetid="{ods_id}"', auth=(creds.user_name, creds.password))
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


def ods_realtime_push_df(df, url, push_key=''):
    if not push_key:
        t = url.partition('?pushkey=')
        url = t[0]
        push_key = t[2]
    row_count = len(df)
    if row_count == 0:
        logging.info(f'No rows to push to ODS... ')
    else:
        logging.info(f'Pushing {row_count} rows to ODS realtime API...')
        payload = df.to_json(orient="records")
        # logging.info(f'Pushing the following data to ODS: {json.dumps(json.loads(payload), indent=4)}')
        # use data=payload here because payload is a string. If it was an object, we'd have to use json=payload.
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


def get_text_from_url(url):
    req = requests_get(url)
    req.raise_for_status()
    return io.stringIO(req.text)
