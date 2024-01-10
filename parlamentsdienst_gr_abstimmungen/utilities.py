import os
import logging
import pandas as pd
import icalendar
import charset_normalizer
import json
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import common
from parlamentsdienst_gr_abstimmungen import credentials


# see https://stackoverflow.com/a/65412797
def is_file_older_than(file, delta):
    cutoff = datetime.utcnow() - delta
    mtime = datetime.utcfromtimestamp(os.path.getmtime(file))
    return True if mtime < cutoff else False


def find_in_sheet(sheet, text_to_find):
    return [(i, text.index(text_to_find)) for i, text in enumerate(sheet) if text_to_find in text]


def is_session_now(ical_file_path, hours_before_start, hours_after_end):
    # see https://stackoverflow.com/a/26329138
    now_in_switzerland = datetime.now(timezone.utc).astimezone(ZoneInfo('Europe/Zurich'))
    # now_in_switzerland = datetime(2022, 3, 23, 10, 15, 12, 11).astimezone(ZoneInfo('Europe/Zurich'))
    with open(ical_file_path, 'rb') as f:
        calendar = icalendar.Calendar.from_ical(f.read())
    # handle case where session takes longer than defined in calendar event
    current_entries = [dict(summary=event['SUMMARY'], dtstart=event['DTSTART'].dt, dtend=event['DTEND'].dt) for event in calendar.walk('VEVENT') if event['DTSTART'].dt - pd.Timedelta(hours=hours_before_start) <= now_in_switzerland <= event['DTEND'].dt + pd.Timedelta(hours=hours_after_end)]
    session_active = True if len(current_entries) > 0 else False
    logging.info(f'Session active now? {session_active}')
    return session_active


def get_trakt_names(session_day):
    ftp = {'server': credentials.gr_session_data_ftp_server, 'user': credentials.gr_session_data_ftp_user, 'password': credentials.gr_session_data_ftp_pass}
    dir_ls_file = credentials.ftp_ls_file.replace('.json', f'_session_data_dir.json')
    dir_ls = get_ftp_ls(remote_path='', pattern='*-*-*', file_name=dir_ls_file, ftp=ftp)
    # Iterate over directory and find closest past session date
    closest_session_path = None
    closest_session_date = None
    for session in dir_ls:
        session_str = session['remote_file']
        session_datetime = datetime.strptime(session_str, '%Y-%m-%d')
        diff_sessions = session_day - session_datetime
        if diff_sessions.days >= 0 and (closest_session_date is None or diff_sessions.days < closest_session_date.days):
            closest_session_date = diff_sessions
            closest_session_path = session_str
    if closest_session_path is None:
        raise ValueError(f'No session found for date {session_day}')
    logging.info(f'Found closest session date {closest_session_path} for date {session_day}')
    # Return BSGR_Agenda.csv saved in closest_session_path as pandas Dataframe
    agenda_save_path = credentials.ftp_ls_file.replace('ftp_listing.json', '')
    csv_file = common.download_ftp([], ftp['server'], ftp['user'], ftp['password'], closest_session_path, agenda_save_path, 'BSGR_Agenda.csv')
    if csv_file[0] is None:
        raise ValueError(f'No BSGR_Agenda.csv found for date {session_day}')
    return pd.read_csv(csv_file[0]['local_file'], delimiter=';')


def simplify_filename_json(filename, remote_file):
    # Find the last underscore and the '.json' extension
    last_underscore_index = filename.rfind('_')
    extension_index = filename.rfind('.json')
    # If no underscore in remote_file, return filename
    if remote_file.rfind('_') == -1:
        return filename
    return filename[:last_underscore_index] + filename[extension_index:]


# See https://codereview.stackexchange.com/q/232647
def recursive_mlsd(ftp_object, path="", maxdepth=None):
    """Run the FTP's MLSD command recursively

    The MLSD is returned as a list of tuples with (name, properties) for each
    object found on the FTP server. This function adds the non-standard
    property "children" which is then again an MLSD listing, possibly with more
    "children".

    Parameters
    ----------
    ftp_object: ftplib.FTP or ftplib.FTP_TLS
        the (authenticated) FTP client object used to make the calls to the
        server
    path: str
        path to start the recursive listing from
    maxdepth: {None, int}, optional
        maximum recursion depth, has to be >= 0 or None (i.e. no limit).

    Returns
    -------
    list
        the recursive directory listing

    See also
    --------
    ftplib.FTP.mlsd : the non-recursive version of this function
    """
    file_list = []
    if maxdepth is not None:
        maxdepth = int(maxdepth)
        if maxdepth < 0:
            raise ValueError("maxdepth is supposed to be >= 0")

    def _inner(path_, depth_):
        if maxdepth is not None and depth_ > maxdepth:
            return
        inner_mlsd = list(ftp_object.mlsd(path=path_))
        for name, properties in inner_mlsd:
            if properties["type"] == "dir":
                rec_path = path_ + "/" + name if path_ else name
                logging.info(f'Recursing into {rec_path}...')
                res = _inner(rec_path, depth_ + 1)
                if res is not None:
                    properties["children"] = res
            else:
                if name not in ['.', '..']:
                    file_list.append(name)
        return inner_mlsd, file_list

    return _inner(path, 0), file_list


def tidy_file(file_name, tidy_fn):
    """Data cleaning"""
    with open(file_name, 'rb') as f:
        raw_data = f.read()
        result = charset_normalizer.detect(raw_data)
        enc = result['encoding']
    with open(file_name, 'r', encoding=enc) as f:
        raw_txt = f.read()
    cleaned_txt = tidy_fn(raw_txt)
    filename, ext = os.path.splitext(file_name)
    clean_file = file_name.replace(ext, f'_clean{ext}')
    with open(clean_file, 'w', encoding=enc) as f:
        f.write(cleaned_txt)
    return clean_file


def get_ftp_ls(remote_path, pattern, file_name, ftp):
    ls = common.download_ftp([], ftp['server'], ftp['user'], ftp['password'], remote_path, credentials.local_data_path, pattern, list_only=True)
    logging.info(f'Saving ftp ls file of pattern {pattern} to {file_name}...')
    json.dump(ls, open(file_name, 'w'), indent=1)
    return ls
