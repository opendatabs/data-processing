import os
import logging
import pandas as pd
import icalendar
import charset_normalizer
import json
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from rapidfuzz import process, fuzz
from itertools import combinations
import pathlib

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
    current_entries = [dict(summary=event['SUMMARY'], dtstart=event['DTSTART'].dt, dtend=event['DTEND'].dt) for event in
                       calendar.walk('VEVENT') if
                       event['DTSTART'].dt - pd.Timedelta(hours=hours_before_start) <= now_in_switzerland <= event[
                           'DTEND'].dt + pd.Timedelta(hours=hours_after_end)]
    session_active = True if len(current_entries) > 0 else False
    logging.info(f'Session active now? {session_active}')
    return session_active


def get_trakt_names(session_day):
    ftp = {'server': credentials.gr_session_data_ftp_server, 'user': credentials.gr_session_data_ftp_user,
           'password': credentials.gr_session_data_ftp_pass}
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
    csv_file = common.download_ftp([], ftp['server'], ftp['user'], ftp['password'], closest_session_path,
                                   credentials.data_path, 'BSGR_AGENDA.csv')
    # if csv_file is empty, raise error
    if len(csv_file) == 0:
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
    ls = common.download_ftp([], ftp['server'], ftp['user'], ftp['password'], remote_path, credentials.local_data_path,
                             pattern, list_only=True)
    logging.info(f'Saving ftp ls file of pattern {pattern} to {file_name}...')
    json.dump(ls, open(file_name, 'w'), indent=1)
    return ls


# Function to create all combinations of names
def create_name_combinations(row, surname_first=False):
    # Remove commas and hyphens, and split names
    first_names = row['vorname'].replace('-', ' ').split()
    surnames = row['name'].replace('-', ' ').split()
    # Create all combinations of names
    first_name_combinations = [' '.join(comb) for r in range(1, len(first_names) + 1) for comb in
                               combinations(first_names, r)]
    surname_combinations = [' '.join(comb) for r in range(1, len(surnames) + 1) for comb in combinations(surnames, r)]
    # Create all combinations of first and last names
    if surname_first:
        name_combinations = [f'{surname} {first_name}' for surname in surname_combinations for first_name in
                             first_name_combinations]
    else:
        name_combinations = [f'{first_name} {surname}' for first_name in first_name_combinations for surname in
                             surname_combinations]
    return [
        {'comb_name_vorname': comb, 'name': row['name'], 'vorname': row['vorname'], 'name_vorname': row['name_vorname'],
         'uni_nr': row['uni_nr'], 'url': row['url']}
        for comb in name_combinations]


def fill_values_from_dataframe(df: pd.DataFrame, df_lookup: pd.DataFrame, index, index_lookup):
    df.loc[index, 'Mitglied_Nachname'] = df_lookup.loc[index_lookup, 'name']
    df.loc[index, 'Mitglied_Vorname'] = df_lookup.loc[index_lookup, 'vorname']
    df.loc[index, 'Mitglied_Name'] = df_lookup.loc[index_lookup, 'name_vorname']
    df.loc[index, 'GR_uni_nr'] = df_lookup.loc[index_lookup, 'uni_nr']
    df.loc[index, 'GR_url'] = df_lookup.loc[index_lookup, 'url']
    df.loc[index, 'GR_url_ods'] += df_lookup.loc[index_lookup, 'uni_nr'].astype(str)
    return df


def get_closest_name_from_member_dataset(df: pd.DataFrame, surname_first=False):
    # Create new columns
    df['Mitglied_Vorname'] = ''
    df['Mitglied_Nachname'] = ''
    df['GR_uni_nr'] = ''
    df['GR_url'] = ''
    df['GR_url_ods'] = 'https://data.bs.ch/explore/dataset/100307/?refine.uni_nr='
    # Download members of Grosser Rat from ods
    raw_data_file = os.path.join(credentials.data_path, 'members_gr.csv')
    logging.info(f'Downloading Members of Grosser Rat from ods to file {raw_data_file}...')
    r = common.requests_get(f'https://data.bs.ch/api/records/1.0/download?dataset=100307')
    with open(raw_data_file, 'wb') as f:
        f.write(r.content)
    df_gr_mitglieder = pd.read_csv(raw_data_file, sep=';')
    df_names = df_gr_mitglieder[['name', 'vorname', 'name_vorname', 'url', 'uni_nr']]
    # Create all combinations of names
    expanded_rows = [create_name_combinations(row, surname_first=surname_first) for index, row in df_names.iterrows()]
    expanded_df = pd.DataFrame([item for sublist in expanded_rows for item in sublist])
    name_list = expanded_df['comb_name_vorname'].tolist()
    path_lookup_table = os.path.join(pathlib.Path(__file__).parents[0], 'data', 'lookup_grossrat.csv')
    if os.path.exists(path_lookup_table):
        logging.info(f'Loading lookup table from {path_lookup_table}...')
        lookup_table = pd.read_csv(path_lookup_table)
    else:
        logging.info(f'Creating lookup table and saving to {path_lookup_table}...')
        lookup_table = pd.DataFrame(
            columns=['fuzzy_name', 'closest_combination', 'fuzz_score',
                     'name', 'vorname', 'name_vorname', 'uni_nr', 'url'])
    for index, row in df.iterrows():
        if row['Mitglied_Name'] in lookup_table['fuzzy_name'].tolist():
            index_lookup = lookup_table.loc[lookup_table['fuzzy_name'] == row['Mitglied_Name']].index[0]
        else:
            logging.info(f'Looking for closest name for {row["Mitglied_Name"]}...')
            closest_name, score, index_gr_mitglieder = process.extractOne(row['Mitglied_Name'], name_list,
                                                                          scorer=fuzz.WRatio)
            logging.info(f'Closest name for {row["Mitglied_Name"]} is {closest_name} with score {score}...')
            lookup_table.loc[-1] = [row['Mitglied_Name'], closest_name, score,
                                    expanded_df.loc[index_gr_mitglieder, 'name'],
                                    expanded_df.loc[index_gr_mitglieder, 'vorname'],
                                    expanded_df.loc[index_gr_mitglieder, 'name_vorname'],
                                    expanded_df.loc[index_gr_mitglieder, 'uni_nr'],
                                    expanded_df.loc[index_gr_mitglieder, 'url']]
            lookup_table.index = lookup_table.index + 1
            lookup_table.sort_index(inplace=True)
            index_lookup = lookup_table.index[0]
        df = fill_values_from_dataframe(df, lookup_table, index, index_lookup)
    # Save lookup table
    lookup_table.to_csv(path_lookup_table, index=False)
    return df
