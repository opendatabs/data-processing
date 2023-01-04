import ftplib
from pathlib import Path
import json
import logging
import os
import xml
import charset_normalizer
from datetime import datetime, timezone, timedelta
from xml.sax.handler import ContentHandler
from zoneinfo import ZoneInfo
import numpy as np
import ods_publish.etl_id as odsp
import icalendar
import pandas as pd
import common
import common.change_tracking as ct
from parlamentsdienst_gr_abstimmungen import credentials


# see https://stackoverflow.com/a/33504236
class ExcelHandler(ContentHandler):
    def __init__(self):
        super().__init__()
        self.chars = []
        self.cells = []
        self.rows = []
        self.tables = []

    def characters(self, content):
        self.chars.append(content)

    def startElement(self, name, atts):
        if name == "Cell":
            self.chars = []
        elif name == "Row":
            self.cells = []
        elif name == "Table":
            self.rows = []

    def endElement(self, name):
        if name == "Cell":
            self.cells.append(''.join(self.chars))
        elif name == "Row":
            self.rows.append(self.cells)
        elif name == "Table":
            self.tables.append(self.rows)


# see https://stackoverflow.com/a/65412797
def is_file_older_than(file, delta):
    cutoff = datetime.utcnow() - delta
    mtime = datetime.utcfromtimestamp(os.path.getmtime(file))
    return True if mtime < cutoff else False


def get_session_calendar(cutoff):
    logging.info(f'Checking if we should reload the ical file from the web...')
    ical_file_path = credentials.ics_file
    cal_export_file = os.path.join(credentials.local_data_path.replace('data_orig', 'data'), 'grosser_rat_sitzungskalender.csv')
    pickle_file_name = cal_export_file.replace('.csv', '.pickle')
    if not os.path.exists(ical_file_path) or not os.path.exists(pickle_file_name) or is_file_older_than(ical_file_path, cutoff):
        logging.info(f'Pickle or iCal file does not exist or is older than required - opening Google Calendar from web...')
        url = 'https://calendar.google.com/calendar/ical/vfb9bndssqs2v9uiun9uk7hkl8%40group.calendar.google.com/public/basic.ics'
        r = common.requests_get(url=url, allow_redirects=True)
        with open(ical_file_path, 'wb') as f:
            f.write(r.content)
        logging.info(f'Parsing events into df to publish to dataset...')
        calendar = icalendar.Calendar.from_ical(r.content)
        df_cal = pd.DataFrame(dict(summary=event['SUMMARY'], dtstart=event['DTSTART'].dt, dtend=event['DTEND'].dt) for event in calendar.walk('VEVENT'))
        logging.info(f'Saving session calendar as pickle and csv: {pickle_file_name}, {cal_export_file}...')
        df_cal.to_pickle(pickle_file_name)
        df_cal.to_csv(cal_export_file, index=False)
        if ct.has_changed(cal_export_file):
            common.upload_ftp(cal_export_file, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'parlamentsdienst/gr_sitzungskalender')
            odsp.publish_ods_dataset_by_id('100188')
            ct.update_hash_file(cal_export_file)
    else:
        logging.info(f'Reading session calendar from pickle {pickle_file_name}')
        df_cal = pd.read_pickle(pickle_file_name)
    return ical_file_path, df_cal


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


def find_in_sheet(sheet, text_to_find):
    return [(i, text.index(text_to_find)) for i, text in enumerate(sheet) if text_to_find in text]


def handle_polls(process_archive=False, df_unique_session_dates=None):
    logging.info(f'Handling polls, value of process_archive: {process_archive}...')
    df_to_return = None
    if process_archive:
        ftp = {'server': credentials.gr_polls_archive_ftp_server, 'user': credentials.gr_polls_archive_ftp_user, 'password': credentials.gr_polls_archive_ftp_pass}
        dir_ls_file = credentials.ftp_ls_file.replace('.json', f'_archive_dir.json')
        # xml and pdf Files are located in folders "Amtsjahr_????-????/????.??.??", e.g. "Amtsjahr_2022-2023/2022.10.19", so we dive into a
        # two folder deep file structure
        dir_ls = get_ftp_ls(remote_path='', pattern='Amtsjahr_*', file_name=dir_ls_file, ftp=ftp)
        all_df = pd.DataFrame()
        for remote_file in dir_ls:
            remote_path = remote_file['remote_file']
            subdir_ls_file = credentials.ftp_ls_file.replace('.json', f'_archive_{remote_path}.json')
            subdir_ls = get_ftp_ls(remote_path=remote_path, pattern='*.*.*', file_name=subdir_ls_file, ftp=ftp)
            for subdir in subdir_ls:
                if subdir['remote_file'] not in ['.', '..']:
                    remote_path_subdir = remote_path + '/' + subdir['remote_file']
                    poll_df = handle_single_polls_folder(df_unique_session_dates, ftp, process_archive, remote_path_subdir)
                    all_df = pd.concat(objs=[all_df, poll_df], sort=False)
        df_to_return = all_df
    else:
        ftp = {'server': credentials.gr_current_polls_ftp_server, 'user': credentials.gr_current_polls_ftp_user, 'password': credentials.gr_current_polls_ftp_pass}
        remote_path = ''
        df_to_return = handle_single_polls_folder(df_unique_session_dates, ftp, process_archive, remote_path)

    if len(df_to_return) > 0:
        file_name_part = 'archiv' if process_archive else 'aktuell'
        polls_filename = os.path.join(credentials.local_data_path.replace('data_orig', 'data'), f'grosser_rat_abstimmungen_{file_name_part}.csv')
        logging.info(f'Saving polls as a backup to {polls_filename}...')
        df_to_return.to_csv(polls_filename, index=False)
        common.upload_ftp(polls_filename, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'parlamentsdienst/gr_abstimmungsergebnisse')
    return df_to_return


def handle_single_polls_folder(df_unique_session_dates, ftp, process_archive, remote_path):
    xml_ls_file = credentials.ftp_ls_file.replace('.json', f'_xml_{remote_path.replace("/", "_")}.json')
    xml_ls = get_ftp_ls(remote_path=remote_path, pattern='*.xml', file_name=xml_ls_file, ftp=ftp)
    df_trakt_filenames = retrieve_traktanden_pdf_filenames(ftp, remote_path)
    all_df = pd.DataFrame()
    # todo: Parse every poll pdf file name to check for the new type "un" (ungültig) and set those polls' type correctly.
    # todo: Check for changes in PDF files, only those change if a poll is invalid (xml file does not change)
    # Renaming of a pdf file to type "un" can happen after session, so we have to check for changes in the poll pdf files even if no change to the poll xml file has happened.
    if process_archive or ct.has_changed(xml_ls_file):
        # todo: handle xlsx files of polls during time at congress center
        xml_files = common.download_ftp([], ftp['server'], ftp['user'], ftp['password'], remote_path, credentials.local_data_path, '*.xml')
        df_trakt = calc_traktanden_from_pdf_filenames(df_trakt_filenames)
        for i, file in enumerate(xml_files):
            local_file = file['local_file']
            logging.info(f'Processing file {i} of {len(xml_files)}: {local_file}...')
            if process_archive or ct.has_changed(local_file):
                df_poll_details = calc_details_from_single_xml_file(local_file)
                df_merge1 = df_poll_details.merge(df_trakt, how='left', on=['session_date', 'Abst_Nr'])
                # Overriding invalid polls: Their pdf file name contains 'un' in column 'Abst_Typ'
                df_merge1['Typ'] = np.where(df_merge1['Abst_Typ'] == 'un', 'ungültig', df_merge1['Typ'])
                df_merge1 = df_merge1.drop(columns=['Abst_Typ'])
                df_merge1['tagesordnung_link'] = 'https://data.bs.ch/explore/dataset/100190/table/?refine.datum=' + df_merge1.Datum + '&refine.traktand=' + df_merge1.Traktandum.astype(str)
                # todo: Add link to pdf file (if possible)
                # Correct historical incidence of wrong seat number 182 (2022-03-17)
                df_merge1.loc[df_merge1.Sitz_Nr == '182', 'Sitz_Nr'] = '60'
                # Remove test polls: (a) polls outside of session days --> done by inner-joining session calendar with abstimmungen
                df_merge2 = df_unique_session_dates.merge(df_merge1, on=['session_date'], how='inner')
                # Remove test polls: (b) polls during session day but with a certain poll type ("Testabstimmung" or similar) --> none detected in whole archive

                curr_poll_df = df_merge2

                # {"session_date":"20141119","Abst_Nr":"745","Datum":"2014-11-19","Zeit":"09:24:56.000","Anz_J":"39","Anz_N":"47","Anz_E":"6","Anz_A":"7","Anz_P":"1","Typ":"Abstimmung","Geschaeft":"Anzug Otto Schmid und Konsorten betreffend befristetes, kostenloses U-Abo bei freiwilliger Abgabe des F\u00fchrerausweises","Zeitstempel_text":"2014-11-19T09:24:56.000000+0100","Sitz_Nr":"1","Mitglied_Name":"Beatriz Greuter","Fraktion":"SP","Mitglied_Name_Fraktion":"Beatriz Greuter (SP)","Datenstand_text":"2022-03-17T12:19:35+01:00","Entscheid_Mitglied":"J","Traktandum":16,"Subtraktandum":3,"tagesordnung_link":"https:\/\/data.bs.ch\/explore\/dataset\/100190\/table\/?refine.datum=2014-11-19&refine.traktand=16"}
                common.ods_realtime_push_df(curr_poll_df, credentials.push_url)
                export_filename_csv = local_file.replace('data_orig', 'data').replace('.xml', '.csv')
                logging.info(f'Saving data files to FTP server as backup: {local_file}, {export_filename_csv}')
                common.upload_ftp(local_file, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'parlamentsdienst/gr_abstimmungsergebnisse')
                curr_poll_df.to_csv(export_filename_csv, index=False)
                common.upload_ftp(export_filename_csv, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'parlamentsdienst/gr_abstimmungsergebnisse')
                all_df = pd.concat(objs=[all_df, curr_poll_df], sort=False)
                ct.update_hash_file(local_file)
        ct.update_hash_file(xml_ls_file)
    return all_df


def get_unique_session_dates(df_cal):
    # df_cal['start_date'] = df_cal.dtstart.dt.strftime(date_format='%Y-%m-%d')
    # df_cal['end_date'] = df_cal.dtend.dt.strftime(date_format='%Y-%m-%d')
    # df_cal.query('start_date != end_date') --> none found, thus use start_date
    logging.info(f'Calculating unique session dates used to filter out test polls...')
    df_cal['session_date'] = df_cal.dtstart.dt.strftime(date_format='%Y%m%d')
    df_unique_cal_dates = df_cal.drop_duplicates(subset=['session_date'])[['session_date']]
    return df_unique_cal_dates


def calc_tagesordnungen_from_txt_files(process_archive=False):
    txt_ls_file_name = credentials.ftp_ls_file.replace('.json', '_txt.json')
    pattern = '*traktanden_col4.txt'
    txt_ls = get_ftp_ls(remote_path='', pattern=pattern, ftp={'server': credentials.gr_trakt_list_ftp_server, 'user': credentials.gr_trakt_list_ftp_user, 'password': credentials.gr_polls_ftp_pass}, file_name=txt_ls_file_name)
    pickle_file_name = os.path.join(credentials.local_data_path.replace('data_orig', 'data'), 'gr_tagesordnung.pickle')
    logging.info(f'Value of process_archive: {process_archive}')
    df_all = None
    if os.path.exists(pickle_file_name) and not process_archive and not ct.has_changed(txt_ls_file_name):
        logging.info(f'Reading tagesordnung data from pickle {pickle_file_name}...')
        df_all = pd.read_pickle(pickle_file_name)
    else:
        # todo: Only download changed files
        tagesordnung_files = common.download_ftp([], credentials.gr_trakt_list_ftp_server, credentials.gr_trakt_list_ftp_user, credentials.gr_trakt_list_ftp_pass, '', credentials.local_data_path, pattern)

        dfs = []
        for file in tagesordnung_files:
            logging.info(f"Cleaning file and reading into df: {file['local_file']}")

            def tidy_fn(txt: str):
                return (txt
                        .replace('\nPartnerschaftliches Geschäft', '\tPartnerschaftliches Geschäft')
                        .replace('\nJSSK', '\tJSSK')
                        .replace('\n17.5250.03', '\t17.5250.03')
                        .replace('IGPK Rhein-häfen', 'IGPK Rheinhäfen')
                        .replace('IGPK Rhein- häfen', 'IGPK Rheinhäfen')
                        .replace('IGPK Uni-versität', 'IGPK Universität')
                        .replace('IGPK Univer-sität', 'IGPK Universität')
                        .replace('Rats-büro', 'Ratsbüro')
                        .replace('00.0000.00', '\t00.0000.00')
                        .replace('12.2035.01', '\t12.2035.01')
                        .replace('16.5326.01', '\t16.5326.01')
                        .replace('16.5327.01', '\t16.5327.01')
                        .replace('17.0552.03', '\t17.0552.03')
                        .replace('FD\t18.5143.02', '\tFD\t18.5143.02')
                        .replace('18.5194.01', '\t18.5194.01')
                        .replace('19.5040.02', '\t19.5040.02')
                        .replace('19.5063.01', '\t19.5063.01')
                        .replace('21.0546.01', '\t21.0546.01')
                        .replace('\t18.0321.01', '18.0321.01')
                        .replace('\t18.0616.02', '18.0616.02')
                        .replace('\t17.5250.03', '17.5250.03')
                        .replace('\t18.1319.02', '18.1319.02')
                        .replace('NIM18.', 'NIM\n18.')
                        .replace('NIM17.', 'NIM\n17.')
                        .replace('NIS15.', 'NIS\n15.')
                        )
            tidy_file_name = tidy_file(file['local_file'], tidy_fn)
            df = pd.read_csv(tidy_file_name, delimiter='\t', encoding='cp1252', on_bad_lines='error', skiprows=1,
                             names=['traktand', 'title', 'commission', 'department', 'geschnr', 'info', 'col_06', 'col_07', 'col_08', 'col_09', 'col_10'],
                             dtype={'traktand': 'str', 'title': 'str', 'commission': 'str', 'department': 'str', 'geschnr': 'str', 'info': 'str', 'col_06': 'str', 'col_07': 'str', 'col_08': 'str', 'col_09': 'str', 'col_10': 'str'}
                             )
            session_date = file['remote_file'].split('_')[0]
            df['session_date'] = session_date
            df['Datum'] = session_date[:4] + '-' + session_date[4:6] + '-' + session_date[6:8]
            # remove leading and trailing characters
            df.traktand = df.traktand.str.rstrip('. ')
            df.commission = df.commission.str.lstrip(' ')
            df.department = df.department.str.strip(' ')
            df.traktand = df.traktand.fillna(method='ffill')
            # todo: Handle mutliple geschnr
            df['geschaeftsnr0'] = df.geschnr.str.split('.', expand=False).str.get(0)
            df['geschaeftsnr1'] = df.geschnr.str.split('.', expand=False).str.get(1)
            df['geschaeftsnr2'] = df.geschnr.str.split('.', expand=False).str.get(2)
            df['geschaeftsnr'] = df.geschaeftsnr0 + '.' + df.geschaeftsnr1
            df['dokumentnr'] = df.geschaeftsnr0 + '.' + df.geschaeftsnr1 + '.' + df.geschaeftsnr2
            df['geschaeft-url'] = 'https://grosserrat.bs.ch/?idurl=' + df.geschaeftsnr
            df['dokument-url'] = 'https://grosserrat.bs.ch/?doknr=' + df.dokumentnr
            # Save pickle to be loaded and returned if no changes in files detected
            logging.info(f'Saving tagesordnung df to pickle {pickle_file_name}...')
            df.to_pickle(pickle_file_name)
            dfs.append(df)
        df_all = pd.concat(dfs)
        ct.update_hash_file(txt_ls_file_name)
    return df_all


def calc_details_from_single_xml_file(local_file):
    session_date = os.path.basename(local_file).split('_')[0]
    excel_handler = ExcelHandler()

    def tidy_fn(txt: str):
        return txt.replace('&', '+')
    xml.sax.parse(tidy_file(local_file, tidy_fn), excel_handler)
    # xml.sax.parse(tidy_xml(local_file), excel_handler)
    sheet_abstimmungen = excel_handler.tables[1]
    polls = pd.DataFrame(sheet_abstimmungen[1:], columns=sheet_abstimmungen[0])
    sheet_resultate = excel_handler.tables[0]
    # Not all sheets are the same of course, thus we have to find blocks of data using specific text
    ja_row = find_in_sheet(sheet_resultate, 'Ja')[0][0]
    details = pd.DataFrame(sheet_resultate[1:ja_row], columns=sheet_resultate[0])
    # Find 'Ja' and take the 7 rows starting there.
    sums_per_decision = pd.DataFrame(sheet_resultate[ja_row:ja_row + 6], columns=sheet_resultate[0])
    datenexport_row = find_in_sheet(sheet_resultate, 'Datenexport')[0][0]
    data_timestamp = datetime.strptime(sheet_resultate[datenexport_row + 1][1], '%Y-%m-%dT%H:%M:%S').replace(tzinfo=ZoneInfo('Europe/Zurich'))
    # Add timezone, then convert to UTC for ODS
    polls['Zeitstempel'] = pd.to_datetime(polls.Zeit, format='%Y-%m-%dT%H:%M:%S.%f').dt.tz_localize('Europe/Zurich').dt.tz_convert('UTC')
    polls['Zeitstempel_text'] = polls.Zeitstempel.dt.strftime(date_format='%Y-%m-%dT%H:%M:%S.%f%z')
    polls[['Datum', 'Zeit']] = polls.Datum.str.split('T', expand=True)
    polls = polls.rename(columns={'Nr': 'Abst_Nr', 'J': 'Anz_J', 'N': 'Anz_N', 'E': 'Anz_E', 'A': 'Anz_A', 'P': 'Anz_P'})
    details['Datum'] = session_date[:4] + '-' + session_date[4:6] + '-' + session_date[6:8]
    details.columns.values[0] = 'Sitz_Nr'
    details.columns.values[1] = 'Mitglied_Name_Fraktion'
    # Replace multiple spaces with single space
    details.Mitglied_Name_Fraktion = details.Mitglied_Name_Fraktion.str.replace(r'\s+', ' ', regex=True)
    # Get the text in between ( and ) as Fraktion
    details['Fraktion'] = details.Mitglied_Name_Fraktion.str.extract(r"\(([^)]+)\)", expand=False)
    # Get the text before ( as Mitglied_Name
    details['Mitglied_Name'] = details.Mitglied_Name_Fraktion.str.split('(', expand=True)[[0]].squeeze().str.strip()
    details['Datenstand'] = pd.to_datetime(data_timestamp.isoformat())
    details['Datenstand_text'] = data_timestamp.isoformat()
    # See usage of Document id e.g. here: http://abstimmungen.grosserrat-basel.ch/index_archiv3_v2.php?path=archiv/Amtsjahr_2022-2023/2022.03.23
    # See document details e.g. here: https://grosserrat.bs.ch/ratsbetrieb/geschaefte/200111156
    details_long = details.melt(id_vars=['Sitz_Nr', 'Mitglied_Name', 'Fraktion', 'Mitglied_Name_Fraktion', 'Datum', 'Datenstand', 'Datenstand_text'], var_name='Abst_Nr', value_name='Entscheid_Mitglied')
    df_merge1 = polls.merge(details_long, how='left', on=['Datum', 'Abst_Nr'])
    df_merge1['session_date'] = session_date  # Only used for joining with df_trakt
    return df_merge1


def calc_traktanden_from_pdf_filenames(df_trakt):
    if len(df_trakt) > 0:
        logging.info(f'Calculating traktanden from pdf filenames...')
        df_trakt[['Abst', 'Abst_Nr', 'session_date', 'Zeit', 'Traktandum', 'Subtraktandum', '_Abst_Typ']] = df_trakt.remote_file.str.split('_', expand=True)
        df_trakt[['Abst_Typ', 'file_ext']] = df_trakt['_Abst_Typ'].str.split('.', expand=True)
        # Remove spaces in filename, get rid of leading zeros.
        df_trakt.Abst_Nr = df_trakt.Abst_Nr.str.replace(' ', '').astype(int).astype(str)
        df_trakt.Traktandum = df_trakt.Traktandum.astype(int)
        # Get rid of some rogue text and leading zeros
        # todo: Keep this as text in order not to fail on live imports?
        df_trakt.Subtraktandum = df_trakt.Subtraktandum.replace('Interpellationen Nr', '0', regex=False).replace('Interpellation Nr', '0', regex=False).astype(int)
        df_trakt = df_trakt[['session_date', 'Abst_Nr', 'Traktandum', 'Subtraktandum', 'Abst_Typ']]
    return df_trakt


def retrieve_traktanden_pdf_filenames(ftp, remote_path):
    logging.info(f'Retrieving traktanden PDF filenames')
    pdf_ls_file = credentials.ftp_ls_file.replace('.json', '_pdf.json')
    pdf_ls = get_ftp_ls(remote_path=remote_path, pattern='*.pdf', file_name=pdf_ls_file, ftp=ftp)
    df_trakt = pd.DataFrame(pdf_ls)
    return df_trakt


def get_ftp_ls(remote_path, pattern, file_name, ftp):
    ls = common.download_ftp([], ftp['server'], ftp['user'], ftp['password'], remote_path, credentials.local_data_path, pattern, list_only=True)
    logging.info(f'Saving ftp ls file of pattern {pattern} to {file_name}...')
    json.dump(ls, open(file_name, 'w'), indent=1)
    return ls


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


def handle_tagesordnungen(process_archive=False):
    df_tagesordnungen = calc_tagesordnungen_from_txt_files(process_archive)
    tagesordnungen_export_file_name = os.path.join(credentials.local_data_path.replace('data_orig', 'data'), 'grosser_rat_tagesordnungen.csv')
    df_tagesordnungen.to_csv(tagesordnungen_export_file_name, index=False)
    if process_archive or ct.has_changed(tagesordnungen_export_file_name):
        common.upload_ftp(tagesordnungen_export_file_name, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'parlamentsdienst/gr_tagesordnungen')
        odsp.publish_ods_dataset_by_id('100190')
        ct.update_hash_file(tagesordnungen_export_file_name)
    return df_tagesordnungen


def handle_congress_center_polls(df_unique_session_dates):
    cc_files = [file for file in Path(f'{credentials.local_data_path}/congress_center').rglob('GR-BS_??????-?.xlsx')]
    all_cc_data_files = []
    for cc_file in cc_files:
        logging.info(f'Reading File {cc_file} into df...')
        df = pd.read_excel(cc_file, usecols='B:I,K')
        cc_data_file_name = os.path.basename(cc_file)
        df['file_name'] = cc_data_file_name
        logging.info(f'Calculating columns to fit target scheme...')
        df['session_date'] = df['Creation Date'].dt.strftime('%Y%m%d')
        df['Abst_Nr'] = df['Current Voting ID']
        df['Datum'] = df['Creation Date'].dt.strftime('%Y-%m-%d')
        df['Zeit'] = df['Creation Date'].dt.strftime('%H:%M:%S.%f')
        df['Typ'] = 'Abstimmung'
        df['Geschaeft'] = df['Headline Text']
        df['Zeitstempel'] = df['Creation Date'].dt.tz_localize('Europe/Zurich')
        df['Zeitstempel_text'] = df.Zeitstempel.dt.strftime(date_format='%Y-%m-%dT%H:%M:%S.%f%z')
        df['Sitz_Nr'] = df['Handset ID'] - 300
        df['Fraktion'] = df['Vote Group']
        df['Datenstand'] = df.Zeitstempel
        df['Datenstand_text'] = df.Zeitstempel_text

        df_names = df.Name.str.replace('von ', 'von_').str.split(' ', n=1, expand=True)
        df_names.columns = ['Nachname', 'Vorname']
        df_names.Nachname = df_names.Nachname.str.replace('von_', 'von ')
        df_names['Mitglied_Name'] = df_names.Vorname + ' ' + df_names.Nachname
        df_names['Mitglied_Name'] = df_names.Mitglied_Name.str.replace('\\xa0', '', regex=False)
        df['Mitglied_Name'] = df_names.Mitglied_Name
        df['Mitglied_Name_Fraktion'] = df.Mitglied_Name + ' (' + df.Fraktion + ')'
        df['Entscheid_Mitglied'] = df['Choice Text'].replace({'Ja': 'J', 'Nein': 'N', '-': 'A', 'Enthaltung': 'E'})

        df_trakt = df.Geschaeft.str.extract(r"Trakt\. (?P<Traktandum>\d+)[\_\:](?P<Subtraktandum>\d+)?")
        df = pd.concat([df, df_trakt], axis=1)
        df.Traktandum = df.Traktandum.fillna('')
        df.Subtraktandum = df.Subtraktandum.fillna('')
        df['tagesordnung_link'] = 'https://data.bs.ch/explore/dataset/100190/table/?refine.datum=' + df.Datum + '&refine.traktand=' + df.Traktandum

        df_poll_counts = df.groupby(['Current Voting ID', 'file_name', 'Choice Text'])['Handset ID'].count().reset_index(name='Anzahl')
        df_poll_counts_pivot = df_poll_counts.pivot_table('Anzahl', ['Current Voting ID', 'file_name'], 'Choice Text').reset_index().rename(columns={'-': 'Anz_A', 'Enthaltung': 'Anz_E', 'Ja': 'Anz_J', 'Nein': 'Anz_N'})
        df = df.merge(df_poll_counts_pivot, how='left', on=['file_name', 'Current Voting ID'])

        # {"session_date": "20141119", "Abst_Nr": "745", "Datum": "2014-11-19", "Zeit": "09:24:56.000", "Anz_J": "39", "Anz_N": "47", "Anz_E": "6", "Anz_A": "7", "Anz_P": "1", "Typ": "Abstimmung", "Geschaeft": "Anzug Otto Schmid und Konsorten betreffend befristetes, kostenloses U-Abo bei freiwilliger Abgabe des F\u00fchrerausweises", "Zeitstempel_text": "2014-11-19T09:24:56.000000+0100", "Sitz_Nr": "1", "Mitglied_Name": "Beatriz Greuter", "Fraktion": "SP", "Mitglied_Name_Fraktion": "Beatriz Greuter (SP)", "Datenstand_text": "2022-03-17T12:19:35+01:00", "Entscheid_Mitglied": "J", "Traktandum": 16, "Subtraktandum": 3, "tagesordnung_link": "https:\/\/data.bs.ch\/explore\/dataset\/100190\/table\/?refine.datum=2014-11-19&refine.traktand=16"}
        # We don't have column 'Anz_P' in Congress Center data, so we don't push it
        columns_to_export = ["session_date", "Abst_Nr", "Datum", "Zeit", "Anz_J", "Anz_N", "Anz_E", "Anz_A", "Typ", "Geschaeft", "Zeitstempel_text", "Sitz_Nr", "Mitglied_Name", "Fraktion", "Mitglied_Name_Fraktion", "Datenstand_text", "Entscheid_Mitglied", "Traktandum", "Subtraktandum", "tagesordnung_link"]
        df_export = df[columns_to_export]
        cc_export_file = os.path.join(credentials.local_data_path.replace('data_orig', 'data'), cc_data_file_name.replace('.xlsx', '.csv'))
        df_export.to_csv(cc_export_file, index=False)
        common.ods_realtime_push_df(df_export, credentials.push_url)
        logging.info(f'Saving data files to FTP server as backup: {cc_export_file}...')
        common.upload_ftp(cc_export_file, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'parlamentsdienst/gr_abstimmungsergebnisse')

        all_cc_data_files.append(df)
    all_cc_data = pd.concat(all_cc_data_files)
    return all_cc_data


def main():
    # df_tagesordn = handle_tagesordnungen(process_archive=False)
    ical_file_path, df_cal = get_session_calendar(cutoff=timedelta(hours=12))
    df_unique_session_dates = get_unique_session_dates(df_cal)
    # Uncomment to process Congress Center data
    # poll_congress_center_archive = handle_congress_center_polls(df_unique_session_dates=None)
    # Uncomment to process archived poll data
    # poll_archive_df = handle_polls(process_archive=True, df_unique_session_dates=df_unique_session_dates)

    if is_session_now(ical_file_path, hours_before_start=4, hours_after_end=10):
        poll_current_df = handle_polls(process_archive=False, df_unique_session_dates=df_unique_session_dates)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info(f'Job completed successfully!')


# This job processes the following data sources:
# - Tagesordnungen (Traktandenlisten) - handle_tagesordnungen():
#   - contents of 1 *.txt (csv) file for each session day: contains details of each Traktandum, which can be linked to a single poll, to none, one or multiple Geschäfte and Dokumente
#   - 1 folder of csv files that contain all past and present Tagesordnungen
#   - file for current session may be present only after first poll of session has been completed
# - Session calendar - get_session_calendar():
#   - iCal retrieved from Google Calendar, 1 entry per session day
# - Live Polls from FTP Server - handle_polls(process_archive=False)
#   - contents of 1 xml file per session: contain each individual Grossratsmitglied's decision for each Traktandum
#       - calc_details_from_single_xml_file()
#   - filename of 1 pdf file per poll: contains Traktandum and Subtraktandum for each single poll
#       - retrieve_traktanden_pdf_filenames()
#       - calc_traktanden_from_pdf_filenames()
# - Past polls from FTP Server - handle_polls(process_archive=True)
#   - contents and filenames contain same as live polls, but for past polls
#   - 1 folder with subfolder structure for pdf file
#   - 1 flat folder for all xml files
#   - 1 flat folder for xlsx files for the time the Grosser Rat held session at Congress Center Basel
