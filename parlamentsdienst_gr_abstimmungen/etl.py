import json
import logging
import os
import xml
from datetime import datetime, timezone, timedelta
from xml.sax.handler import ContentHandler
from zoneinfo import ZoneInfo
# import ods_publish.etl_id as odsp
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


def main():
    if is_session_now():
        handle_polls()


# see https://stackoverflow.com/a/65412797
def is_file_older_than(file, delta):
    cutoff = datetime.utcnow() - delta
    mtime = datetime.utcfromtimestamp(os.path.getmtime(file))
    return True if mtime < cutoff else False


def is_session_now(cutoff=timedelta(hours=12)):
    logging.info(f'Checking if we should reload the ical file from the web...')
    ical_file_path = credentials.ics_file
    if not os.path.exists(ical_file_path) or is_file_older_than(ical_file_path, cutoff):
        logging.info(f'Ical file does not exist or is older than required - opening Google Calendar from web...')
        url = 'https://calendar.google.com/calendar/ical/vfb9bndssqs2v9uiun9uk7hkl8%40group.calendar.google.com/public/basic.ics'
        r = common.requests_get(url=url, allow_redirects=True)
        with open(ical_file_path, 'wb') as f:
            f.write(r.content)
    # see https://stackoverflow.com/a/26329138
    now_in_switzerland = datetime.now(timezone.utc).astimezone(ZoneInfo('Europe/Zurich'))
    # todo: remove this test datetime
    now_in_switzerland = datetime(2022, 3, 23, 10, 15, 12, 11).astimezone(ZoneInfo('Europe/Zurich'))
    with open(ical_file_path, 'rb') as f:
        calendar = icalendar.Calendar.from_ical(f.read())
    # all_entries = [dict(summary=event['SUMMARY'], dtstart=event['DTSTART'].dt, dtend=event['DTEND'].dt) for event in calendar.walk('VEVENT')]
    # handle case where session takes longer than defined in calendar event
    hours_before_start = 4
    hours_after_end = 10
    current_entries = [dict(summary=event['SUMMARY'], dtstart=event['DTSTART'].dt, dtend=event['DTEND'].dt) for event in calendar.walk('VEVENT') if event['DTSTART'].dt - pd.Timedelta(hours=hours_before_start) <= now_in_switzerland <= event['DTEND'].dt + pd.Timedelta(hours=hours_after_end)]
    session_active = True if len(current_entries) > 0 else False
    logging.info(f'Session active now? {session_active}')
    return session_active


def handle_polls():
    ftp_ls = common.download_ftp([], credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, '', credentials.local_data_path, '*.xml', list_only=True)
    logging.info(f'Saving ftp ls file to {credentials.ftp_ls_file}...')
    json.dump(ftp_ls, open(credentials.ftp_ls_file, 'w'), indent=1)
    if ct.has_changed(credentials.ftp_ls_file, do_update_hash_file=False):
        xml_files = common.download_ftp([], credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, '', credentials.local_data_path, '*.xml')
        for file in xml_files:
            local_file = file['local_file']
            if ct.has_changed(local_file, do_update_hash_file=False):
                session_date = os.path.basename(local_file).split('_')[0]
                excel_handler = ExcelHandler()
                xml.sax.parse(local_file, excel_handler)
                polls = pd.DataFrame(excel_handler.tables[1][1:], columns=excel_handler.tables[1][0])
                details = pd.DataFrame(excel_handler.tables[0][1:101], columns=excel_handler.tables[0][0])
                sums_per_decision = pd.DataFrame(excel_handler.tables[0][101:107], columns=excel_handler.tables[0][0])
                data_timestamp = datetime.strptime(excel_handler.tables[0][108][1], '%Y-%m-%dT%H:%M:%S').astimezone(ZoneInfo('Europe/Zurich'))

                polls['Zeitstempel_text'] = polls.Zeit
                polls['Zeitstempel'] = pd.to_datetime(polls.Zeit, format='%Y-%m-%dT%H:%M:%S.%f').dt.tz_localize('Europe/Zurich')
                polls[['Datum', 'Zeit']] = polls.Datum.str.split('T', expand=True)
                polls = polls.rename(columns={'Nr': 'Abst_Nr', 'J': 'Anz_J', 'N': 'Anz_N', 'E': 'Anz_E', 'A': 'Anz_A', 'P': 'Anz_P'})

                details['Datum'] = session_date[:4] + '-' + session_date[4:6] + '-' + session_date[6:8]
                details.columns.values[0] = 'Sitz_Nr'
                details.columns.values[1] = 'Mitglied_Name_Fraktion'
                details['Fraktion'] = details.Mitglied_Name_Fraktion.str.extract(r"\(([^)]+)\)", expand=False)
                details['Mitglied_Name'] = details.Mitglied_Name_Fraktion.str.split('(', expand=True)[[0]]
                details['Datenstand'] = pd.to_datetime(data_timestamp.isoformat())
                details_long = details.melt(id_vars=['Sitz_Nr', 'Mitglied_Name', 'Fraktion', 'Mitglied_Name_Fraktion', 'Datum', 'Datenstand'], var_name='Abst_Nr', value_name='Entscheid_Mitglied')

                all_df = polls.merge(details_long, how='left', left_on=['Datum', 'Abst_Nr'], right_on=['Datum', 'Abst_Nr'])
                print(all_df)
                pass

                # todo: Push data to ODS dataset

                # ct.update_hash_file(local_file)
        # ct.update_hash_file(credentials.ftp_ls_file)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
