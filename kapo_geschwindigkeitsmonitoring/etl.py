import pandas as pd
import pandas
import io
import linecache
import sys
import re
import subprocess
import glob
import pandas.io.sql as psql
import os
from ftplib import FTP
import common
from datetime import datetime, timedelta
from kapo_geschwindigkeitsmonitoring import credentials
import psycopg2 as pg
import cchardet as chardet


# Read csv file into dta frame. Fix missing line break on lines (multiples of 50006) using temporary file.
def read_csv_failsafe(data, encoding, error_bad_lines):
    try:
        print(f'Reading data using encoding {encoding} into data frame {data}...')
        return {'data': pd.read_table(file, skiprows=6, header=0, encoding=encoding, names=['Geschwindigkeit', 'Zeit', 'Datum', 'Richtung ID', 'Fahrzeuglänge'], error_bad_lines=error_bad_lines, warn_bad_lines=True),
                'errors': None}
    except pandas.errors.ParserError as detail:
        print(f'Exception has been raised: {detail}')
        err = sys.exc_info()[1]
        tmp = str(err).split(',')[0].split()
        line_number = int(tmp[len(tmp) - 1])
        print(f'Error in line {line_number}')
        with open(data, 'r', encoding=encoding) as fp:
            lines = fp.readlines()
        print(f'Line text: ')
        line_text_wrong = lines[line_number - 1].rstrip('\n')
        print(line_text_wrong)

        print(f'Trying to fix problem with missing newline character...')
        wrong_value = line_text_wrong.split('\t')[4]
        newline_position = wrong_value.index('.') + 2
        fixed_value = wrong_value[:newline_position] + '\n' + wrong_value[newline_position:]
        line_text_fixed = line_text_wrong.replace(wrong_value, fixed_value) + '\n'
        print(f'Fixed lines: ')
        print(line_text_fixed)
        errors = pd.DataFrame.from_dict({'file': [data], 'line_number': [line_number], 'line_text_orig': [line_text_wrong], 'line_text_fixed': [line_text_fixed]})
        # error_df = error_df.append({'file': data, 'line_number': line_number, 'line_text_orig': line_text_wrong,
        #                             'line_text_fixed': line_text_fixed}, ignore_index=True)
        lines[line_number - 1] = line_text_fixed
        # print(f'Trying to import fixed data into data frame...')
        # read csv from list of strings: https://stackoverflow.com/a/42172031/5005585
        # fixed_data = io.StringIO('\n'.join(lines))
        fixed_filename = os.path.join(credentials.path, f'temp_raw_data_{line_number}.csv')
        print(f'Writing temp file with fixed data: {fixed_filename}')
        with open(fixed_filename, 'w', encoding=encoding) as fixed_file:
            fixed_file.writelines(lines)
        # todo: fix - for now, only fix one error per file - otherwise I get an infinite loop here it seems.
        res = read_csv_failsafe(data=fixed_filename, encoding=encoding, error_bad_lines=False)
        return {'data': res['data'], 'errors': errors.append(res['errors'])}


print(f'Connecting to DB...')
con = pg.connect(credentials.pg_connection)
print(f'Reading data into data frame...')
df = psql.read_sql('SELECT * FROM projekte.geschwindigkeitsmonitoring', con)
con.close()
filename = os.path.join(credentials.path, credentials.filename.replace('.csv', '_metadata.csv'))
print(f'Exporting data to {filename}...')
df.to_csv(filename, index=False)

dfs = []
error_df = pd.DataFrame(columns=['line_text_orig', 'line_text_fixed', 'file', 'line_number'])
empty_df = pd.DataFrame(columns=['ID'])
for index, row in df.iterrows():
    print(f'Processing row {index} of {len(df)}...')
    if row['Verzeichnis'] is not None:
        metadata_file_path = os.path.join(
            credentials.detail_data_q_drive,
            row['Verzeichnis'].replace('Q:\\', '').replace('Ka', 'KA'))

        data_search_string = os.path.join(metadata_file_path, "**/*.txt")
        raw_files = glob.glob(data_search_string)
        for file in raw_files:
            file = file.replace('\\', '/')
            print(f'detecting encoding of {file}...')
            with open(file, 'rb') as f:
                raw_data = f.read()
                result = chardet.detect(raw_data)
                enc = result['encoding']
            failsafe_result = read_csv_failsafe(file, encoding=enc, error_bad_lines=True)
            raw_df = failsafe_result['data']
            error_df = error_df.append(failsafe_result['errors'])
            if not raw_df.empty:
                raw_df['Messung-ID'] = row['ID']
                dfs.append(raw_df)
                print(f'Calcuating timestamp...')
                raw_df['Datum_Zeit'] = raw_df['Datum'] + ' ' + raw_df['Zeit']
                #todo: fix ambiguous times - setting ambiguous to 'infer' raises an exception for some times
                raw_df['Timestamp'] = pd.to_datetime(raw_df['Datum_Zeit'], format='%d.%m.%y %H:%M:%S').dt.tz_localize('Europe/Zurich', ambiguous=True, nonexistent='shift_forward')
                raw_df = raw_df.drop(columns=['Fahrzeuglänge'])
                filename_current_measure = os.path.join(credentials.path, 'processed', credentials.filename.replace('.csv', f'_{row["ID"]}.csv'))
                if not os.path.exists(filename_current_measure):
                    print(f'Exporting data file for current measurement to {filename_current_measure}')
                    raw_df.to_csv(filename_current_measure, index=False)
                else:
                    print(f'File {filename_current_measure} already exists, ignoring...')
            else:
                print(f'Data frame is empty, ignoring...')
    else:
        print(f'"Verzeichnis" field is empty, skipping record: ')
        print(f'{row}')
        empty_df = empty_df.append({'ID': row['ID']}, ignore_index=True)

error_filename = os.path.join(credentials.path, credentials.filename.replace('.csv', '_errors.csv'))
print(f'Exporting file parse errors to {error_filename}')
error_df.to_csv(error_filename, index=False)

empty_filename = os.path.join(credentials.path, credentials.filename.replace('.csv', '_empty.csv'))
print(f'Exporting empty csv files to {empty_filename}')
empty_df.to_csv(empty_filename, index=False)

print(f'Creating one huge data frame...')
all_df = pd.concat(dfs)

# print(f'Exporting into one huge csv...')
# all_data_filename = os.path.join(credentials.path, credentials.filename.replace('.csv', '_data.csv'))
# all_df = all_df.drop(columns=['Fahrzeuglänge'])
# all_df.to_csv(all_data_filename, index=False)

print('Job successful!')
