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
import fileinput

# Add missing line breaks for lines with more than 5 columns
def fix_data(data_file, id, encoding):
    filename_fixed = os.path.join(credentials.path, 'fixed', id + os.path.basename(data_file))
    # print(f'Fixing data if necessary and writing to {filename_fixed}...')
    with open(data_file, 'r', encoding=encoding) as input_file, \
            open(filename_fixed, 'w', encoding=encoding) as output_file:
        for i, line in enumerate(input_file):
            if len(line.split('\t')) > 5:
                wrong_value = line.split('\t')[4]
                newline_position = wrong_value.index('.') + 2
                fixed_value = wrong_value[:newline_position] + '\n' + wrong_value[newline_position:]
                line_fixed = line.replace(wrong_value, fixed_value) + '\n'
                print(f'Fixed line on line {i}:')
                print(f'Bad line: \n{line}')
                print(f'Fixed line: \n{line_fixed}')
                output_file.write(line_fixed)
            else:
                output_file.write(line)
    return filename_fixed


print(f'Connecting to DB...')
con = pg.connect(credentials.pg_connection)
print(f'Reading data into data frame...')
df = psql.read_sql('SELECT *, ST_AsGeoJSON(the_geom) as the_geom_json, ST_AsEWKT(the_geom) as the_geom_EWKT, ST_AsText(the_geom) as the_geom_WKT FROM projekte.geschwindigkeitsmonitoring', con)
con.close()
df_metadata = df[['ID', 'the_geom', 'Strasse', 'Strasse_Nr', 'Ort', 'Zone',
       'Richtung_1', 'Fzg_1', 'V50_1', 'V85_1', 'Ue_Quote_1',
       'Richtung_2', 'Fzg_2', 'V50_2', 'V85_2', 'Ue_Quote_2', 'Messbeginn', 'Messende',
        # 'the_geom_json', 'the_geom_ewkt', 'the_geom_wkt'
      ]]
filename = os.path.join(credentials.path, credentials.filename.replace('.csv', '_metadata.csv'))
print(f'Exporting data to {filename}...')
df_metadata.to_csv(filename, index=False)
quit(0)

dfs = []
# error_df = pd.DataFrame(columns=['line_text_orig', 'line_text_fixed', 'file', 'line_number'])
empty_df = pd.DataFrame(columns=['ID'])
for index, row in df.iterrows():
    print(f'Processing row {index + 1} of {len(df)}...')
    measure_id = row['ID']
    if row['Verzeichnis'] is not None:
        metadata_file_path = os.path.join(
            credentials.detail_data_q_drive,
            row['Verzeichnis'].replace('Q:\\', '').replace('Ka', 'KA'))

        data_search_string = os.path.join(metadata_file_path, "**/*.txt")
        raw_files = glob.glob(data_search_string)
        for file in raw_files:
            file = file.replace('\\', '/')
            # print(f'Detecting encoding of {file}...')
            with open(file, 'rb') as f:
                raw_data = f.read()
                result = chardet.detect(raw_data)
                enc = result['encoding']
            print(f'Fixing errors and reading data into dataframe from {file}...')
            raw_df = pd.read_table(fix_data(data_file=file, id=str(measure_id), encoding=enc), skiprows=6, header=0, encoding=enc, names=['Geschwindigkeit', 'Zeit', 'Datum', 'Richtung ID', 'Fahrzeuglänge'], error_bad_lines=True, warn_bad_lines=True)

            if not raw_df.empty:
                raw_df['Messung-ID'] = measure_id
                dfs.append(raw_df)
                print(f'Calculating timestamp...')
                raw_df['Datum_Zeit'] = raw_df['Datum'] + ' ' + raw_df['Zeit']
                #todo: fix ambiguous times - setting ambiguous to 'infer' raises an exception for some times
                raw_df['Timestamp'] = pd.to_datetime(raw_df['Datum_Zeit'], format='%d.%m.%y %H:%M:%S').dt.tz_localize('Europe/Zurich', ambiguous=True, nonexistent='shift_forward')
                raw_df = raw_df.drop(columns=['Fahrzeuglänge'])
                filename_current_measure = os.path.join(credentials.path, 'processed', str(measure_id) + os.path.basename(file))
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

# error_filename = os.path.join(credentials.path, credentials.filename.replace('.csv', '_errors.csv'))
# print(f'Exporting file parse errors to {error_filename}')
# error_df.to_csv(error_filename, index=False)

empty_filename = os.path.join(credentials.path, credentials.filename.replace('.csv', '_empty.csv'))
print(f'Exporting dataframe of empty csv files to {empty_filename}')
empty_df.to_csv(empty_filename, index=False)

print(f'Creating one huge data frame...')
all_df = pd.concat(dfs)
all_data_filename = os.path.join(credentials.path, credentials.filename.replace('.csv', '_data.csv'))
# print(f'Exporting into one huge csv...')
# all_df = all_df.drop(columns=['Fahrzeuglänge'])
# all_df.to_csv(all_data_filename, index=False)

print('Job successful!')
