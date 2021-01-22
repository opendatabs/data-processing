import pandas as pd
import glob
import pandas.io.sql as psql
import os
import common
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
print(f'Reading data into dataframe...')
df = psql.read_sql('SELECT *, ST_AsGeoJSON(the_geom) as the_geom_json, ST_AsEWKT(the_geom) as the_geom_EWKT, ST_AsText(the_geom) as the_geom_WKT FROM projekte.geschwindigkeitsmonitoring', con)
con.close()


dfs = []
files_to_upload = []
# error_df = pd.DataFrame(columns=['line_text_orig', 'line_text_fixed', 'file', 'line_number'])
empty_df = pd.DataFrame(columns=['ID'])
print(f'Removing metadata without data...')
df = df.dropna(subset=['Verzeichnis'])
for index, row in df.iterrows():
    print(f'Processing row {index + 1} of {len(df)}...')
    measure_id = row['ID']
    # print(f'Creating case-sensitive directory to data files...')
    metadata_file_path = credentials.detail_data_q_drive + os.sep + row.Verzeichnis.replace('\\', os.sep).replace(credentials.detail_data_q_base_path, '')
    data_search_string = os.path.join(metadata_file_path, "**/*.txt")
    raw_files = glob.glob(data_search_string, recursive=True)
    if len(raw_files) == 0:
        print(f'No data files found using search path {data_search_string}...')
    for i, file in enumerate(raw_files):
        file = file.replace('\\', '/')
        # Does not work - not all files have #1 or #2 in their filename
        # direction_csv = os.path.basename(file).split('#')[1]
        filename_current_measure = os.path.join(credentials.path, 'processed',  f'{str(measure_id)}_{i}.csv')
        if os.path.exists(filename_current_measure):
            print(f'Processed csv file already exists, ignoring ({filename_current_measure})...')
        else:
            # print(f'Detecting encoding of {file}...')
            with open(file, 'rb') as f:
                raw_data = f.read()
                result = chardet.detect(raw_data)
                enc = result['encoding']
            print(f'Fixing errors and reading data into dataframe from {file}...')
            raw_df = pd.read_table(fix_data(data_file=file, id=str(measure_id), encoding=enc), skiprows=6, header=0, encoding=enc, names=['Geschwindigkeit', 'Zeit', 'Datum', 'Richtung ID', 'Fahrzeuglänge'], error_bad_lines=True, warn_bad_lines=True)
            if raw_df.empty:
                print(f'Dataframe is empty, ignoring...')
            else:
                raw_df['Messung-ID'] = measure_id
                dfs.append(raw_df)
                print(f'Calculating timestamp...')
                raw_df['Datum_Zeit'] = raw_df['Datum'] + ' ' + raw_df['Zeit']
                # todo: fix ambiguous times - setting ambiguous to 'infer' raises an exception for some times
                raw_df['Timestamp'] = pd.to_datetime(raw_df['Datum_Zeit'], format='%d.%m.%y %H:%M:%S').dt.tz_localize('Europe/Zurich', ambiguous=True, nonexistent='shift_forward')
                raw_df = raw_df.drop(columns=['Fahrzeuglänge'])
                print(f'Exporting data file for current measurement to {filename_current_measure}')
                raw_df.to_csv(filename_current_measure, index=False)
                files_to_upload.append(filename_current_measure)

df_metadata = df[['ID', 'the_geom', 'Strasse', 'Strasse_Nr', 'Ort', 'Zone',
       'Richtung_1', 'Fzg_1', 'V50_1', 'V85_1', 'Ue_Quote_1',
       'Richtung_2', 'Fzg_2', 'V50_2', 'V85_2', 'Ue_Quote_2', 'Messbeginn', 'Messende'
      ]]
metadata_filename = os.path.join(credentials.path, credentials.filename.replace('.csv', '_metadata.csv'))
print(f'Exporting data to {metadata_filename}...')
df_metadata.to_csv(metadata_filename, index=False)
common.upload_ftp(filename=metadata_filename, server=credentials.ftp_server, user=credentials.ftp_user, password=credentials.ftp_pass, remote_path=credentials.ftp_remote_path_metadata)

for data_file in files_to_upload:
    #todo: if upload fails, file will never be uploaded because it is locally present. Thus we have to check if it is already on the FTP Server instead of locally present.
    common.upload_ftp(filename=data_file, server=credentials.ftp_server, user=credentials.ftp_user, password=credentials.ftp_pass, remote_path=credentials.ftp_remote_path_data)

if len(dfs) == 0:
    print(f'No new data present.')
else:
    print(f'Creating one huge dataframe...')
    all_df = pd.concat(dfs)
    print(f'{len(dfs)} datasets have been processed:')
    new_df = all_df.groupby(['Messung-ID', 'Richtung ID'])[['Messung-ID', 'Richtung ID']].agg(['unique'])
    print(new_df[['Messung-ID', 'Richtung ID']])

    all_data_filename = os.path.join(credentials.path, credentials.filename.replace('.csv', '_data.csv'))
    print(f'Exporting into one huge csv to {all_data_filename}...')
    all_df.to_csv(all_data_filename, index=False)

print('Job successful!')
