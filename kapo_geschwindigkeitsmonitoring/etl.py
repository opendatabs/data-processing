import pandas as pd
import glob
import pandas.io.sql as psql
import os
from ftplib import FTP
import common
from datetime import datetime, timedelta
from kapo_geschwindigkeitsmonitoring import credentials
import psycopg2 as pg

print(f'Database opened successfullyConnecting to DB...')
con = pg.connect(credentials.pg_connection)

print(f'Reading data into data frame...')
df = psql.read_sql('SELECT * FROM projekte.geschwindigkeitsmonitoring', con)

filename = os.path.join(credentials.path, credentials.filename)
print(f'Exporting data to {filename}...')
df.to_csv(filename, index=False)

metadata_file_path = os.path.join(
    credentials.detail_data_q_drive,
    df['Verzeichnis'].iloc[0].replace('Q:\\', '').replace('Ka', 'KA'))
print(f'{metadata_file_path}')

data_search_string = os.path.join(metadata_file_path, "**/*.txt")
raw_files = glob.glob(data_search_string)
print(f'*.txt Contents of directory {data_search_string}: {raw_files}')
dfs = []
for file in raw_files:
    file = file.replace('\\', '/')
    raw_df = pd.read_table(file, skiprows=6, header=0, names=['Geschwindigkeit', 'Zeit', 'Datum', 'Richtung ID', 'Fahrzeuglänge'])
    dfs.append(raw_df)

# print(f'{df}')
con.close()


print('Job successful!')
