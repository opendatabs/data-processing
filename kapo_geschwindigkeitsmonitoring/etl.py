import pandas as pd
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

print(f'{df}')
con.close()

print('Job successful!')
