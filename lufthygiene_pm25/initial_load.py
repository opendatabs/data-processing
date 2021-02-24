import pandas as pd
from datetime import datetime
import common
import glob
import os
from lufthygiene_pm25 import credentials

print(f'Reading data from multiple csv into single dataframe...')
df = pd.concat([common.pandas_read_csv(f, skiprows=range(1, 6), sep=';', encoding='cp1252') for f in glob.glob('c:/dev/workspace/data-processing/lufthygiene_pm25/archive/*.csv')], ignore_index=True)
print(f'Sorting...')
print(f'Calculating columns...')
df['timestamp'] = pd.to_datetime(df.Zeit, format='%d.%m.%Y %H:%M:%S').dt.tz_localize('Europe/Zurich', ambiguous=True, nonexistent='shift_forward')
df = df.sort_values(by=['timestamp'], ascending=False, ignore_index=True)
print(f'Dropping duplicate rows...')
df = df.drop_duplicates()
print(f'Melting dataframe...')
ldf = df.melt(id_vars=['Zeit', 'timestamp'], var_name='station', value_name='pm_2_5')
print(f'Dropping rows with empty pm25 value...')
ldf = ldf.dropna(subset=['pm_2_5'])

# Better do the join in ODS
# stat_df = common.pandas_read_csv('https://data-bs.ch/lufthygiene/pm25/stations/stations.csv', sep='\t')
# merged_df = ldf.merge(stat_df, how='left', left_on='station', right_on='Titel')

filename = os.path.join(credentials.path, f'archive_{datetime.today().strftime("%Y-%m-%dT%H-%M-%S%z")}.csv')
print(f'Exporting data to {filename}...')
ldf.to_csv(filename, index=False)

print(f'Job completed successfully!')
