from covid19bs import credentials
import pandas as pd
import os
import common

sourcefile = 'https://raw.githubusercontent.com/openZH/covid_19/master/fallzahlen_kanton_total_csv_v2/COVID19_Fallzahlen_Kanton_BS_total.csv'
print(f'Reading date from {sourcefile}...')
df = pd.read_csv(sourcefile)
print('Calculating differences between current and previous row...')
df_diff = df[['ncumul_conf', 'ncumul_released', 'ncumul_deceased', 'ncumul_confirmed_non_resident']].diff()
df['ndiff_conf'] = df_diff.ncumul_conf
df['ndiff_released'] = df_diff.ncumul_released
df['ndiff_deceased'] = df_diff.ncumul_deceased
df['ndiff_confirmed_non_resident'] = df_diff.ncumul_confirmed_non_resident

filename = os.path.join(credentials.path, credentials.filename)
print(f'Exporting data to {filename}')
df.to_csv(filename, index=False)

common.upload_ftp(filename, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'covid19bs')
print('Job successful!')