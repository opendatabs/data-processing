from md_covid19cases import credentials
import pandas as pd
import common
import os

filename = os.path.join(credentials.path, credentials.filename_gestorbene)
print(f'Reading data from {filename}, renaming columns and performing some calculations...')
df = pd.read_csv(filename, sep=';')
df = df.rename(columns={'sterbe_datum': 'date_of_death',
                        'melde_datum': 'date_of_publication',
                        'alter': 'AgeYear',
                        'geschlecht': 'Gender',
                        'vorerkrankung': 'PreExistingCond',
                        'verstorbene_kumuliert': 'ncumul_deceased'})
df['source'] = 'https://www.gesundheit.bs.ch'
df['Area'] = 'Canton_BS'
df['NewDeaths'] = 1
df['CaseNumberPerDay'] = df.groupby(['date_of_death']).cumcount() + 1

export_filename = os.path.join(credentials.export_path, credentials.export_filename_gestorbene)
print(f'Exporting data to {export_filename}...')
df.to_csv(export_filename, index=False)
common.upload_ftp(export_filename, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'md/covid19_cases')
print('Job successful!')
