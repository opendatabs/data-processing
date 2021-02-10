import os
import common
import pandas as pd
from md_covid19cases import credentials

old_data_file = os.path.join(credentials.path, credentials.filename_gestorbene_alt)
print(f'Reading old data from file {old_data_file}...')
old_df = pd.read_csv(old_data_file, sep=';')
old_df['datum_typ'] = 'Meldedatum'
current_data_file = os.path.join(credentials.path, credentials.filename_gestorbene_neu)
print(f'Reading current data from file {current_data_file}...')
new_df = pd.read_csv(current_data_file, sep=';')
new_df['datum_typ'] = 'Sterbedatum'
df = old_df.append(new_df)
print(f'Replacing sex code W by F...')
df = df.replace('W', 'F')
print(f'Calculating column "fall_nr_pro_tag"...')
df['fall_nr_pro_tag'] = df.groupby(['datum']).cumcount() + 1
df = df.sort_values(by=['datum', 'fall_nr_pro_tag'])

export_filename = os.path.join(credentials.export_path, credentials.export_filename_gestorbene)
print(f'Exporting dataset to file {export_filename}...')
df.to_csv(export_filename, index=False)
common.upload_ftp(export_filename, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'md/covid19_cases')
print('Job successful!')
