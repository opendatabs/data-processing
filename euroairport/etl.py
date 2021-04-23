from euroairport import credentials
import pandas as pd
import os
import common

print(f'Downloading data from FTP server...')
common.download_ftp([credentials.data_orig], credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass,
                    credentials.ftp_remote_path, credentials.local_path)

import_file_name = os.path.join(credentials.path, credentials.data_orig)
print(f'Reading dataset from {import_file_name}...')
df = pd.read_excel(import_file_name, index_col=None)

print('Create date column as a first column, then drop d, m, y columns...')
df['date'] = pd.to_datetime(df.Annee * 10000 + df.Mois * 100 + df.Jour, format='%Y%m%d')
df.insert(0, 'date', df.pop('date'))
df2 = df.drop(columns=['Annee', 'Mois', 'Jour'])

print('Removing rows with empty date...')
df3 = df2.dropna(subset=['date'])

print('Unpivoting table...')
df_pax =  df3.melt(id_vars=['date'], value_name='Pax',  var_name='variable_pax',  value_vars=['PAX_Pax', 'FRET_EXPRESS_Pax', 'FRET_CARGO_Pax', 'AUTRES_Pax', 'Total_Pax'])
df_fret = df3.melt(id_vars=['date'], value_name='Fret', var_name='variable_fret', value_vars=['PAX_Fret', 'FRET_EXPRESS_Fret', 'FRET_CARGO_Fret', 'AUTRES_Fret', 'Total_Fret'])
df_mvt =  df3.melt(id_vars=['date'], value_name='Mvt',  var_name='variable_mvt',  value_vars=['PAX_Mvt', 'FRET_EXPRESS_Mvt', 'FRET_CARGO_Mvt', 'AUTRES_Mvt', 'Total_Mvt'])

print('Getting Kategorie as first part of string...')
# df_pax['Kategorien'] = df_pax['variable'].str.split('_', n=1)
# df_pax['Kategorie'] = df_pax['Kategorien'].apply(lambda x: x[0])
df_pax['Kategorie'] = df_pax['variable_pax'].str.rsplit('_', n=1).apply(lambda x: x[0])
df_fret['Kategorie'] = df_fret['variable_fret'].str.rsplit('_', n=1).apply(lambda x: x[0])
df_mvt['Kategorie'] = df_mvt['variable_mvt'].str.rsplit('_', n=1).apply(lambda x: x[0])

# df_pax.to_csv('C:/dev/workspace/data-processing/euroairport/data/pax.csv', index=False)
# df_fret.to_csv('C:/dev/workspace/data-processing/euroairport/data/fret.csv', index=False)
# df_mvt.to_csv('C:/dev/workspace/data-processing/euroairport/data/mvt.csv', index=False)

print('Merging data frames into one again...')
df_merged1 = pd.merge(df_pax, df_fret, on=['date', 'Kategorie'], how='outer')
df_merged = pd.merge(df_merged1, df_mvt, on=['date', 'Kategorie'], how='outer')

print('Sorting...')
df_sort = df_merged.sort_values(by=['date', 'Kategorie'], ascending=False)

print('Replacing french with german words in Kategorie...')
df_german = df_sort.replace({'Kategorie': {
    'PAX':              'Passagierverkehr',
    'FRET_EXPRESS':     'Fracht Express',
    'FRET_CARGO':       'Fracht Cargo',
    'AUTRES':           'Andere Kategorien'}})

print('Removing Totals...')
df_nototal = df_german[df_german.Kategorie != "Total"]

export_file_name = os.path.join(credentials.path, credentials.data_export)
print(f'Exporting to {export_file_name}...')
df_nototal.to_csv(export_file_name, index=False)

common.upload_ftp(export_file_name, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, '')
print('Job successful!')

