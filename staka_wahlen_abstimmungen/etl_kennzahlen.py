from staka_wahlen_abstimmungen import credentials
import pandas as pd
import os
import dateparser
import common
from functools import reduce

import_file_name = os.path.join(credentials.path, credentials.data_orig)
print(f'Reading dataset from {import_file_name} to retrieve sheet names...')
sheets = pd.read_excel(import_file_name, sheet_name=None, skiprows=4, index_col=None)
dat_sheet_names = []
print(f'Determining "DAT n" sheets...')
for key in sheets:
    if key.startswith('DAT '):
        dat_sheet_names.append(key)

# specific for Kennzahlen dataset
valid_wahllokale = ['Total Basel', 'Total Riehen', 'Total Bettingen', 'Total Auslandschweizer (AS)', 'Total Kanton']

dat_sheets = []
for sheet_name in dat_sheet_names:
    print(f'Reading Abstimmungstitel from {sheet_name}...')
    df_title = pd.read_excel(import_file_name, sheet_name=sheet_name, skiprows=4, index_col=None)
    abst_title_raw = df_title.columns[1]
    # Get String that starts form ')' plus space + 1 characters to the right
    abst_title = abst_title_raw[abst_title_raw.find(')') + 2:]

    print(f'Reading Abstimmungsart and Date from {sheet_name}...')
    df_meta = pd.read_excel(import_file_name, sheet_name=sheet_name, skiprows=2, index_col=None)
    title_string = df_meta.columns[1]
    abst_type = 'kantonal' if title_string.startswith('Kantonal') else 'national'
    abst_date_raw = title_string[title_string.find('vom ') + 4:]
    abst_date = dateparser.parse(abst_date_raw).strftime('%Y-%m-%d')
    result_type = df_meta.columns[8]

    print(f'Reading data from {sheet_name}...')
    df = pd.read_excel(import_file_name, sheet_name=sheet_name, skiprows=6, index_col=None)# , header=[0, 1, 2])
    df.reset_index(inplace=True)

    print('Filtering out Wahllokale...')
    df = df[df['Wahllokale'].isin(valid_wahllokale)]

    print('Renaming columns...')
    df.rename(columns={'Wahllokale': 'Gemein_Name',
                       'Unnamed: 2': 'Stimmr_Anz',
                       'eingelegte': 'Eingel_Anz',
                       'leere': 'Leer_Anz',
                       'ungültige': 'Unguelt_Anz',
                       'Total gültige': 'Guelt_Anz',
                       'Ja': 'Ja_Anz',
                       'Nein': 'Nein_Anz'}, inplace=True)

    print(f'Setting cell values retrieved earlier...')
    df['Abst_Titel'] = abst_title
    df['Abst_Art'] = abst_type
    df['Abst_Datum'] = abst_date
    df['Result_Art'] = result_type
    df['Abst_ID'] = sheet_name[sheet_name.find('DAT ') + 4]

    print(f'Calculating columns...')
    df['anteil_ja_stimmen'] = df['Ja_Anz'] / df['Guelt_Anz']

    print('Dropping unnecessary columns...')
    df.drop(columns=['index', 'Unnamed: 0'], inplace=True)


    # df.to_csv(f'c:/dev/workspace/data-processing/staka_wahlen_abstimmungen/data/{sheet_name}.csv', index=False)
    dat_sheets.append(df)

print(f'Creating one dataframe for all Abstimmungen...')
all_df = pd.concat(dat_sheets)

# Code specific for Kennzahlen dataset
print(f'Cleaning up Gemeinde names in all_df...')
wahllok_replacements = {'Total Basel': 'Basel',
                        'Total Riehen': 'Riehen',
                        'Total Bettingen': 'Bettingen',
                        'Total Auslandschweizer (AS)': 'Auslandschweizer/-innen',
                        'Total Kanton': 'Basel-Stadt'}
for repl in wahllok_replacements:
    all_df.loc[(df['Gemein_Name'] == repl), 'Gemein_Name'] = wahllok_replacements[repl]

print(f'Adding Gemein_ID to all_df...')
gemein_ids = {'Basel': 1,
              'Riehen': 2,
              'Bettingen': 3,
              'Auslandschweizer/-innen': 9,
              'Basel-Stadt': 99}
for gemein_name in gemein_ids:
    all_df.loc[(all_df['Gemein_Name'] == gemein_name), 'Gemein_ID'] = gemein_ids[gemein_name]

stimmber_sheet_name = 'Stimmberechtigte (Details)'
print(f'Reading data from {stimmber_sheet_name}...')
df_stimmber = pd.read_excel(import_file_name, sheet_name=stimmber_sheet_name, skiprows=4, index_col=None)
print(f'Renaming columns in sheet {stimmber_sheet_name}...')
df_stimmber.rename(columns={'Unnamed: 0': 'empty',
                            'Unnamed: 1': 'Gemein_Name',
                            '\nStimmberechtigte': 'Stimmber_Anz',
                            'davon                                   Männer': 'Stimmber_Anz_M',
                            'davon                                          Frauen': 'Stimmber_Anz_F',
                            'Unnamed: 5': 'empty2'}, inplace=True)
print(f'Removing unnecessary columns from {stimmber_sheet_name}...')
df_stimmber.drop(columns=['empty', 'empty2'], inplace=True)
print(f'Cleaning up Gemeinde names in {stimmber_sheet_name}...')
# df_stimmber.loc[(df_stimmber['Gemein_Name'] == 'Total Kanton'), 'Gemein_Name'] = 'Basel-Stadt'
gemein_replacements = {'Total Kanton': 'Basel-Stadt'}
for repl in gemein_replacements:
    df_stimmber.loc[(df_stimmber['Gemein_Name'] == repl), 'Gemein_Name'] = gemein_replacements[repl]

kennz_sheet_name = 'Abstimmungs-Kennzahlen'
print(f'Reading data from {kennz_sheet_name}')
df_kennz = pd.read_excel(import_file_name, sheet_name=kennz_sheet_name, skiprows=7, index_col=None)
df_kennz.rename(columns={'Unnamed: 0': 'empty',
                            'Unnamed: 1': 'Gemein_Name',
                            '\nStimmberechtigte': 'Stimmber_Anz',
                            'Durchschnittliche\nStimmbeteiligung': 'Stimmbet',
                            'Durchschnittlicher Anteil der brieflich Stimmenden': 'Briefl_Ant',
                            'Durchschnittlicher Anteil der elektronisch Stimmenden': 'Elektr_Ant'}, inplace=True)
print(f'Cleaning up Gemeinde names in {kennz_sheet_name}...')
for repl in gemein_replacements:
    df_kennz.loc[(df_kennz['Gemein_Name'] == repl), 'Gemein_Name'] = gemein_replacements[repl]
print(f'Removing unnecessary columns from {kennz_sheet_name}...')
df_kennz.drop(columns=['empty', 'Elektr_Ant', 'Stimmber_Anz'], inplace=True)
print('Joining all sheets into one...')
frames_to_join = [all_df, df_kennz, df_stimmber]
df_merged = reduce(lambda left,right: pd.merge(left,right,on=['Gemein_Name'], how='inner'), frames_to_join)



export_file_name = os.path.join(credentials.path, f'Abstimmungen_{abst_date}.csv')
print(f'Exporting to {export_file_name}...')
df_merged.to_csv(export_file_name, index=False)

common.upload_ftp(export_file_name, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'wahlen_abstimmungen/abstimmungen')
print('Job successful!')

