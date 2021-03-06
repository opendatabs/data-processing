from staka_abstimmungen import credentials
import pandas as pd
import os
import dateparser
import numpy as np
import common

appended_data = []
data_file_names = credentials.data_orig
print(f'Starting to work with data file(s) {data_file_names}...')
for data_file_name in data_file_names:
    import_file_name = os.path.join(credentials.path, data_file_name)
    print(f'Reading dataset from {import_file_name} to retrieve sheet names...')
    sheets = pd.read_excel(import_file_name, sheet_name=None, skiprows=4, index_col=None)
    dat_sheet_names = []
    print(f'Determining "DAT n" sheets...')
    for key in sheets:
        if key.startswith('DAT '):
            dat_sheet_names.append(key)

    valid_wahllokale = ['Bahnhof SBB', 'Rathaus', 'Polizeiwache Clara', 'Basel brieflich Stimmende', 'Riehen Gemeindehaus',
                        'Riehen brieflich Stimmende', 'Bettingen Gemeindehaus', 'Bettingen brieflich Stimmende',
                        'Persönlich an der Urne Stimmende AS', 'Brieflich Stimmende AS']

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
        df.rename(columns={'Wahllokale': 'Wahllok_name',
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

        dat_sheets.append(df)

    print(f'Creating one dataframe for all Abstimmungen...')
    all_df = pd.concat(dat_sheets)

    print(f'Calculating anteil_ja_stimmen...')
    all_df.Guelt_Anz.replace(0, pd.NA, inplace=True)
    all_df['anteil_ja_stimmen'] = all_df['Ja_Anz'] / all_df['Guelt_Anz']

    print('Keeping only necessary columns...')
    all_df = all_df.filter(['Wahllok_name', 'Stimmr_Anz', 'Eingel_Anz', 'Leer_Anz', 'Unguelt_Anz', 'Guelt_Anz', 'Ja_Anz', 'Nein_Anz',
                   'Abst_Titel', 'Abst_Art', 'Abst_Datum', 'Result_Art', 'Abst_ID', 'anteil_ja_stimmen'],)

    appended_data.append(all_df)

print(f'Concatenating data from all import files ({appended_data})...')
concatenated_df = pd.concat(appended_data)

print(f'Calculating Abstimmungs-ID based on all data...')
nat_df = concatenated_df[concatenated_df['Abst_Art'] == 'national']
if 'national' in nat_df['Abst_Art'].unique():
    max_nat_id = int(nat_df['Abst_ID'].max())
    concatenated_df['Abst_ID'] = np.where(concatenated_df['Abst_Art'] == 'kantonal',
                                          max_nat_id + concatenated_df['Abst_ID'].astype('int32'),
                                          concatenated_df['Abst_ID'])

print('Creating column "Abst_ID_Titel"...')
all_df['Abst_ID_Titel'] = all_df['Abst_ID'].astype(str) + ': ' + all_df['Abst_Titel']

export_file_name = os.path.join(credentials.path, 'data-processing-output', f'Abstimmungen_Details_{abst_date}.csv')
print(f'Exporting to {export_file_name}...')
concatenated_df.to_csv(export_file_name, index=False)

common.upload_ftp(export_file_name, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'wahlen_abstimmungen/abstimmungen')
print('Job successful!')
