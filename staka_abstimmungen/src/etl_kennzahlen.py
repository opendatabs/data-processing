from staka_abstimmungen import credentials
import pandas as pd
import os
import dateparser
import numpy as np
import common
from functools import reduce


def main():
    data_file_names = credentials.data_orig
    abst_date, concatenated_df = calculate_kennzahlen(data_file_names)

    export_file_name = os.path.join(credentials.path, 'data-processing-output', f'Abstimmungen_{abst_date}.csv')
    print(f'Exporting to {export_file_name}...')
    concatenated_df.to_csv(export_file_name, index=False)

    common.upload_ftp(export_file_name, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'wahlen_abstimmungen/abstimmungen')
    print('Job successful!')


def calculate_kennzahlen(data_file_names):
    print(f'Starting to work with data file(s) {data_file_names}...')
    abst_date = ''
    appended_data = []
    columns_to_keep = ['Gemein_Name', 'Stimmr_Anz', 'Eingel_Anz', 'Leer_Anz', 'Unguelt_Anz', 'Guelt_Anz',
                       'Ja_Anz', 'Nein_Anz', 'Abst_Titel', 'Abst_Art', 'Abst_Datum', 'Result_Art',
                       'Abst_ID',
                       'anteil_ja_stimmen', 'Gemein_ID', 'Durchschn_Stimmbet_pro_Abst_Art',
                       'Durchschn_Briefl_Ant_pro_Abst_Art', 'Stimmber_Anz',
                       'Stimmber_Anz_M', 'Stimmber_Anz_F', 'abst_typ']
    for data_file_name in data_file_names:
        import_file_name = os.path.join(credentials.path, data_file_name)
        print(f'Reading dataset from {import_file_name} to retrieve sheet names...')
        sheets = pd.read_excel(import_file_name, sheet_name=None, skiprows=4, index_col=None)
        dat_sheet_names = []
        print(f'Determining "DAT n" sheets...')
        for key in sheets:
            if key.startswith('DAT '):
                dat_sheet_names.append(key)

        # specific for Kennzahlen dataset
        valid_wahllokale = ['Total Basel', 'Total Riehen', 'Total Bettingen', 'Total Auslandschweizer (AS)',
                            'Total Kanton']

        dat_sheets = []
        for sheet_name in dat_sheet_names:
            is_gegenvorschlag = False  # Is this a sheet that contains a Gegenvorschlag?
            print(f'Reading Abstimmungstitel from {sheet_name}...')
            df_title = pd.read_excel(import_file_name, sheet_name=sheet_name, skiprows=4, index_col=None)
            abst_title_raw = df_title.columns[1]
            # Get String that starts form ')' plus space + 1 characters to the right
            abst_title = abst_title_raw[abst_title_raw.find(')') + 2:]
            if 'Gegenvorschlag' in abst_title:
                is_gegenvorschlag = True

            print(f'Reading Abstimmungsart and Date from {sheet_name}...')
            df_meta = pd.read_excel(import_file_name, sheet_name=sheet_name, skiprows=2, index_col=None)
            title_string = df_meta.columns[1]
            abst_type = 'kantonal' if title_string.startswith('Kantonal') else 'national'
            abst_date_raw = title_string[title_string.find('vom ') + 4:]
            abst_date = dateparser.parse(abst_date_raw).strftime('%Y-%m-%d')

            print(f'Reading data from {sheet_name}...')
            df = pd.read_excel(import_file_name, sheet_name=sheet_name, skiprows=6,
                               index_col=None)  # , header=[0, 1, 2])
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
            df['Abst_ID'] = sheet_name[sheet_name.find('DAT ') + 4]
            df['abst_typ'] = ''

            df.Guelt_Anz.replace(0, pd.NA, inplace=True)

            if is_gegenvorschlag:
                print(f'Adding Gegenvorschlag data...')
                result_type = df_meta.columns[15]
                df.abst_typ = 'Initiative mit Gegenvorschlag und Stichfrage'
                df.rename(columns={'Ja.1': 'Gege_Ja_Anz',
                                   'Nein.1': 'Gege_Nein_Anz',
                                   'Initiative': 'Sti_Initiative_Anz',
                                   'Gegen-vorschlag': 'Sti_Gegenvorschlag_Anz',
                                   'ohne gültige Antwort': 'Init_OGA_Anz',
                                   'ohne gültige Antwort.1': 'Gege_OGA_Anz',
                                   'ohne gültige Antwort.2': 'Sti_OGA_Anz'}, inplace=True)

                print(f'Calculating anteil_ja_stimmen for Gegenvorschlag case...')
                for column in [df.Ja_Anz, df.Nein_Anz, df.Gege_Ja_Anz, df.Gege_Nein_Anz, df.Sti_Initiative_Anz,
                               df.Sti_Gegenvorschlag_Anz]:
                    column.replace(0, pd.NA, inplace=True)  # Prevent division by zero errors

                df['anteil_ja_stimmen'] = df.Ja_Anz / (df.Ja_Anz + df.Nein_Anz)
                df['gege_anteil_ja_Stimmen'] = df.Gege_Ja_Anz / (df.Gege_Ja_Anz + df.Gege_Nein_Anz)
                df['sti_anteil_init_stimmen'] = df.Sti_Initiative_Anz / (
                            df.Sti_Initiative_Anz + df.Sti_Gegenvorschlag_Anz)
                columns_to_keep = columns_to_keep + ['Gege_Ja_Anz', 'Gege_Nein_Anz', 'Sti_Initiative_Anz',
                                                     'Sti_Gegenvorschlag_Anz', 'gege_anteil_ja_Stimmen',
                                                     'sti_anteil_init_stimmen', 'Init_OGA_Anz', 'Gege_OGA_Anz',
                                                     'Sti_OGA_Anz']
            else:
                print(f'Adding data for case that is not with Gegenvorschlag...')
                result_type = df_meta.columns[8]
                print(f'Calculating anteil_ja_stimmen for case that is not with Gegenvorschlag...')
                df['anteil_ja_stimmen'] = df['Ja_Anz'] / df['Guelt_Anz']

            df['Result_Art'] = result_type
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
                                    'Stimmberechtigte': 'Stimmber_Anz',
                                    'davon                                   Männer': 'Stimmber_Anz_M',
                                    'davon                                          Frauen': 'Stimmber_Anz_F',
                                    'Unnamed: 5': 'empty2'}, inplace=True)
        print(f'Cleaning up Gemeinde names in {stimmber_sheet_name}...')
        # df_stimmber.loc[(df_stimmber['Gemein_Name'] == 'Total Kanton'), 'Gemein_Name'] = 'Basel-Stadt'
        gemein_replacements = {'Total Kanton': 'Basel-Stadt'}
        for repl in gemein_replacements:
            df_stimmber.loc[(df_stimmber['Gemein_Name'] == repl), 'Gemein_Name'] = gemein_replacements[repl]

        kennz_sheet_name = 'Abstimmungs-Kennzahlen'
        # number of empty rows may be different for KAN and EID files
        skip_rows = 4 if '_KAN' in import_file_name else 7
        print(f'Reading data from {kennz_sheet_name}, skipping first {skip_rows} rows...')
        df_kennz = pd.read_excel(import_file_name, sheet_name=kennz_sheet_name, skiprows=skip_rows, index_col=None)
        print("df_kenz:", df_kennz.columns)
        df_kennz.rename(columns={'Unnamed: 0': 'empty',
                                 'Unnamed: 1': 'Gemein_Name',
                                 '\nStimmberechtigte': 'Stimmber_Anz',
                                 'Stimmberechtigte': 'Stimmber_Anz',
                                 'Durchschnittliche\nStimmbeteiligung': 'Durchschn_Stimmbet_pro_Abst_Art',
                                 '\nStimmbeteiligung': 'Durchschn_Stimmbet_pro_Abst_Art',
                                 'Stimmbeteiligung': 'Durchschn_Stimmbet_pro_Abst_Art',
                                 'Durchschnittlicher Anteil der brieflich Stimmenden': 'Durchschn_Briefl_Ant_pro_Abst_Art',
                                 'Durchschnittlicher Anteil\nder brieflich Stimmenden': 'Durchschn_Briefl_Ant_pro_Abst_Art',
                                 'Anteil\nder brieflich Stimmenden': 'Durchschn_Briefl_Ant_pro_Abst_Art',
                                 'Anteil der brieflich\nStimmenden': 'Durchschn_Briefl_Ant_pro_Abst_Art',
                                 'Anteil der brieflich Stimmenden': 'Durchschn_Briefl_Ant_pro_Abst_Art',
                                 'Durchschnittlicher Anteil der elektronisch Stimmenden': 'Durchschn_Elektr_Ant_pro_Abst_Art'},
                        inplace=True)

        print(f'Cleaning up Gemeinde names in {kennz_sheet_name}...')
        for repl in gemein_replacements:
            df_kennz.loc[(df_kennz['Gemein_Name'] == repl), 'Gemein_Name'] = gemein_replacements[repl]
        # print(f'Removing duplicate column stimmber_anz from {kennz_sheet_name}...')
        # df_kennz.drop(columns=['Stimmber_Anz'], inplace=True)
        print('Joining all sheets into one...')
        all_df = make_anz_columns_float(all_df)
        df_kennz = make_anz_columns_float(df_kennz)
        df_stimmber = make_anz_columns_float(df_stimmber)
        df_merged = pd.merge(all_df, df_kennz, on=['Gemein_Name'], how='inner')
        df_merged = pd.merge(df_merged, df_stimmber, on=['Gemein_Name', 'Stimmber_Anz'])
        print('Keeping only necessary columns...')
        df_merged = df_merged.filter(columns_to_keep)

        appended_data.append(df_merged)
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
    concatenated_df['Abst_ID_Titel'] = concatenated_df['Abst_ID'].astype(str) + ': ' + concatenated_df['Abst_Titel']
    # print(f'Calculating Stimmbeteiligung...')
    # concatenated_df['Stimmbet'] = concatenated_df['Eingel_Anz'] / concatenated_df['Stimmber_Anz']
    return abst_date, concatenated_df

def make_anz_columns_float(df):
    for x in df.columns:
        if 'Anz' in x:
            df[x] = df[x].astype('float64')
    return df


if __name__ == "__main__":
    print(f'Executing {__file__}...')
    main()
