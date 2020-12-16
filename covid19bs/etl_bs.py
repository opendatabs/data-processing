from covid19bs import credentials
import pandas as pd
import os
import common
import numpy as np


print(f'Starting processing python script {__file__}...')
hosp_url = 'https://data.bs.ch/explore/dataset/100109/download/?format=csv'
print(f'Reading hosp data from {hosp_url}...')
df_hosp = pd.read_csv(hosp_url, sep=';')
print(f'Keeping only necessary hosp columns...')
df_hosp = df_hosp[['date', 'current_hosp', 'current_hosp_resident', 'current_hosp_non_resident', 'current_icu']]

sourcefile = os.path.join(credentials.path_orig, credentials.filename_faelle)
print(f'Reading case data from {sourcefile}...')
df_cases = pd.read_csv(sourcefile, sep=';')
print(f'Keeping only certain case columns...')
df_cases = df_cases[['publikationsdatum', 'meldezeit', 'datum', 'faelle_bs_kum', 'hospitalisierte_total', 'hospitalisierte_icu',
         'erholt_bs', 'gestorbene_bs_kum', 'isoliert_bs', 'quarantaene_kontakt_bs',
         'quarantaene_reise_bs', 'quarantaene_bs', 'hospitalisierte_bs']]

print(f'Left-joining case and hosp datasets, then dropping date from hosp...')
df_merged = pd.merge(df_cases, df_hosp, left_on=['publikationsdatum'], right_on=['date'], how='left')
df_merged = df_merged.drop(columns=['date'])

print(f'Renaming columns to match openZH file...')
df_merged = df_merged.rename(columns={
                        'publikationsdatum': 'date',
                        'meldezeit': 'time',
                        'datum': 'test_date',
                        'faelle_bs_kum': 'ncumul_conf',
                        #'hospitalisierte_total': 'current_hosp',
                        #'hospitalisierte_icu': 'current_icu',
                        'erholt_bs': 'ncumul_released',
                        'gestorbene_bs_kum': 'ncumul_deceased',
                        'isoliert_bs': 'current_isolated',
                        'quarantaene_kontakt_bs': 'current_quarantined',
                        'quarantaene_reise_bs': 'current_quarantined_riskareatravel',
                        'quarantaene_bs': 'current_quarantined_total'
                        #'hospitalisierte_bs': 'current_hosp_resident'
                        })

print(f'Calculating columns...')
df_merged['abbreviation_canton_and_fl'] = 'BS'
df_merged['source'] = 'https://www.gesundheit.bs.ch'
# df_cases['current_hosp_non_resident'] = df_cases['current_hosp'] - df_cases['current_hosp_resident']
# values for some columns are currently not available
df_merged['ncumul_tested'] = np.nan
df_merged['new_hosp'] = np.nan
df_merged['current_vent'] = np.nan
df_merged['ncumul_confirmed_non_resident'] = np.nan

print('Calculating differences between current and previous row...')
df_diff = df_merged[['ncumul_conf', 'ncumul_released', 'ncumul_deceased', 'current_hosp']].diff(periods=-1)
df_merged['ndiff_conf'] = df_diff.ncumul_conf
df_merged['ndiff_released'] = df_diff.ncumul_released
df_merged['ndiff_deceased'] = df_diff.ncumul_deceased
df_merged['ndiff_confirmed_non_resident'] = np.nan

print(f'Change column order...')
df_merged = df_merged[['date', 'time', 'abbreviation_canton_and_fl', 'ncumul_tested', 'ncumul_conf', 'new_hosp', 'current_hosp',
         'current_icu', 'current_vent', 'ncumul_released', 'ncumul_deceased', 'source', 'current_isolated',
         'current_quarantined', 'ncumul_confirmed_non_resident', 'current_hosp_non_resident',
         'current_quarantined_riskareatravel', 'current_quarantined_total', 'current_hosp_resident',
         'ndiff_conf', 'ndiff_released', 'ndiff_deceased', 'ndiff_confirmed_non_resident', 'test_date']]

# print(f'Removing test_date column for the moment...')
# df_merged = df_merged.drop(columns=['test_date'])

print(f'Keeping only latest record...')
df_latest = df_merged.head(1)
latest_date = df_latest['date'][0]
export_filename = os.path.join(credentials.path, credentials.filename).replace('.csv', '_' + latest_date + '.csv')
print(f'Exporting csv to {export_filename}')
df_latest.to_csv(export_filename, index=False)

common.upload_ftp(export_filename, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'covid19bs/daily')
print('Job successful!')
