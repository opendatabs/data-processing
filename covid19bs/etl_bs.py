from covid19bs import credentials
import pandas as pd
import os
import common
import numpy as np

print(f'Starting processing python script {__file__}...')

pub_file = os.path.join(credentials.path_orig, credentials.filename_pub_date)
print(f'Reading data from {pub_file}...')
df_pubdate = pd.read_csv(pub_file, sep=';')
print(f'Renaming columns to match openZH dataset...')
df_pubdate = df_pubdate.rename(columns={
    'datum': 'date',
    'meldezeit': 'time',
    'publizierte_neue_faelle_kum': 'ncumul_conf',
    'hospitalisierte_bs': 'current_hosp_resident',
    'hospitalisierte_icu': 'current_icu',
    'hospitalisierte_total': 'current_hosp'
    })

test_file = os.path.join(credentials.path_orig, credentials.filename_test_date)
print(f'Reading data from {test_file}...')
df_testdate = pd.read_csv(test_file, sep=';')
print(f'Renaming columns to match openZH dataset...')
df_testdate = df_testdate.rename(columns={
    'datum': 'test_date',
    'erholt_bs': 'ncumul_released',
    'gestorbene_bs_kum': 'ncumul_deceased',
    'isoliert_bs': 'current_isolated',
    'quarantaene_bs': 'current_quarantined_total',
    'quarantaene_reise_bs': 'current_quarantined_riskareatravel',
    'quarantaene_kontakt_bs': 'current_quarantined'
})
print(f'Calculating pub date...')
df_testdate['date'] = (pd.to_datetime(df_testdate['test_date']) + pd.Timedelta(days=1)).dt.strftime('%Y-%m-%d')

conf_non_resident_file = os.path.join(credentials.path_orig, credentials.filename_conf_non_resident)
print(f'Reading data from {conf_non_resident_file}...')
df_nonresident = pd.read_csv(conf_non_resident_file)
print(f'Keeping only necessary columns...')
df_nonresident = df_nonresident[['date', 'ncumul_confirmed_non_resident']]

print(f'Joining test and pub datasets...')
df_merged0 = pd.merge(df_pubdate, df_testdate, on=['date'], how='outer')
print(f'Joining the result with the ncumul_non_resident file...')
df_merged = pd.merge(df_merged0, df_nonresident, on=['date'], how='left')

print(f'Calculating columns...')
df_merged['abbreviation_canton_and_fl'] = 'BS'
df_merged['source'] = 'https://www.gesundheit.bs.ch'
df_merged['current_hosp_non_resident'] = df_merged['current_hosp'] - df_merged['current_hosp_resident']
# values for some columns are currently not available
df_merged['ncumul_tested'] = np.nan
df_merged['new_hosp'] = np.nan
df_merged['current_vent'] = np.nan
#df_merged['ncumul_confirmed_non_resident'] = np.nan

print('Calculating differences between current and previous row...')
df_diff = df_merged[['ncumul_conf', 'ncumul_released', 'ncumul_deceased', 'current_hosp',
                     'ncumul_confirmed_non_resident']].diff(periods=-1)
df_merged['ndiff_conf'] = df_diff.ncumul_conf
df_merged['ndiff_released'] = df_diff.ncumul_released
df_merged['ndiff_deceased'] = df_diff.ncumul_deceased
df_merged['ndiff_confirmed_non_resident'] = df_diff.ncumul_confirmed_non_resident

print(f'Change column order and keeping only necessary columns...')
df_merged = df_merged[['date', 'time', 'abbreviation_canton_and_fl', 'ncumul_tested', 'ncumul_conf', 'new_hosp', 'current_hosp',
        'current_icu', 'current_vent', 'ncumul_released', 'ncumul_deceased', 'source', 'current_isolated',
        'current_quarantined', 'ncumul_confirmed_non_resident', 'current_hosp_non_resident',
        'current_quarantined_riskareatravel', 'current_quarantined_total',
        'current_hosp_resident', 'ndiff_conf', 'ndiff_released', 'ndiff_deceased', 'ndiff_confirmed_non_resident', 'test_date']]

print(f'Removing test_date column for the moment...')
df_merged = df_merged.drop(columns=['test_date'])

# print(f'Keeping only top row...')
# df_latest = df_merged.head(1)
# latest_date = df_latest['date'][0]

# export_filename = os.path.join(credentials.path, credentials.filename).replace('.csv', f'_{latest_date}.csv')
export_filename = os.path.join(credentials.path, credentials.filename)
print(f'Exporting csv to {export_filename}')
df_merged.to_csv(export_filename, index=False)

common.upload_ftp(export_filename, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'covid19bs/daily')
print('Job successful!')
