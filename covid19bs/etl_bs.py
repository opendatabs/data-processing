from covid19bs import credentials
import pandas as pd
import os
import common
import numpy as np

sourcefile = os.path.join(credentials.path_orig, credentials.filename_faelle)
print(f'Reading date from {sourcefile}...')
df = pd.read_csv(sourcefile, sep=';')

print(f'Keeping only certain columns...')
df = df[['publikationsdatum', 'meldezeit', 'datum', 'faelle_bs_kum', 'hospitalisierte_total', 'hospitalisierte_icu',
         'erholt_bs', 'gestorbene_bs_kum', 'isoliert_bs', 'quarantaene_kontakt_bs',
         'quarantaene_reise_bs', 'quarantaene_bs', 'hospitalisierte_bs']]

print(f'Renaming columns to match openZH file...')
df = df.rename(columns={'publikationsdatum': 'date',
                        'meldezeit': 'time',
                        'datum': 'test_date',
                        'faelle_bs_kum': 'ncumul_conf',
                        'hospitalisierte_total': 'current_hosp',
                        'hospitalisierte_icu': 'current_icu',
                        'erholt_bs': 'ncumul_released',
                        'gestorbene_bs_kum': 'ncumul_deceased',
                        'isoliert_bs': 'current_isolated',
                        'quarantaene_kontakt_bs': 'current_quarantined',
                        'quarantaene_reise_bs': 'current_quarantined_riskareatravel',
                        'quarantaene_bs': 'current_quarantined_total',
                        'hospitalisierte_bs': 'current_hosp_resident'})

print(f'Calculating columns...')
df['abbreviation_canton_and_fl'] = 'BS'
df['source'] = 'https://www.gesundheit.bs.ch'
df['current_hosp_non_resident'] = df['current_hosp'] - df['current_hosp_resident']
# values for some columns are currently not available
df['ncumul_tested'] = np.nan
df['new_hosp'] = np.nan
df['current_vent'] = np.nan
df['ncumul_confirmed_non_resident'] = np.nan

print('Calculating differences between current and previous row...')
df_diff = df[['ncumul_conf', 'ncumul_released', 'ncumul_deceased', 'current_hosp']].diff(periods=-1)
df['ndiff_conf'] = df_diff.ncumul_conf
df['ndiff_released'] = df_diff.ncumul_released
df['ndiff_deceased'] = df_diff.ncumul_deceased
df['ndiff_confirmed_non_resident'] = np.nan

print(f'Change column order...')
df = df[['date','time','abbreviation_canton_and_fl','ncumul_tested','ncumul_conf','new_hosp','current_hosp',
         'current_icu','current_vent','ncumul_released','ncumul_deceased','source','current_isolated',
         'current_quarantined','ncumul_confirmed_non_resident','current_hosp_non_resident',
         'current_quarantined_riskareatravel','current_quarantined_total','current_hosp_resident',
         'ndiff_conf','ndiff_released','ndiff_deceased','ndiff_confirmed_non_resident','test_date']]

print(f'Keeping only latest record...')
df_latest = df.head(1)
latest_date = df_latest['date'][0]
export_filename = os.path.join(credentials.path, credentials.filename).replace('.csv', '_' + latest_date + '.csv')
print(f'Exporting csv to {export_filename}')
df_latest.to_csv(export_filename, index=False)

common.upload_ftp(export_filename, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'covid19bs/daily')
print('Job successful!')
