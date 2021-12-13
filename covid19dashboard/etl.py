from covid19dashboard import credentials
import pandas as pd
import os
import common
import common.change_tracking as ct
import ods_publish.etl_id as odsp


@common.retry(common.http_errors_to_handle, tries=6, delay=10, backoff=1)
def read_data_from_url(uri):
    return pd.read_csv(uri)


sourcefile = 'https://raw.githubusercontent.com/openZH/covid_19/master/COVID19_Fallzahlen_CH_total_v2.csv'
print(f'Reading data from {sourcefile}...')
df = read_data_from_url(sourcefile)
print('Getting rid of unnecessary columns...')
df.drop(columns=['time', 'source', 'ncumul_tested', 'new_hosp', 'current_vent'], inplace=True)

print('Calculating date range...')
df.date = pd.to_datetime(df.date)
date_range = pd.date_range(start=df.date.min(), end=df.date.max())

# https://skipperkongen.dk/2018/11/26/how-to-fill-missing-dates-in-pandas/
# https://stackoverflow.com/questions/52430892/forward-filling-missing-dates-into-python-pandas-dataframe
print('Iterating over each canton, sorting, adding missing dates, then filling the value gaps using ffill()...')
cantons = df.abbreviation_canton_and_fl.unique()
df_filled = pd.DataFrame(columns=df.columns)
for canton in cantons:
    print(f'Working through canton {canton}...')
    df_canton = df[df.abbreviation_canton_and_fl == canton].sort_values(by='date')
    df_canton_filled = df_canton.set_index('date').reindex(date_range).ffill().reset_index().rename(columns={'index': 'date'})

    print('Getting rid of rows with empty date...')
    df_canton_filled.dropna(subset=['abbreviation_canton_and_fl'], inplace=True)

    print('Calculating differences between rows in new columns...')
    df_canton_diff = df_canton_filled.drop(columns=['abbreviation_canton_and_fl']).diff()
    df_canton_filled['ndiff_conf'] = df_canton_diff.ncumul_conf
    df_canton_filled['ndiff_released'] = df_canton_diff.ncumul_released
    df_canton_filled['ndiff_deceased'] = df_canton_diff.ncumul_deceased

    df_filled = df_filled.append(df_canton_filled, ignore_index=True)

filename = os.path.join(credentials.path, credentials.filename)
print(f'Exporting data to {filename}')
df_filled.to_csv(filename, index=False)
if ct.has_changed(filename, do_update_hash_file=False):
    common.upload_ftp(filename, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'covid19dashboard')
    odsp.publish_ods_dataset_by_id('100085')
    ct.update_hash_file(filename)
print('Job successful!')