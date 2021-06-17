from bag_coronavirus import credentials
import os
import common
import pandas as pd


def get_bag_data(dataset_name, url, suffix):
    print(f'Reading current csv from {url} into data frame...')
    df = common.pandas_read_csv(url)
    print(f'Checking which column contains the date...')
    date_column = 'datum' if 'datum' in df.columns else 'date'
    print(f'Dropping lines with empty value in date column "{date_column}"...')
    print(f'{df[date_column].isna()}')
    df = df.dropna(subset=[date_column])
    print(f'Calculating columns...')
    if 'weekly' not in suffix:
        print(f'Date column is regarded as being a calendar day, calculating dayofweek, wochentag, week...')
        df['dayofweek'] = pd.to_datetime(df[date_column]).dt.dayofweek
        df['wochentag'] = df['dayofweek'].apply(lambda x: common.weekdays_german[x])
        df['week'] = pd.to_datetime(df[date_column]).dt.week
    else:
        print(f'Date column is regarded as being a week number. Calculating year, week...')
        df['year'] = df[date_column].astype(str).str.slice(stop=4)
        df['week'] = df[date_column].astype(str).str.slice(start=-2)
    suffix_string = f'_{suffix}' if suffix != '' else ''
    export_file_name = os.path.join(credentials.path, f'covid19_{dataset_name}{suffix_string}.csv')
    print(f'Exporting to file {export_file_name}...')
    df.to_csv(export_file_name, index=False)
    common.upload_ftp(export_file_name, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'bag')


print(f"Getting today's data url...")
context_json = common.requests_get(url='https://www.covid19.admin.ch/api/data/context').json()
path_base_csv = context_json['sources']['individual']['csv']
path_base_csv_daily = context_json['sources']['individual']['csv']['daily']
path_base_csv_weeklyVacc_byAge = context_json['sources']['individual']['csv']['weeklyVacc']['byAge']
path_base_csv_weeklyVacc_bySex = context_json['sources']['individual']['csv']['weeklyVacc']['bySex']
datasets = [
    {'name': 'vaccDosesAdministered',   'base_path': path_base_csv_weeklyVacc_byAge, 'suffix': 'weekly_byAge'},
    #{'name': 'fullyVaccPersons',        'base_path': path_base_csv_weeklyVacc_byAge, 'suffix': 'weekly_byAge'},
    {'name': 'vaccDosesAdministered',   'base_path': path_base_csv_weeklyVacc_bySex, 'suffix': 'weekly_bySex'},
    {'name': 'fullyVaccPersons',        'base_path': path_base_csv_weeklyVacc_bySex, 'suffix': 'weekly_bySex'},
    {'name': 'testPcrAntigen',          'base_path': path_base_csv_daily,   'suffix': ''},
    {'name': 'hospCapacity',            'base_path': path_base_csv_daily,   'suffix': ''},
    {'name': 'cases',                   'base_path': path_base_csv_daily,   'suffix': ''},
    {'name': 'vaccDosesDelivered',      'base_path': path_base_csv,         'suffix': ''},
    {'name': 'vaccDosesAdministered',   'base_path': path_base_csv,         'suffix': ''},
    {'name': 'fullyVaccPersons',        'base_path': path_base_csv,         'suffix': ''},
]

for dataset in datasets:
    name = dataset['name']
    get_bag_data(dataset_name=name, url=dataset['base_path'][name], suffix=dataset['suffix'])

print(f'Job successful!')
