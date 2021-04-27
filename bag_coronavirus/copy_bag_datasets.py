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
    df['dayofweek'] = pd.to_datetime(df[date_column]).dt.dayofweek
    df['wochentag'] = df['dayofweek'].apply(lambda x: common.weekdays_german[x])
    df['week'] = pd.to_datetime(df[date_column]).dt.week
    export_file_name = os.path.join(credentials.path, f'covid19_{dataset_name}.csv')
    print(f'Exporting to file {export_file_name}...')
    df.to_csv(export_file_name, index=False)
    common.upload_ftp(export_file_name, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'bag')


print(f"Getting today's data url...")
context_json = common.requests_get(url='https://www.covid19.admin.ch/api/data/context').json()
path_base_csv = context_json['sources']['individual']['csv']
path_base_csv_daily = context_json['sources']['individual']['csv']['daily']
datasets_daily = [{'name': 'testPcrAntigen',        'base_path': path_base_csv_daily,   'suffix': ''},
                  {'name': 'hospCapacity',          'base_path': path_base_csv_daily,   'suffix': ''},
                  {'name': 'cases',                 'base_path': path_base_csv_daily,   'suffix': ''},
                  {'name': 'vaccDosesDelivered',    'base_path': path_base_csv,         'suffix': ''},
                  {'name': 'vaccDosesAdministered', 'base_path': path_base_csv,         'suffix': ''},
                  {'name': 'fullyVaccPersons',      'base_path': path_base_csv,         'suffix': ''},
                  ]

for dataset in datasets_daily:
    name = dataset['name']
    get_bag_data(dataset_name=name, url=dataset['base_path'][name], suffix=dataset['suffix'])

print(f'Job successful!')
