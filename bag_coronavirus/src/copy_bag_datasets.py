import logging
from bag_coronavirus import credentials
import os
import common
import pandas as pd
import ods_publish.etl_id as odsp


def main():
    datasets = get_dataset_metadata()
    for dataset in datasets:
        name = dataset['name']
        df_raw = extract(url=dataset['base_path'][name])
        df_transformed = transform(df_raw, dataset['suffix'])
        export_file_name = load(name, df_transformed, dataset['suffix'])
        common.upload_ftp(export_file_name, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'bag')
        odsp.publish_ods_dataset_by_id(dataset['ods_id'])
    logging.info(f'Job successful!')


def get_dataset_metadata():
    logging.info(f"Getting today's data url...")
    context_json = common.requests_get(url='https://www.covid19.admin.ch/api/data/context').json()
    # path_base_csv = context_json['sources']['individual']['csv']
    path_base_csv_daily = context_json['sources']['individual']['csv']['daily']
    # path_base_csv_weeklyVacc_byAge = context_json['sources']['individual']['csv']['weeklyVacc']['byAge']
    # path_base_csv_weeklyVacc_bySex = context_json['sources']['individual']['csv']['weeklyVacc']['bySex']
    datasets = [
        # {'name': 'vaccDosesAdministered', 'base_path': path_base_csv_weeklyVacc_byAge, 'suffix': 'weekly_byAge'},
        # {'name': 'vaccDosesAdministered', 'base_path': path_base_csv_weeklyVacc_bySex, 'suffix': 'weekly_bySex'},
        {'name': 'testPcrAntigen', 'base_path': path_base_csv_daily, 'suffix': '', 'ods_id': '100116'},
        {'name': 'hospCapacity', 'base_path': path_base_csv_daily, 'suffix': '', 'ods_id': '100119'},
        {'name': 'cases', 'base_path': path_base_csv_daily, 'suffix': '', 'ods_id': '100123'},
        # {'name': 'vaccDosesDelivered', 'base_path': path_base_csv, 'suffix': ''},
        # {'name': 'vaccDosesAdministered', 'base_path': path_base_csv, 'suffix': ''},
    ]
    return datasets


def extract(url):
    logging.info(f'Reading current csv from {url} into data frame...')
    df_raw = common.pandas_read_csv(url)
    return df_raw


def transform(df, suffix):
    logging.info(f'Checking which column contains the date...')
    date_column = 'datum' if 'datum' in df.columns else 'date'
    logging.info(f'Dropping lines with empty value in date column "{date_column}"...')
    logging.info(f'{df[date_column].isna()}')
    df = df.dropna(subset=[date_column])
    logging.info(f'Calculating columns...')
    if 'weekly' not in suffix:
        logging.info(f'Date column is regarded as being a calendar day, calculating dayofweek, wochentag, week...')
        df['dayofweek'] = pd.to_datetime(df[date_column]).dt.dayofweek
        df['wochentag'] = df['dayofweek'].apply(lambda x: common.weekdays_german[x])
        df['week'] = pd.to_datetime(df[date_column]).dt.week
    else:
        logging.info(f'Date column is regarded as being a week number. Calculating year, week...')
        df['year'] = df[date_column].astype(str).str.slice(stop=4)
        df['week'] = df[date_column].astype(str).str.slice(start=-2)
    return df


def load(dataset_name: str, df: pd.DataFrame, suffix: str) -> str:
    suffix_string = f'_{suffix}' if suffix != '' else ''
    export_file_name = os.path.join(credentials.path, f'covid19_{dataset_name}{suffix_string}.csv')
    logging.info(f'Exporting to file {export_file_name}...')
    df.to_csv(export_file_name, index=False)
    return export_file_name


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
