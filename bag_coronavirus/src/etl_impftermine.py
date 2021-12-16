import logging
import numpy
import common
import common.change_tracking as ct
import ods_publish.etl_id as odsp
import datetime
import os
import glob
import openpyxl
from datetime import datetime
from bag_coronavirus import credentials
import pandas as pd


def main():
    logging.info(f'Checking for new data...')
    latest_data_file = list(sorted(get_data_files_list(), reverse=True))[0]
    if ct.has_changed(latest_data_file, do_update_hash_file=False):
        logging.info(f'New data found.')
        df = load_data()
        df, df_agg = transform(df)
        export_data(df, df_agg)
        odsp.publish_ods_dataset_by_id('100136')
        ct.update_hash_file(latest_data_file)
    logging.info(f'Job successful!')


def transform(df):
    df = clean_parse(df)
    df = calculate_age(df=df, bin_defs=get_age_group_periods())
    df = df.append(calculate_missing_dates(df, find_missing_dates(df)))
    (df, df_agg) = filter_aggregate(df)
    return df, df_agg


def find_missing_dates(df):
    logging.info(f'Finding missing days...')
    existing_dates = df.date.unique()
    d = pd.DataFrame(data=existing_dates, index=existing_dates, columns=['date'])
    dr = pd.date_range(df.creation_day.min(), df.date.max())
    # find missing days using reindex, see https://stackoverflow.com/a/19324591
    missing_dates = d.reindex(dr).query('date.isnull()').index.to_list()
    logging.info(f'Missing days: {missing_dates}')
    return missing_dates


def calculate_missing_dates(df, missing_dates):
    df_calc = pd.DataFrame()
    for date in missing_dates:
        date_text = date.strftime("%Y-%m-%d")
        # find next existing dataset after a missing day
        # filter out calculated days, they have has_appointments set to Unknown
        date_of_next = df.query(f'has_appointments != "Unknown" and date > @date_text').date.min()
        date_of_next_text = date_of_next.strftime("%Y-%m-%d")
        df_for_calc = df.query(f'date == @date_of_next_text').reset_index(drop=True).copy(deep=True)
        df_single_day = calc_missing_date(day=date, df_for_calc=df_for_calc)
        logging.info(f'Calculated missing day {date_text} with {len(df_single_day)} rows using dataset of {date_of_next_text}...')
        df_calc = df_calc.append(df_single_day)
    return df_calc


def calc_missing_date(day, df_for_calc):
    day_text = day.strftime('%Y-%m-%d')
    df_then = df_for_calc.query(f'creation_day <= @day_text').reset_index(drop=True)
    # Set date to the day we are currently analysing, so we can treat these data as if we had a data export from that day
    df_then.date = day
    # We don't know when people received their appointment in retrospect, so set has_appointments to "Unknown"
    df_then.has_appointments = 'Unknown'
    # logging.info(f'Calculated day {day_text} with {len(df_then)} rows...')
    return df_then


def load_data():
    files = sorted(get_data_files_list())
    df = pd.DataFrame()
    for f in files:
        logging.info(f'Get date from filename...')
        file_date = datetime.strptime(f[-15:][:-5], '%Y-%m-%d')
        logging.info(f'Read data from {f}, add file date {file_date} as a new column...')
        df_single = pd.read_excel(f)
        df_single['date'] = file_date
        logging.info(f'Appending...')
        df = df.append(df_single)
    return df


def get_data_files_list():
    return glob.glob(os.path.join(credentials.impftermine_path, "users-minimum-info2-????-??-??.xlsx"))


def clean_parse(df):
    logging.info(f'Dropping rows with no Birthdate, parsing dates...')
    df = df.dropna(subset=['Birthdate']).reset_index(drop=True)
    df['birthday'] = pd.to_datetime(df.Birthdate)
    df['creation_day'] = pd.to_datetime(df['Creation date'], format='%d.%m.%Y')
    df['appointment_1_dt'] = pd.to_datetime(df['Appointment 1'], format='%Y-%m-%d %H:%M:%S')
    df['appointment_2_dt'] = pd.to_datetime(df['Appointment 2'], format='%Y-%m-%d %H:%M:%S')
    return df


def calculate_age(df, bin_defs):
    logging.info(f'Calculating age...')
    # df['age'] = [relativedelta(a, b).years for a, b in zip(df.date, df.birthday)]
    df['age'] = (df.date - df.birthday).astype('timedelta64[Y]')
    age_group_df = pd.DataFrame()
    logging.info(f'Iterating over age_group periods...')
    for bin_def in bin_defs:
        from_date = bin_def['from_date']
        until_date = bin_def['until_date']
        temp_df = df.query('date >= @from_date and date <= @until_date').reset_index(drop=True)
        logging.info(f'Treating period between {from_date} and {until_date}...')
        temp_df = calculate_age_group(df=temp_df, bins=bin_def['bins'], labels=bin_def['labels'])
        age_group_df = age_group_df.append(temp_df)
    return age_group_df


def calculate_age_group(df, bins, labels):
    logging.info(f'Calculating age group...')
    df['age_group'] = pd.cut(df.age, bins=bins, labels=labels, include_lowest=True)
    df = df.rename(columns={'Has appointments': 'has_appointments',
                            'Appointment 1': 'appointment_1',
                            'Appointment 2': 'appointment_2',
                            'Creation date': 'creation_date'
                            })
    return df


def filter_aggregate(df):
    logging.info(f'Keeping only entries that have not had their first vaccination...')
    df = df.query('appointment_1.isnull() or date < appointment_1_dt').reset_index()
    logging.info(f'Aggregating data...')
    df_agg = (df.groupby(['date', 'age_group', 'has_appointments'])
                .agg(len)
                .reset_index()
                .rename(columns={'age': 'count'})[['date', 'age_group', 'has_appointments', 'count']])
    logging.info(f'Filtering age_group "Unbekannt"...')
    df_agg = df_agg[df_agg.age_group != 'Unbekannt']
    logging.info(f'Removing lines with no counts...')
    df_agg = df_agg.dropna(subset=['count']).reset_index(drop=True)
    logging.info(f'Adding week of year...')
    df_agg['week'] = df_agg['date'].dt.isocalendar().week
    logging.info(f'Making sure only certain columns are exported...')
    df_agg = df_agg[['date', 'age_group', 'has_appointments', 'count', 'week']]
    return df, df_agg


def get_age_group_periods() -> list:
    return [
        {
            'from_date':  '2020-12-01',
            'until_date': '2021-06-24',
            'bins':      [numpy.NINF, 15,     49,         64,         74,         numpy.inf],
            'labels':    ['Unbekannt',       '16-49',    '50-64',    '65-74',    '> 74']

        },
        {
            'from_date':  '2021-06-25',
            'until_date': '2099-12-31',
            'bins':      [numpy.NINF, 11,        15,         49,         64,         74,         numpy.inf],
            'labels':    ['Unbekannt',           '12-15',    '16-49',    '50-64',    '65-74',    '> 74']
        }
    ]


def export_data(df, df_agg):
    logging.info(f'Exporting resulting data to {agg_export_file_name()}...')
    df_agg.to_csv(agg_export_file_name(), index=False)
    common.upload_ftp(agg_export_file_name(), credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'md/covid19_vacc')
    # raw_export_file = os.path.join(credentials.impftermine_path, 'export', f'impftermine.csv')
    # logging.info(f'Exporting resulting data to {raw_export_file}...')
    # df[['date', 'Birthdate', 'birthday', 'age', 'age_group', 'has_appointments']].to_csv(raw_export_file, index=False)


def agg_export_file_name():
    """Path to aggregated calculated reporting file"""
    return os.path.join(credentials.impftermine_path, 'export', f'impftermine_agg.csv')


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
