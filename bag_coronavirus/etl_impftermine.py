import common
import datetime
import os
import glob
import openpyxl
from datetime import datetime
from dateutil.relativedelta import relativedelta
from bag_coronavirus import credentials
from bag_coronavirus import etl_vmdl_altersgruppen as vmdl
import pandas as pd


def main():
    df = load_data()
    df = clean_parse(df)
    df = calculate_age(df)
    df_simulated = calculate_previous_data(df)
    print(f'Appending calculated previous data to retrieved data...')
    df = df.append(df_simulated)
    df, df_agg = filter_aggregate(df)
    export_data(df, df_agg)
    print(f'Job successful!')


def load_data():
    files = sorted(glob.glob(os.path.join(credentials.impftermine_path, "*.xlsx")))
    df = pd.DataFrame()
    for f in files:
        print(f'Get date from filename...')
        file_date = datetime.strptime(f[-15:][:-5], '%Y-%m-%d')
        print(f'Read data from {f}, add file date {file_date} as a new column, append...')
        df_single = pd.read_excel(f)
        df_single['date'] = file_date
        df = df.append(df_single)
    return df


def clean_parse(df):
    print(f'Dropping rows with no Birthdate, parsing dates...')
    df = df.dropna(subset=['Birthdate']).reset_index(drop=True)
    df['birthday'] = pd.to_datetime(df.Birthdate, format='%d.%m.%Y')
    df['creation_day'] = pd.to_datetime(df['Creation date'], format='%d.%m.%Y')
    df['appointment_1_dt'] = pd.to_datetime(df['Appointment 1'], format='%Y-%m-%d %H:%M:%S')
    df['appointment_2_dt'] = pd.to_datetime(df['Appointment 2'], format='%Y-%m-%d %H:%M:%S')
    return df


def calculate_age(df):
    print(f'Calculating age...')
    df['age'] = df.apply(lambda x: relativedelta(x['date'], x['birthday']).years, axis=1)
    print(f'Calculating age group...')
    df['age_group'] = pd.cut(df.age, bins=vmdl.get_age_groups()['bins'], labels=vmdl.get_age_groups()['labels'],
                             include_lowest=True)
    df = df.rename(columns={'Has appointments': 'has_appointments',
                            'Appointment 1': 'appointment_1',
                            'Appointment 2': 'appointment_2',
                            'Creation date': 'creation_date'
                            })
    return df


def calculate_previous_data(df):
    print(f'Calculating data for time before first data file...')
    min_date = df.date.min()
    min_date_text = min_date.strftime('%Y-%m-%d')
    ts_start = df.creation_day.min()
    ts_start_text = ts_start.strftime('%Y-%m-%d')
    days_before = pd.date_range(ts_start, min_date, closed='left')
    print(f'Using earliest dataset ({min_date_text}) for calculations...')
    df_before = df.query(f'date == "{min_date_text}"').reset_index(drop=True).copy(deep=True)
    # We don't know when people received their appointment in retrospect, so set has_appointments to "Unknown"
    df_before.has_appointments = 'Unknown'
    df_simulated = pd.DataFrame()
    for day in days_before:
        day_text = day.strftime('%Y-%m-%d')

        df_then = df_before.query(f'creation_day <= "{day_text}"').reset_index(drop=True)
        # Set date to the day we are currently analysing so we can treat these data as if we had a data export from that day
        df_then.date = day
        print(f'Calculating day {day_text} with {len(df_then)} rows...')
        df_simulated = df_simulated.append(df_then)
    return df_simulated


def filter_aggregate(df):
    print(f'Keeping only entries that have not had their first vaccination...')
    df = df.query('appointment_1.isnull() or date < appointment_1_dt').reset_index()
    print(f'Aggregating data...')
    df_agg = (df.groupby(['date', 'age_group', 'has_appointments'])
        .agg(len)
        .reset_index()
        .rename(columns={'age': 'count'})[['date', 'age_group', 'has_appointments', 'count']]
        )
    print(f'Filtering "Unbekannt"...')
    df_agg = df_agg[df_agg.age_group != 'Unbekannt']
    print(f'Making sure only certain columns are exported...')
    df_agg = df_agg[['date', 'age_group', 'has_appointments', 'count']]
    return df, df_agg


def export_data(df, df_agg):
    agg_export_file_name = os.path.join(credentials.impftermine_path, 'export', f'impftermine_agg.csv')
    print(f'Exporting resulting data to {agg_export_file_name}...')
    df_agg.to_csv(agg_export_file_name, index=False)
    common.upload_ftp(agg_export_file_name, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass,'md/covid19_vacc')
    raw_export_file = os.path.join(credentials.impftermine_path, 'export', f'impftermine.csv')
    print(f'Exporting resulting data to {raw_export_file}...')
    df[['date', 'Birthdate', 'birthday', 'age', 'age_group', 'has_appointments']].to_csv(raw_export_file, index=False)


if __name__ == "__main__":
    print(f'Executing {__file__}...')
    main()
