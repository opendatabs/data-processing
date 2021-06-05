import common
import datetime
import os
import glob
import openpyxl
from datetime import datetime
from bag_coronavirus import credentials
from bag_coronavirus import etl_vmdl_altersgruppen as vmdl
import pandas as pd


def main():
    df = load_data()
    df, df_agg = transform(df)
    export_data(df, df_agg)
    print(f'Job successful!')


def transform(df):
    df = clean_parse(df)
    df = calculate_age(df)
    df = df.append(calculate_missing_dates(df, find_missing_dates(df)))
    (df, df_agg) = filter_aggregate(df)
    return df, df_agg


def find_missing_dates(df):
    print(f'Finding missing days...')
    existing_dates = df.date.unique()
    d = pd.DataFrame(data=existing_dates, index=existing_dates, columns=['date'])
    dr = pd.date_range(df.creation_day.min(), df.date.max())
    # find missing days using reindex, see https://stackoverflow.com/a/19324591
    missing_dates = d.reindex(dr).query('date.isnull()').index.to_list()
    print(f'Missing days: {missing_dates}')
    return missing_dates


def calculate_missing_dates(df, missing_dates):
    df_calc = pd.DataFrame()
    for date in missing_dates:
        date_text = date.strftime("%Y-%m-%d")
        # find next existing dataset after a missing day
        # filter out calculated days, they have has_appointments set to Unknown
        date_of_next = df.query(f'has_appointments != "Unknown" and date > "{date_text}"').date.min()
        date_of_next_text = date_of_next.strftime("%Y-%m-%d")
        df_for_calc = df.query(f'date == "{date_of_next_text}"').reset_index(drop=True).copy(deep=True)
        df_single_day = calc_missing_date(day=date, df_for_calc=df_for_calc)
        print(f'Calculated missing day {date_text} with {len(df_single_day)} rows using dataset of {date_of_next_text}...')
        df_calc = df_calc.append(df_single_day)
    return df_calc


def calc_missing_date(day, df_for_calc):
    day_text = day.strftime('%Y-%m-%d')
    df_then = df_for_calc.query(f'creation_day <= "{day_text}"').reset_index(drop=True)
    # Set date to the day we are currently analysing so we can treat these data as if we had a data export from that day
    df_then.date = day
    # We don't know when people received their appointment in retrospect, so set has_appointments to "Unknown"
    df_then.has_appointments = 'Unknown'
    print(f'Calculated day {day_text} with {len(df_then)} rows...')
    return df_then


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
    # df['age'] = [relativedelta(a, b).years for a, b in zip(df.date, df.birthday)]
    df['age'] = (df.date - df.birthday).astype('timedelta64[Y]')
    print(f'Calculating age group...')
    df['age_group'] = pd.cut(df.age, bins=vmdl.get_age_groups()['bins'], labels=vmdl.get_age_groups()['labels'],
                             include_lowest=True)
    df = df.rename(columns={'Has appointments': 'has_appointments',
                            'Appointment 1': 'appointment_1',
                            'Appointment 2': 'appointment_2',
                            'Creation date': 'creation_date'
                            })
    return df


def filter_aggregate(df):
    print(f'Keeping only entries that have not had their first vaccination...')
    df = df.query('appointment_1.isnull() or date < appointment_1_dt').reset_index()
    print(f'Aggregating data...')
    df_agg = (df.groupby(['date', 'age_group', 'has_appointments'])
                .agg(len)
                .reset_index()
                .rename(columns={'age': 'count'})[['date', 'age_group', 'has_appointments', 'count']])
    print(f'Filtering age_group "Unbekannt"...')
    df_agg = df_agg[df_agg.age_group != 'Unbekannt']
    print(f'Removing lines with no counts...')
    df_agg = df_agg.dropna(subset=['count']).reset_index(drop=True)
    print(f'Making sure only certain columns are exported...')
    df_agg = df_agg[['date', 'age_group', 'has_appointments', 'count']]
    return df, df_agg


def export_data(df, df_agg):
    agg_export_file_name = os.path.join(credentials.impftermine_path, 'export', f'impftermine_agg.csv')
    print(f'Exporting resulting data to {agg_export_file_name}...')
    df_agg.to_csv(agg_export_file_name, index=False)
    common.upload_ftp(agg_export_file_name, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'md/covid19_vacc')
    raw_export_file = os.path.join(credentials.impftermine_path, 'export', f'impftermine.csv')
    print(f'Exporting resulting data to {raw_export_file}...')
    df[['date', 'Birthdate', 'birthday', 'age', 'age_group', 'has_appointments']].to_csv(raw_export_file, index=False)


if __name__ == "__main__":
    print(f'Executing {__file__}...')
    main()
