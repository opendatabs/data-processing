import common
import datetime
import numpy
import os
import glob
import openpyxl
from bag_coronavirus import credentials
from bag_coronavirus import etl_vmdl_altersgruppen as vmdl
import pandas as pd


def main():

    files = sorted(glob.glob(os.path.join(credentials.impftermine_path, "*.xlsx")))
    df = pd.DataFrame()
    for f in files:
        print(f'Reading data from {f}...')
        df_single = pd.read_excel(f)
        print(f'Get file modification datetime...')
        # file_date = datetime.datetime.fromtimestamp(os.path.getmtime(f)).strftime('%Y-%m-%d')
        # See https://stackoverflow.com/a/7983848
        file_date = f[-15:][:-5]
        print(f'Adding file date {file_date} as a new column...')
        df_single['date'] = file_date
        print(f'Appending dataset...')
        df = df.append(df_single)


    print(f'Calculating age, age group...')
    df['birthday'] = pd.to_datetime(df.Birthdate, format='%d.%m.%Y')
    df['age'] = (pd.to_datetime('now') - df.birthday).astype('<m8[Y]')
    df['age_group'] = pd.cut(df.age, bins=vmdl.get_age_groups()['bins'], labels=vmdl.get_age_groups()['labels'], include_lowest=True)
    df = df.rename(columns={'Has appointments': 'has_appointments'})

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

    export_file_name = os.path.join(credentials.impftermine_path, 'export', f'impftermine_agg.csv')
    print(f'Exporting resulting data to {export_file_name}...')
    df_agg.to_csv(export_file_name, index=False)
    common.upload_ftp(export_file_name, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'md/covid19_vacc')

    print(f'Job successful!')


if __name__ == "__main__":
    print(f'Executing {__file__}...')
    main()
