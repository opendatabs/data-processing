import common
import datetime
import numpy
import os
import openpyxl
from bag_coronavirus import credentials
import pandas as pd


def main():
    impftermine_file = os.path.join(credentials.impftermine_path, credentials.impftermine_file)
    print(f'Reading data from {impftermine_file}...')
    df = pd.read_excel(impftermine_file)

    print(f'Calculating age, age group...')
    df['birthday'] = pd.to_datetime(df.Birthdate, format='%d.%m.%Y')
    df['age'] = (pd.to_datetime('now') - df.birthday).astype('<m8[Y]')
    bins =      [numpy.NINF, 15,     49,         64,         74,         numpy.inf]
    labels =    ['Unbekannt',       '16-49',    '50-64',    '65-74',    '> 74']
    df['age_group'] = pd.cut(df.age, bins=bins, labels=labels, include_lowest=True)
    df = df.rename(columns={'Has appointments': 'has_appointments'})

    print(f'Aggregating data...')
    df_agg = df.groupby(['age_group', 'has_appointments']).agg(len).reset_index().rename(columns={'age': 'count'})[['age_group', 'has_appointments', 'count']]

    print(f'Filtering "Unbekannt"...')
    df_agg = df_agg[df_agg.age_group != 'Unbekannt']

    print(f'Get file modification datetime...')
    file_date = datetime.datetime.fromtimestamp(os.path.getmtime(impftermine_file)).strftime('%Y-%m-%d')
    df_agg['date'] = file_date
    df_agg = df_agg[['age_group', 'has_appointments', 'count', 'date']]

    export_file_name = os.path.join(credentials.impftermine_path, 'export', f'impftermine_agg_{file_date}.csv')
    print(f'Exporting resulting data to {export_file_name}...')
    df_agg.to_csv(export_file_name, index=False)
    common.upload_ftp(export_file_name, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'md/covid19_vacc')

    print(f'Job successful!')


if __name__ == "__main__":
    print(f'Executing {__file__}...')
    main()
