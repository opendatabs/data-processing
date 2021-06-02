import common
import datetime
import numpy
import os
import glob
import openpyxl
from datetime import datetime
from dateutil.relativedelta import relativedelta
from bag_coronavirus import credentials
from bag_coronavirus import etl_vmdl_altersgruppen as vmdl
import pandas as pd


def main():
    files = sorted(glob.glob(os.path.join(credentials.impftermine_path, "*.xlsx")))
    df = pd.DataFrame()
    for f in files:
        print(f'Get file modification datetime from filename...')
        file_date = datetime.strptime(f[-15:][:-5], '%Y-%m-%d')
        print(f'Read data from {f}, add file date {file_date} as a new column, append...')
        df_single = pd.read_excel(f)
        df_single['date'] = file_date
        df = df.append(df_single)

    print(f'Dropping rows with no Birthdate, calculating age and age group...')
    df = df.dropna(subset=['Birthdate']).reset_index()
    df['birthday'] = pd.to_datetime(df.Birthdate, format='%d.%m.%Y')
    df['age'] = df.apply(lambda x: relativedelta(x['date'], x['birthday']).years, axis=1)
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

    agg_export_file_name = os.path.join(credentials.impftermine_path, 'export', f'impftermine_agg.csv')
    print(f'Exporting resulting data to {agg_export_file_name}...')
    df_agg.to_csv(agg_export_file_name, index=False)
    common.upload_ftp(agg_export_file_name, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'md/covid19_vacc')

    raw_export_file = os.path.join(credentials.impftermine_path, 'export', f'impftermine.csv')
    print(f'Exporting resulting data to {raw_export_file}...')
    df[['date', 'Birthdate', 'birthday', 'age', 'age_group', 'has_appointments']].to_csv(raw_export_file, index=False)

    print(f'Job successful!')


if __name__ == "__main__":
    print(f'Executing {__file__}...')
    main()
