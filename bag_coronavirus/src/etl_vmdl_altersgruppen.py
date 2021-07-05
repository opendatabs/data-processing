import logging
import shutil
import numpy
import pandas as pd
import os
import common
from pandasql import sqldf
from bag_coronavirus import credentials
from bag_coronavirus.src import vmdl
import common.change_tracking as ct
import ods_publish.etl_id as odsp


def main():
    pysqldf = lambda q: sqldf(q, globals())
    vmdl_copy_path = vmdl.file_path().replace('vmdl.csv', 'vmdl_altersgruppen.csv')
    logging.info(f'Copying vmdl csv for this specific job to {vmdl_copy_path}...')
    shutil.copy(vmdl.file_path(), vmdl_copy_path)
    if ct.has_changed(vmdl_copy_path):
        df_bs_long_all = get_raw_df(file_path=vmdl_copy_path, bins=get_age_group_periods())
        df_bs_perc = get_reporting_df(file_path=vmdl_copy_path, bins=get_age_group_periods())
        for dataset in [
            {'dataframe': df_bs_long_all, 'filename': f'vaccinations_by_age_group.csv', 'ods_id': '100135'},
            {'dataframe': df_bs_perc, 'filename': f'vaccination_report_bs_age_group_long.csv', 'ods_id': '100137'}
        ]:
            export_file_name = os.path.join(credentials.vmdl_path, dataset['filename'])
            print(f'Exporting resulting data to {export_file_name}...')
            dataset['dataframe'].to_csv(export_file_name, index=False)
            common.upload_ftp(export_file_name, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'bag/vmdl')
            odsp.publish_ods_dataset_by_id(dataset['ods_id'])
    else:
        logging.info(f'Data have not changed, doing nothing ({vmdl_copy_path})')
    print(f'Job successful!')


def get_age_group_periods() -> list:
    return [
        {
            'from_date':  '2020-12-01',
            'until_date': '2021-06-27',
            'bins':      [numpy.NINF, 15,     49,         64,         74,         numpy.inf],
            'labels':    ['Unbekannt',       '16-49',    '50-64',    '65-74',    '> 74']

        },
        {
            'from_date':  '2021-06-28',
            'until_date': '2099-12-31',
            'bins':      [numpy.NINF, 11,        15,         49,         64,         74,         numpy.inf],
            'labels':    ['Unbekannt',           '12-15',    '16-49',    '50-64',    '65-74',    '> 74']
        }
    ]


def get_raw_df(file_path, bins) -> pd.DataFrame:
    raw_df = pd.DataFrame()
    logging.info(f'Iterating over all bins (periods), calculating raw_df for each period, and concatenating them...')
    for bin_def in bins:
        partial_raw_df = get_partial_raw_df(file_path, bin_def)
        raw_df = raw_df.append(other=partial_raw_df, ignore_index=True)
    return raw_df


def get_partial_raw_df(file_path: str, bin_def: dict) -> pd.DataFrame:
    print(f'Reading data into dataframe...')
    df = pd.read_csv(file_path, sep=';')
    # df['vacc_date_dt'] = pd.to_datetime(df.vacc_date, format='%Y-%m-%dT%H:%M:%S.%f%z')
    df['vacc_day'] = df.vacc_date.str.slice(stop=10)

    print(f'Executing calculations...')
    print(f"Filter by BS and vacc_date < {vmdl.today_string()}, and vacc_date between {bin_def['from_date']} and {bin_def['until_date']}...")
    df_bs = sqldf(f'''
        select * 
        from df 
        where person_residence_ctn = "BS" and vacc_day < "{vmdl.today_string()}"
        and vacc_day >= "{bin_def['from_date']}" and vacc_day <= "{bin_def['until_date']}"; 
    ''')

    print(f'Calculating age groups. Cases where (age < minimal_age) currently must be errors, replacing with "Unbekannt"...')
    df_bs['age_group'] = pd.cut(df_bs.person_age, bins=bin_def['bins'], labels=bin_def['labels'], include_lowest=True)

    latest_vacc_day = vmdl.yesterday_string() if vmdl.yesterday_string() < bin_def['until_date'] else bin_def['until_date']
    logging.info(f'Creating all combinations of days until {latest_vacc_day}...')
    df_all_days = pd.DataFrame(data=pd.date_range(start=df_bs.vacc_day.min(), end=latest_vacc_day).astype(str), columns=['vacc_day'])
    df_all_days_indexed = df_all_days.set_index('vacc_day', drop=False)

    print(f'Create empty table of all combinations for long df...')
    df_labels = pd.DataFrame(bin_def['labels'], columns=['age_group'])
    df_vacc_count = pd.DataFrame([1, 2], columns=['vacc_count'])
    df_all_comb = sqldf('select * from df_all_days cross join df_labels cross join df_vacc_count;')

    print(f'Creating long table...')
    df_bs_long = sqldf('''
        select vacc_day, age_group, vacc_count, count(*) as count
        from df_bs
        group by vacc_day, age_group, vacc_count
        order by vacc_day desc;
    ''')

    print(f'Adding days without vaccinations to long df...')
    df_bs_long_all = df_all_comb.merge(df_bs_long, on=['vacc_day', 'age_group', 'vacc_count'], how='outer').fillna(0)
    return df_bs_long_all


def get_reporting_df(file_path, bins) -> pd.DataFrame:
    reporting_df = pd.DataFrame()
    logging.info(f'Iterating over all bins (periods), calculating raw_df reporting_df for each period, appending them...')
    for bin_def in bins:
        partial_raw_df = get_partial_raw_df(file_path, bin_def)
        partial_reporting_df = get_partial_reporting_df(partial_raw_df, bin_def)
        reporting_df = reporting_df.append(other=partial_reporting_df, ignore_index=True)

    print(f'Calculating cumulative sums...')
    # See https://stackoverflow.com/a/32847843
    df_bs_cum = reporting_df.copy(deep=True)
    df_bs_cum['count_cum'] = (df_bs_cum
                              .sort_values(by=['vacc_day'])
                              .groupby(['age_group', 'vacc_count'])['count']
                              .cumsum())

    print(f'Calculating cumulative sums of _only_ first vaccinations...')
    df_only_first = df_bs_cum.copy(deep=True)
    # Negate cumulative numbers of 2nd vacc, then sum cum numbers of vacc 1 and 2 to get the cum number of _only_ 1st vacc
    df_only_first.count_cum = numpy.where(df_only_first.vacc_count == 2, df_only_first.count_cum * -1, df_only_first.count_cum)
    df_only_first = df_only_first.groupby(['vacc_day', 'age_group'])['count_cum'].sum().reset_index()
    df_only_first['vacc_count'] = -1
    df_bs_cum = df_bs_cum.append(df_only_first)

    print(f'Adding explanatory text for vacc_count...')
    vacc_count_desc = pd.DataFrame.from_dict({'vacc_count': [-1, 1, 2],
                                              'vacc_count_description': ['Ausschliesslich erste Impfdosis', 'Erste Impfdosis', 'Zweite Impfdosis']})
    df_bs_cum = df_bs_cum.merge(vacc_count_desc, on=['vacc_count'], how='left')

    df_bs_perc = pd.DataFrame()
    for bin_def in bins:
        df_pop_age_group = get_pop_data(bin_def)

        from_date = bin_def["from_date"]
        until_date = bin_def["until_date"]
        print(f'Joining pop data and calculating percentages for period between {from_date} and {until_date}...')
        df_partial_cum = df_bs_cum.query('vacc_day >= @from_date and vacc_day <= @until_date')
        df_partial_perc = df_partial_cum.merge(df_pop_age_group, on=['age_group'], how='left')
        df_partial_perc['count_cum_percentage_of_total_pop'] = df_partial_perc.count_cum / df_partial_perc.total_pop * 100
        df_bs_perc = df_bs_perc.append(df_partial_perc, ignore_index=True)

    return df_bs_perc


def get_partial_reporting_df(df_bs_long_all, bin_def) -> pd.DataFrame:
    print(f'Calculating age group "Gesamtbevölkerung"...')
    df_all_ages = df_bs_long_all.copy(deep=True)
    df_all_ages = df_all_ages.groupby(['vacc_day', 'vacc_count']).sum().reset_index()
    df_all_ages['age_group'] = 'Gesamtbevölkerung'
    df_bs_long_all = df_bs_long_all.append(df_all_ages)
    print('calculating age group "Impfberechtigte Bevölkerung"...')
    df_vacc_allowed = df_all_ages.copy()
    df_vacc_allowed['age_group'] = 'Impfberechtigte Bevölkerung'
    df_bs_long_all = df_bs_long_all.append(df_vacc_allowed)
    return df_bs_long_all


def get_pop_data(bin_def):
    # Retrieve data from https://data.bs.ch/explore/dataset/100128
    print(f'Retrieving population data from {credentials.pop_data_file_path}')
    df_pop = common.pandas_read_csv(credentials.pop_data_file_path, sep=';')
    print(f'Filter 2020-12-31 data, create age groups, and sum')
    df_pop_2020 = df_pop.loc[df_pop['datum'] == '2020-12-31'][['person_alter', 'anzahl']]
    df_pop_2020['age_group'] = pd.cut(df_pop_2020.person_alter, bins=bin_def['bins'], labels=bin_def['labels'], include_lowest=True)
    df_pop_age_group = df_pop_2020.groupby(['age_group'])['anzahl'].sum().reset_index().rename(columns={'anzahl': 'total_pop'})
    print(f'Removing "Unbekannt" age group from population dataset...')
    df_pop_age_group = df_pop_age_group.query('age_group != "Unbekannt"')
    print(f'Calculating count of age group "Impfberechtige Bevölkerung"...')
    df_pop_vacc_allowed = pd.DataFrame({'age_group': 'Impfberechtigte Bevölkerung', 'total_pop': df_pop_age_group.total_pop.sum()}, index=[0])
    print(f'Calculating count of age group "Gesamtbevölkerung"...')
    df_pop_total = pd.DataFrame({'age_group': 'Gesamtbevölkerung', 'total_pop': df_pop_2020.anzahl.sum()}, index=[0])
    print(f'Appending totals for "Impfberechtigte Bevölkerung" and "Gesamtbevölkerung" ')
    df_pop_age_group = df_pop_age_group.append(df_pop_vacc_allowed).append(df_pop_total)
    return df_pop_age_group


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
