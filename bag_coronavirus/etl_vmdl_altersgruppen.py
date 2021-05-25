import numpy
import pandas as pd
import os
import common
from pandasql import sqldf
from bag_coronavirus import credentials
from bag_coronavirus import vmdl


def main():
    # file_path = vmdl_extract.retrieve_vmdl_data()
    file_path = vmdl.file_path()

    print(f'Reading data into dataframe...')
    df = pd.read_csv(file_path, sep=';')
    # df['vacc_date_dt'] = pd.to_datetime(df.vacc_date, format='%Y-%m-%dT%H:%M:%S.%f%z')
    df['vacc_day'] = df.vacc_date.str.slice(stop=10)

    print(f'Executing calculations...')
    pysqldf = lambda q: sqldf(q, globals())

    print(f'Filter by BS and vacc_date < {vmdl.today_string()}...')

    df_bs = sqldf(f'''
        select * 
        from df 
        where person_residence_ctn = "BS" and vacc_day < "{vmdl.today_string()}"''')

    print(f'Calculating age groups - age < 16 currently must be errors, replacing with "Unbekannt"...')
    bins =      [numpy.NINF, 15,     49,         64,         74,         numpy.inf]
    labels =    ['Unbekannt',       '16-49',    '50-64',    '65-74',    '> 74']
    df_bs['age_group'] = pd.cut(df_bs.person_age, bins=bins, labels=labels, include_lowest=True)

    print(f'Creating all combinations of days until {vmdl.yesterday_string()}...')
    df_all_days = pd.DataFrame(data=pd.date_range(start=df_bs.vacc_day.min(), end=vmdl.yesterday_string()).astype(str), columns=['vacc_day'])
    df_all_days_indexed = df_all_days.set_index('vacc_day', drop=False)

    print(f'Creating crosstab...')
    df_crosstab = pd.crosstab(df_bs.vacc_day, df_bs.age_group).sort_values(by='vacc_day', ascending=False)

    print(f'Adding all days without vaccinations to crosstab...')
    df_crosstab_all = df_crosstab.join(df_all_days_indexed, how='outer').fillna(0)
    # print(f'Reordering columns...')
    # cols = df_crosstab_all.columns.tolist()
    # cols = cols = cols[-1:] + cols[:-1]
    # df_crosstab_all = df_crosstab_all[cols]

    print(f'Calculating cumulative sums...')
    df_crosstab_cumsum = df_crosstab_all.cumsum().drop(columns=['vacc_day'], axis=1)

    print(f'Joining cumsums to crosstab...')
    df_pivot = df_crosstab_all.drop(columns=['vacc_day'])\
        .merge(df_crosstab_cumsum, on=['vacc_day'], how='outer', suffixes=(None, '_cumsum')).reset_index()

    print(f'Create empty table of all combinations for long df...')
    df_labels = pd.DataFrame(labels, columns=['age_group'])
    df_vacc_count = pd.DataFrame([1,2], columns=['vacc_count'])
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
    print(f'Calculating cumulative sums for long df...')
    # See https://stackoverflow.com/a/32847843
    df_bs_long_all['count_cum'] = df_bs_long_all.groupby(['age_group', 'vacc_count'])['count'].cumsum()

    # print(f'Calculating cumulative sums of _only_ first vaccinations...')
    # df_only_first = df_bs_long_all.copy(deep=True)
    # # Negate cumulative numbers of 2nd vacc, then sum cum numbers of vacc 1 and 2 to get the cum number of _only_ 1st vacc
    # df_only_first.count_cum = numpy.where(df_only_first.vacc_count == 2, df_only_first.count_cum * -1, df_only_first.count_cum)
    # df_only_first = df_only_first.groupby(['vacc_day', 'age_group'])['count_cum'].sum().reset_index()
    # df_only_first['vacc_count'] = -1
    # df_bs_long_all = df_bs_long_all.append(df_only_first)

    # Retrieve data from https://data.bs.ch/explore/dataset/100128
    print(f'Retrieving population data from {credentials.pop_data_file_path}')
    df_pop = common.pandas_read_csv(credentials.pop_data_file_path, sep=';')
    print(f'Filter 2020-12-31 data, create age groups, and sum')
    df_pop_2020 = df_pop.loc[df_pop['datum'] == '2020-12-31'][['person_alter', 'anzahl']]
    df_pop_2020['age_group'] = pd.cut(df_pop_2020.person_alter, bins=bins, labels=labels, include_lowest=True)
    df_pop_age_group = df_pop_2020.groupby(['age_group'])['anzahl'].sum().reset_index().rename(columns={'anzahl': 'total_pop'})
    print(f'Removing "Unbekannt" age group from population dataset...')
    df_pop_age_group = df_pop_age_group.query('age_group != "Unbekannt"')

    print(f'Joining pop data and calculating percentages...')
    df_bs_perc = df_bs_long_all.merge(df_pop_age_group, on=['age_group'], how='left')
    df_bs_perc['count_cum_percentage_of_total_pop'] = df_bs_perc.count_cum / df_bs_perc.total_pop * 100

    for dataset in [
        {'dataframe': df_pivot,         'filename': f'vaccination_report_bs_age_group.csv'},
        {'dataframe': df_bs_perc,   'filename': f'vaccination_report_bs_age_group_long.csv'}
    ]:
        export_file_name = os.path.join(credentials.vmdl_path, dataset['filename'])
        print(f'Exporting resulting data to {export_file_name}...')
        dataset['dataframe'].to_csv(export_file_name, index=False)
        common.upload_ftp(export_file_name, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'bag/vmdl')

        print(f'Job successful!')


if __name__ == "__main__":
    print(f'Executing {__file__}...')
    main()
