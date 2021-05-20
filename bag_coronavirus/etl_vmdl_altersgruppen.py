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

    df_crosstab = pd.crosstab(df_bs.vacc_day, df_bs.age_group).sort_values(by='vacc_day', ascending=False)

    df_bs_long = sqldf('''
        select vacc_day, age_group, count(*) as vacc_count
        from df_bs
        group by vacc_day, age_group
        order by vacc_day desc;
    ''')

    print(f'Creating all combinations of days until {vmdl.yesterday_string()}...')
    df_all_days = pd.DataFrame(data=pd.date_range(start=df_bs.vacc_day.min(), end=vmdl.yesterday_string()).astype(str), columns=['vacc_day'])
    df_all_days_indexed = df_all_days.set_index('vacc_day', drop=False)
    print(f'Adding all days without vaccinations to crosstab...')
    df_crosstab_all = df_crosstab.join(df_all_days_indexed, how='outer').fillna(0)

    print(f'Create empty table of all combinations...')
    df_labels = pd.DataFrame(labels, columns=['age_group'])
    df_all_comb = sqldf('select * from df_all_days d cross join df_labels l;')
    print(f'Adding days without vaccinations to long df...')
    df_bs_long_all = df_all_comb.merge(df_bs_long, on=['vacc_day', 'age_group'], how='outer').fillna(0)

    for dataset in [
        {'dataframe': df_crosstab_all, 'filename': f'vaccination_report_bs_age_group.csv'},
        {'dataframe': df_bs_long_all,  'filename': f'vaccination_report_bs_age_group_long.csv'}
    ]:
        export_file_name = os.path.join(credentials.vmdl_path, dataset['filename'])
        print(f'Exporting resulting data to {export_file_name}...')
        dataset['dataframe'].to_csv(export_file_name, index=False)
        common.upload_ftp(export_file_name, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'bag/vmdl')

    print(f'Job successful!')


if __name__ == "__main__":
    print(f'Executing {__file__}...')
    main()
