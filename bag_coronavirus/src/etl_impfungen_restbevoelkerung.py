import logging
import os
import pandas as pd
from pandasql import sqldf
import etl_impftermine as impftermine
from bag_coronavirus import credentials


def main():
    pysqldf = lambda q: sqldf(q, globals())
    df_wl, df_impf = extract_data()
    df = calculate_report(df_wl, df_impf)
    filename = export_data(df)
    logging.info('Job successful!')


def export_data(df):
    export_file_name = os.path.join(credentials.vmdl_path, 'vacc_report_restbev.csv')
    logging.info(f'Exporting data to {export_file_name}...')
    df.to_csv(export_file_name, index=False)
    return export_file_name


def calculate_report(df_wl, df_impf):
    logging.info('Calculating peersons on waiting list, vaccinated, and population in age groups than can get vaccinated...')
    df_on_wl = sqldf('select date, sum(count) as auf_warteliste from df_wl group by date')
    df_geimpft = sqldf('''
        select 
            vacc_day, 
            max(case when vacc_count = -1 then count_cum end) as nur_erste_impfung, 
            max(case when vacc_count = 2 then count_cum end) as zweite_impfung, 
            max(case when vacc_count = 1 then count_cum end) as mind_erste_impfung, 
            total_pop from df_impf 
        where 
            age_group = "Impfberechtigte Bevölkerung" group by vacc_day
    ''')
    df_rest = sqldf('''
        select 
            w.date, 
            w.auf_warteliste, 
            i.nur_erste_impfung, 
            i.zweite_impfung, 
            i.mind_erste_impfung, 
            (i.total_pop - i.mind_erste_impfung- w.auf_warteliste ) as pop_rest, 
            i.total_pop as pop_impfberechtigt 
        from 
            df_on_wl w inner join 
            df_geimpft i on w.date = i.vacc_day
    ''')
    return df_rest


def extract_data():
    """Read data from datasets created by the other jobs run before this one."""
    df_wl = pd.read_csv(impftermine.agg_export_file_name())
    df_impf = pd.read_csv(os.path.join(credentials.vmdl_path, 'vaccination_report_bs_age_group_long.csv'))
    return df_wl, df_impf


if __name__ == "__main__":
    print(f'Executing {__file__}...')
    main()
