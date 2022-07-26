import logging
import shutil
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
    vmdl_copy_path = vmdl.file_path().replace('vmdl.csv', 'vmdl_impftyp.csv')
    logging.info(f'Copying vmdl csv for this specific job to {vmdl_copy_path}...')
    shutil.copy(vmdl.file_path(), vmdl_copy_path)
    # We don't check for changes here anymore to handle cases where no vacc have happened and we need to add a 0 line for yesterday
    # if ct.has_changed(vmdl_copy_path):
    logging.info(f'Reading vmdl file {vmdl_copy_path} into dataframe...')
    df = pd.read_csv(vmdl_copy_path, sep=';')
    df['vacc_day'] = df.vacc_date.str.slice(stop=10)
    types = [
        ['1',   'Impfung mit Einmaldosis (Grundimmunisierung)'],
        ['100', 'Genesen mit Einmaldosis'],
        ['22',  'Zweite Dosis mit einer Mehrdosisimpfung (Auffrischimpfung einer Impfung mit Einmaldosis)'],
        ['11',  'Erste Dosis einer Mehrdosisimpfung (Grundimmunisierung)'],
        ['12',  'Zweite Dosis einer Mehrdosisimpfung (Grundimmunisierung)'],
        ['19',  'Mindestens dritte Dosis einer Mehrdosisimpfung (Grundimmunisierung)'],
        ['29',  'Mindestens dritte Dosis einer Mehrdosisimpfung (Auffrischimpfung)'],
        ['39',  'Mindestens zweite Auffrischimpfung'],
        ['101', 'Genesen mit erster Dosis einer Mehrdosisimpfung'],
        ['102', 'Genesen mit zweiter Dosis einer Mehrdosisimpfung (Grundimmunisierung)'],
        ['202', 'Genesen mit zweiter Dosis einer Mehrdosisimpfung (Auffrischimpfung)'],
        ['-21', 'Fehleingabe: serie 2, vacc_count 1'],
        ['-1',  'Andere'],
    ]
    df_types = pd.DataFrame(types, columns=['type_id', 'type'])
    logging.info(f'Filtering BS population, determining vaccination type...')
    # vacc_id = 5413868120110 --> Johnson & Johnson Einmaldosis
    df_bs = sqldf(f'''
        select 
            *, -- vacc_day, vacc_count, serie,
            case                  
                when person_recovered_from_covid = 0.0 and vacc_count = 1 and vacc_id = 5413868120110 then '1'
                when person_recovered_from_covid = 1.0 and vacc_count = 1 and vacc_id = 5413868120110 then '100'
                when person_recovered_from_covid = 1.0 and serie = 1 and vacc_count = 1 and vacc_id <> 5413868120110 then '101'
                when person_recovered_from_covid = 1.0 and serie = 1 and vacc_count = 2 and vacc_id <> 5413868120110 then '102'
                when person_recovered_from_covid = 1.0 and serie = 2 and vacc_count = 2 and vacc_id <> 5413868120110 then '202'
                when serie = 2 and vacc_count = 2 and vacc_id <> 5413868120110 then '22'
                when serie = 1 and vacc_count = 1 and vacc_id <> 5413868120110 then '11'
                when serie = 1 and vacc_count = 2 and vacc_id <> 5413868120110 then '12'
                when serie = 1 and vacc_count > 2 and vacc_id <> 5413868120110 then '19'
                when serie = 2 and vacc_count > 2 and vacc_id <> 5413868120110 then '29'
                when serie > 2 and vacc_count > 2 then '39'
                when serie = 2 and vacc_count = 1 then '-21'
                else '-1'
            end as type_id
        from df 
        where person_residence_ctn = "BS" and vacc_day < "{vmdl.today_string()}" 
    ''')

    df_bs = df_bs.merge(df_types, on='type_id', how='left')

    logging.info(f'Calculating counts und cumulative sums...')
    # df_type_counts = (df_bs.groupby(['vacc_day', 'type_id', 'type']).size().to_frame('type_count').reset_index().sort_values(by='vacc_day'))
    df_type_counts = sqldf('''
        select vacc_day, type_id, type, count(vacc_day) as type_count
        from df_bs
        group by vacc_day, type_id, type
        order by vacc_day
    ''')
    df_type_counts['type_count_cumsum'] = df_type_counts[['type_id', 'type_count']].groupby(['type_id']).cumsum()

    logging.info(f'Adding days with no vaccinations...')
    # int columns become float once nan values are added
    df_date_range = pd.DataFrame(data=pd.date_range(start=df_bs.vacc_day.min(), end=df_bs.vacc_day.max()).astype(str), columns=['vacc_day'])
    df_filled = df_type_counts.merge(df_date_range, how='right', on='vacc_day')

    logging.info(f'Pivoting dataframe...')
    df_pivot = (
        df_filled.pivot(index='vacc_day', columns=['type_id', 'type'], values='type_count_cumsum')
        .fillna(method='ffill')  # fill holes with last value
        .fillna(0)  # replace existing nans to the beginning of a series with 0
        .reset_index()
        .sort_values(by='vacc_day')
        .convert_dtypes()
    )

    logging.info(f'Retrieving current population count...')
    # # Retrieve data from https://data.bs.ch/explore/dataset/100128
    # print(f'Retrieving population data from {credentials.pop_data_file_path}')
    # df_pop = common.pandas_read_csv(credentials.pop_data_file_path, sep=';')
    # pop_count = df_pop.query('datum == "2020-12-31"')['anzahl'].sum()
    # https://www.pxweb.bfs.admin.ch/pxweb/de/px-x-0102010000_101/px-x-0102010000_101/px-x-0102010000_101.px/table/tableViewLayout2/
    pop_count_statpop = 196735

    logging.info(f'Calculating type columns, differences and precentages...')
    teilw = df_pivot['11'].squeeze() - df_pivot['12'].squeeze()
    vollst = df_pivot['1'].squeeze() + df_pivot['100'].squeeze() + df_pivot['12'].squeeze() + df_pivot['101'].squeeze()
    aufgefr = df_pivot['22'].squeeze() + df_pivot['29'].squeeze() + df_pivot['-21'].squeeze()
    mind_1 = df_pivot['1'].squeeze() + df_pivot['100'].squeeze() + df_pivot['11'].squeeze() + df_pivot['101'].squeeze()
    neu_teilw = df_pivot['11'].diff()
    df_pivot.insert(1,  column='Vollstaendig geimpft', value=vollst)
    df_pivot.insert(2,  column='Teilweise geimpft', value=teilw)
    df_pivot.insert(3,  column='Impfung aufgefrischt', value=aufgefr)
    df_pivot.insert(4,  column='Mit mindestens einer Dosis geimpft', value=mind_1)
    df_pivot.insert(5,  column='Neu teilweise geimpft', value=neu_teilw)
    df_pivot.insert(6,  column='Neu vollstaendig geimpft', value=(df_pivot['Vollstaendig geimpft'].diff()))
    df_pivot.insert(7,  column='Neu Impfung aufgefrischt', value=(df_pivot['Impfung aufgefrischt'].diff()))

    df_pivot.insert(8,  column='Anteil vollstaendig geimpft an Wohnbevoelkerung', value=vollst/pop_count_statpop*100)
    df_pivot.insert(9,  column='Anteil teilweise geimpft an Wohnbevoelkerung', value=teilw/pop_count_statpop*100)
    df_pivot.insert(10, column='Anteil Impfung aufgefrischt an Wohnbevoelkerung', value=aufgefr/pop_count_statpop*100)
    df_pivot.insert(11, column='Anteil mit mindestens einer Dosis geimpft', value=mind_1/pop_count_statpop*100)
    df_pivot.insert(12, column='Bevoelkerungszahl STATPOP', value=pop_count_statpop)

    # df_pivot.insert(13, column='BFS Anteil vollstaendig geimpft an Wohnbevoelkerung', value=vollst/pop_count_statpop*100)
    # df_pivot.insert(14, column='BFS Anteil teilweise geimpft an Wohnbevoelkerung STATPOP', value=teilw/pop_count_statpop*100)
    # df_pivot.insert(15, column='BFS Anteil Impfung aufgefrischt an Wohnbevoelkerung STATPOP', value=aufgefr/pop_count_statpop*100)
    # df_pivot.insert(16, column='BFS Anteil mit mindestens einer Dosis geimpft STATPOP', value=mind_1/pop_count_statpop*100)
    # df_pivot.insert(17, column='BFS Bevoelkerungszahl', value=pop_count_statpop)

    logging.info(f'Cleaning up column names...')
    # Replace the 2-level multi-index column names with a string that concatenates both strings,
    # then remove trailing _, then replace space with _
    df_pivot.columns = ["_".join(str(c) for c in col).rstrip('_').replace(' ', '_') for col in df_pivot.columns.values]
    df_pivot = df_pivot.sort_values(by='vacc_day', ascending=False)

    logging.info(f'Filling nan values with 0, droping unwanted columns...')
    df_pivot = (df_pivot
                .fillna(0)
                .drop(columns=['nan_nan']))

    export_file_name = os.path.join(credentials.vmdl_path, 'vaccination_report_bs_impftyp.csv')
    logging.info(f'Exporting dataframe to file {export_file_name}...')
    df_pivot.to_csv(export_file_name, index=False, sep=';')
    if ct.has_changed(export_file_name):
        common.upload_ftp(export_file_name, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'bag/vmdl')
        odsp.publish_ods_dataset_by_id('100162')
        ct.update_hash_file(export_file_name)
    # ct.update_hash_file(vmdl_copy_path)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
