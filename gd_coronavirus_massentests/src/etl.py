import glob
import os
import zipfile
import sqlite3
import logging
import pandas as pd
import xml.etree.ElementTree as Et
from pandasql import sqldf
import common
import common.change_tracking as ct
import ods_publish.etl_id as odsp

from gd_coronavirus_massentests.src import credentials


def get_report_defs():
    return [{'db_path': credentials.data_path_db,
             'report_defs': [{'file_name': 'massentests_pool_primarsek1.csv',
                              'table_name': 'LaborGroupOrder',
                              'anzahl_proben_colname': 'AnzahlProbenPrimarSek1',
                              'organization_count_colname': 'SchoolCount',
                              'ods_id': '100145'},
                             {'file_name': 'massentests_single_betriebe.csv',
                              'table_name': 'LaborSingleOrder',
                              'anzahl_proben_colname': 'AnzahlProbenPrimarSek1',
                              'organization_count_colname': 'BusinessCount',
                              'ods_id': '100146'}]},
            {'db_path': credentials.data_path_db_sek2,
             'report_defs': [{'file_name': 'massentests_single_sek2.csv',
                              'table_name': 'LaborSingleOrder',
                              'anzahl_proben_colname': 'AnzahlProbenSek2',
                              'organization_count_colname': 'SchoolCount',
                              'ods_id': '100153'}]}]


def pysqldf(q):
    return sqldf(q, globals())


def main():
    df_lab = None
    for db in get_report_defs():
        archive_path = get_latest_archive(glob.glob(os.path.join(db['db_path'], "*.zip")))
        if ct.has_changed(archive_path, False):
            if df_lab is None:
                logging.info(f'No lab data yet, processing...')
                common.download_ftp([], credentials.down_ftp_server, credentials.down_ftp_user, credentials.down_ftp_pass, credentials.down_ftp_dir, credentials.data_path_xml, '*.xml')
                df_lab = extract_lab_data(os.path.join(credentials.data_path_xml, "*.xml"))
            else:
                logging.info(f'Lab data already downloaded, no need to do it again. ')
            date, dfs = extract_db_data(archive_path)
            dfs['df_lab'] = df_lab
            add_global_dfs(dfs)
            convert_datetime_columns(dfs)
            # conn = create_db('../tests/fixtures/coronavirus_massentests.db', dfs)
            for report_def in db['report_defs']:
                report = calculate_report(table_name=report_def['table_name'],
                                          anzahl_proben_colname=report_def['anzahl_proben_colname'],
                                          organization_count_colname=report_def['organization_count_colname'])
                export_file = os.path.join(credentials.export_path, report_def['file_name'])
                logging.info(f'Exporting data derived from table {report_def["table_name"]} to file {export_file}...')
                report.to_csv(export_file, index=False)
                if ct.has_changed(export_file, False):
                    common.upload_ftp(export_file, credentials.up_ftp_server, credentials.up_ftp_user, credentials.up_ftp_pass, 'gd_gs/coronavirus_massenteststs')
                    odsp.publish_ods_dataset_by_id(report_def['ods_id'])
                    ct.update_hash_file(export_file)
            # conn.close()
            ct.update_hash_file(archive_path)
    logging.info(f'Job successful!')


def extract_lab_data(glob_string: str):
    """Extract lab data from xml files as a Pandas DataFrame"""
    lab_df = pd.DataFrame()
    for xml_file in glob.glob(glob_string):
        with open(xml_file, 'r') as f:
            xml_str = f.read()
            root = Et.fromstring(xml_str)
        data = []
        cols = []
        for i, child in enumerate(root):
            data.append(int(child.text))
            cols.append(child.tag)
        df = pd.DataFrame(data).T
        df.columns = cols
        lab_df = lab_df.append(df)
    lab_df.Datum = pd.to_datetime(lab_df.Datum, format='%Y%m%d')
    return lab_df


def get_latest_archive(g: glob):
    """
    Load csv files from the first archive file in the folder when ordered descending by name.
    :param g: file pattern
    :return: Path of archive file
    """
    archive_path = sorted(g, reverse=True)[0]
    return archive_path


def extract_db_data(archive_path: str) -> (str, dict[str, pd.DataFrame]):
    """
    Returns a dict of Pandas DataFrames with the DataFrame name as key.
    """
    date = os.path.basename(archive_path).split('_')[0]
    logging.info(f'Reading data from zip file {archive_path}...')
    zf = zipfile.ZipFile(archive_path)
    dfs = {}
    for data_file in zf.namelist():
        df_name = data_file.split('.')[0]
        dfs[df_name] = pd.read_csv(zf.open(data_file), sep=';')
    return date, dfs


def calculate_report(table_name, anzahl_proben_colname, organization_count_colname) -> pd.DataFrame:
    """Calculate the reports based on raw data table name."""
    # get start of week in sqlite using strftime: see https://stackoverflow.com/a/15810438
    results = sqldf(f'''
        select      strftime('%Y-%m-%d', ResultDate, 'weekday 0', '-6 day') as FirstDayOfWeek,
                    strftime("%W", ResultDate) as WeekOfYear,
                    Result,
                    count(*) as Count
        from        {table_name}
        where       Result in ("positiv", "negativ") and
                    ResultDate is not null and
                    WeekOfYear is not null
        group by    WeekOfYear, Result
    ''')
    total = sqldf('''
        select      WeekOfYear,
                    sum(Count) as CountTotal
        from        results
        group by    WeekOfYear    
    ''')
    positive = sqldf('''
        select      WeekOfYear,
                    sum(Count) as CountPositive
        from        results
        where       Result = 'positiv'
        group by    WeekOfYear
    ''')
    positivity_rate = sqldf('''
        select      total.WeekOfYear,
                    total.CountTotal,
                    coalesce(cast(positive.CountPositive as real) / total.CountTotal * 100, 0) as PositivityRatePercent
        from        total 
                    left join positive on total.WeekOfYear = positive.WeekOfYear
    
    ''')
    # Filter out data for current week since we publish on a weekly basis
    results_per_week = sqldf('''
       select       r.FirstDayOfWeek,
                    r.WeekOfYear,
                    r.Result,
                    r.Count,
                    p.CountTotal,
                    p.PositivityRatePercent
        from results r 
        left join positivity_rate p on r.WeekOfYear = p.WeekOfYear  
        where r.FirstDayOfWeek < strftime('%Y-%m-%d', 'now', 'weekday 0', '-6 day')
    ''')

    # Count the number of businesses or schools that take part in testing per week
    businesses_per_week = sqldf(f'''
        select
           strftime("%W", ResultDate) as WeekOfYear,
           count(distinct BusinessId) as {organization_count_colname}
        from
             LaborSingleOrder l left join
             employee e on l.PersonId = e.EmployeeId
        where 
            Result is not null and 
            ResultDate is not null
        group by WeekOfYear
        order by WeekOfYear desc
    ''')

    results_per_week_with_businesses = sqldf(f'''
        select r.*, b.{organization_count_colname}
        from results_per_week r left join businesses_per_week b on r.WeekOfYear = b.WeekOfYear
    ''')

    samples = sqldf(f'''
        select      strftime("%W", Datum) as WeekOfYear, 
                    sum({anzahl_proben_colname}) as CountSamples   
        from df_lab
        group by WeekOfYear
    ''')
    # Filter out data before 2021-09-06: Starting then the number of samples is delivered in a new format
    results_per_week_with_samples = sqldf('''
        select r.*, p.CountSamples 
        from results_per_week r left join samples p on r.WeekOfYear = p.WeekOfYear
        where r.FirstDayOfWeek >= date('2021-09-06')
        order by WeekOfYear 
    ''')
    # Return a different dataset for schools (LaborGroupOrder) vs. businesses (LaborSingleOrder)
    return results_per_week_with_samples if table_name == 'LaborGroupOrder' else results_per_week_with_businesses


def create_db(db, dfs):
    """Write Pandas DataFrames into database"""
    conn = sqlite3.connect(db)
    for key in dfs:
        dfs[key].to_sql(key, conn, if_exists='replace')
        conn.commit()
    return conn


def convert_datetime_columns(dfs):
    """Convert columns with names that hint to describe a date to type datetime"""
    date_column_hints = ['At', 'Am', 'datum', 'Date']
    for key in dfs:
        for column_name in dfs[key]:
            if any(x in column_name for x in date_column_hints):
                dfs[key][column_name] = pd.to_datetime(dfs[key][column_name], format='%m/%d/%Y %H:%M:%S', errors='coerce')


def add_global_dfs(dfs):
    """Create global variable for each df so they can be used in sqldf"""
    for key in dfs:
        globals()[key] = dfs[key]


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
