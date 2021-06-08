import glob
import os
import typing
import zipfile
import sqlite3
import logging
import pandas as pd
import xml.etree.ElementTree as et
from pandasql import sqldf
import common
from gd_coronavirus_massentests.src import credentials

table_names = ['LaborGroupOrder', 'LaborSingleOrder']
TABLE_NAME = typing.Literal['LaborGroupOrder', 'LaborSingleOrder']


def pysqldf(q):
    return sqldf(q, globals())


def main():
    date, dfs = extract_db_data(glob.glob(os.path.join(credentials.data_path_db, "*.zip")))
    dfs['df_lab'] = extract_lab_data(os.path.join(credentials.data_path_xml, "*.xml"))
    add_global_dfs(dfs)
    convert_datetime_columns(dfs)
    # conn = create_db('../tests/fixtures/coronavirus_massentests.db', dfs)
    for report_def in [{'file_name': 'massentests_pool.csv', 'table_name': table_names[0]},
                       {'file_name': 'massentests_single.csv', 'table_name': table_names[1]}]:
        report = calculate_report(report_def['table_name'])
        export_file = os.path.join(credentials.export_path, report_def['file_name'])
        logging.info(f'Exporting data derived from table {report_def["table_name"]} to file {export_file}...')
        report.to_csv(export_file, index=False)
        common.upload_ftp(export_file, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'gd_gs/coronavirus_massenteststs')
    # conn.close()


def extract_lab_data(glob_string: str):
    """Extract lab data from xml files as a Pandas DataFrame"""
    lab_df = pd.DataFrame()
    for xml_file in glob.glob(glob_string):
        with open(xml_file, 'r') as f:
            xml_str = f.read().replace(' BS', 'BS')
            root = et.fromstring(xml_str)
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


def extract_db_data(g: glob) -> (str, dict[str, pd.DataFrame]):
    """
    Load csv files from the first zip archive in the folder when ordered descending by name.
    Returns a dict of Pandas DataFrames with the DataFrame name as key.
    """
    archive_path = sorted(g, reverse=True)[0]
    date = os.path.basename(archive_path).split('_')[0]
    logging.info(f'Reading data from zip file {archive_path}...')
    zf = zipfile.ZipFile(archive_path)
    dfs = {}
    for data_file in zf.namelist():
        df_name = data_file.split('.')[0]
        dfs[df_name] = pd.read_csv(zf.open(data_file), sep=';')
    return date, dfs


def calculate_report(table_name: TABLE_NAME) -> pd.DataFrame:
    """Calculate the reports based on raw data table name."""
    # get start of week in sqlite using strftime: see https://stackoverflow.com/a/15810438
    results = sqldf(f'''
        select      strftime('%Y-%m-%d', ResultDate, 'weekday 0', '-6 day') as FirstDayOfWeek,
                    strftime("%W", ResultDate) as WeekOfYear,
                    Result,
                    count(*) as Count
        from        {table_name}
        where       Result is not null and
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
                    cast(positive.CountPositive as real) / total.CountTotal * 100 as PositivityRatePercent
        from        total 
                    left join positive on total.WeekOfYear = positive.WeekOfYear
    
    ''')
    results_per_week = sqldf('''
       select       r.FirstDayOfWeek,
                    r.WeekOfYear,
                    r.Result,
                    r.Count,
                    p.CountTotal,
                    p.PositivityRatePercent
        from results r 
        left join positivity_rate p on r.WeekOfYear = p.WeekOfYear    
    ''')
    samples = sqldf('''
        select      strftime("%W", Datum) as WeekOfYear, 
                    sum(AnzahlProben) as CountSamples   
        from df_lab
        group by WeekOfYear
    ''')
    results_per_week_with_samples = sqldf('''
        select r.*, p.CountSamples 
        from results_per_week r left join samples p on r.WeekOfYear = p.WeekOfYear
        order by WeekOfYear 
    ''')
    # Return column CountSamples only makes sense for LaborGroupOrder
    return results_per_week_with_samples if table_name == 'LaborGroupOrder' else results_per_week


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
                dfs[key][column_name] = pd.to_datetime(dfs[key][column_name], format='%m/%d/%Y %H:%M:%S')


def add_global_dfs(dfs):
    """Create global variable for each df so they can be used in sqldf"""
    for key in dfs:
        globals()[key] = dfs[key]


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
