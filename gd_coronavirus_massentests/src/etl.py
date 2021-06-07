import glob
import os
import typing
import zipfile
import sqlite3
import logging
import pandas as pd
from pandasql import sqldf
import common
from gd_coronavirus_massentests.src import credentials

table_names = ['LaborGroupOrder', 'LaborSingleOrder']
TABLE_NAME = typing.Literal['LaborGroupOrder', 'LaborSingleOrder']


def pysqldf(q):
    return sqldf(q, globals())


def main():
    glob_string = os.path.join(credentials.data_path, "*.zip")
    date, dfs = extract_data(glob.glob(glob_string))
    add_global_dfs(dfs)
    convert_datetime_columns(dfs)
    # conn = create_db('../tests/fixtures/coronavirus_massentests.db', dfs)
    for report_def in [{'file_name': 'massentests_pool.csv', 'table_name': table_names[0]},
                    {'file_name': 'massentests_single.csv', 'table_name': table_names[1]}]:
        report = calculate_report(report_def['table_name'])
        export_file = os.path.join(credentials.export_path, report_def['file_name'])
        logging.info(f'Exporting data derived from table {report_def["table_name"]} to file {export_file}...')
        report.to_csv(export_file, index=False)
    # conn.close()


def calculate_report(table_name: TABLE_NAME) -> pd.DataFrame:
    """Calculate the reports based on raw data table name.
    """
    # todo: add column Kategorie
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
    return results_per_week


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


def extract_data(g: glob) -> (str, dict[str, pd.DataFrame]):
    """
    Load csv files from the first zip archive in the folder when ordered descending by name.
    Returns a dict of Pandas DataFrames with the DataFrame name as key.
    """
    archive_path = sorted(g, reverse=True)[0]
    date = os.path.basename(archive_path).split('_')[0]
    zf = zipfile.ZipFile(archive_path)
    dfs = {}
    for data_file in zf.namelist():
        df_name = data_file.split('.')[0]
        dfs[df_name] = pd.read_csv(zf.open(data_file), sep=';')
    return date, dfs


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
