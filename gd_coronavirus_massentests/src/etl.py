import glob
import os
import zipfile

import pandas as pd
from pandasql import sqldf
from gd_coronavirus_massentests.src import credentials


def pysqldf(q):
    return sqldf(q, globals())


def main():
    dfs = extract_data()
    add_global_dfs(dfs)
    convert_datetime_columns(dfs)

    pool_results_per_week = sqldf('''
        select 
            strftime("%W", lgo.ResultDate) as WeekOfYear, 
            lgo.Result, 
            count(lgo.ResultDate) as PoolCount
        from 
            LaborGroupOrder lgo 
        where 
            lgo.ResultDate is not null and 
            lgo.Result is not null
        group by WeekOfYear, Result 
        order by WeekOfYear, Result;
    ''')
    pass


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


def extract_data() -> dict[str, pd.DataFrame]:
    """
    Load csv files from the first zip archive in the folder when ordered descending by name.
    Returns a dict of Pandas DataFrames with the DataFrame name as key.
    """
    zf = zipfile.ZipFile(sorted(glob.glob(os.path.join(credentials.data_path, "*.zip")), reverse=True)[0])
    dfs = {}
    for data_file in zf.namelist():
        df_name = data_file.split('.')[0]
        dfs[df_name] = pd.read_csv(zf.open(data_file), sep=';')
    return dfs


if __name__ == "__main__":
    print(f'Executing {__file__}...')
    main()
    pass
