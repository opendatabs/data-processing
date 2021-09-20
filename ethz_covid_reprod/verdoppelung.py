"""
sources:    Formulas:
            https://royalsociety.org/-/media/policy/projects/set-c/set-covid-19-R-estimates.pdf
            Estimate of generation time T:
            https://twitter.com/C_Althaus/status/1327572765784858625
            https://twitter.com/C_Althaus/status/1327567433142558725/photo/1
            data source:
            https://github.com/covid-19-Re/dailyRe-Data/blob/master/CHE-estimates.csv
"""
import logging
import common
import credentials
import os
# import requests
import numpy as np
import pandas as pd

def main():
    logging.info(f"Getting today's data url...")
    url = 'https://raw.githubusercontent.com/covid-19-Re/dailyRe-Data/master/CHE-estimates.csv'
    req = common.requests_get(url, proxies=credentials.proxies)
    with open(os.path.join(credentials.path, credentials.file_name), 'wb') as f:
        f.write(req.content)
    logging.info(f'Reading current csv into data frame...')
    df_R = common.pandas_read_csv(credentials.file_name)
    logging.info(f"Extending file...")
    export_file_name = os.path.join(credentials.path, credentials.file_name_new)
    return_data(export_file_name, df=df_R, T=4.8)

    """
    logging.info(f'Importing data...')
    latest_data_file = list(sorted(get_data_files_list(), reverse=True))[0]
    new_data = True
    # new_data = ct.has_changed(latest_data_file)
    if new_data:
        logging.info(f'New data found.')
        df = load_data()
        df, df_agg = transform(df)
        export_data(df, df_agg)
        # odsp.publish_ods_dataset_by_id('100136')
    else:
        logging.info(f'No new data found - doing nothing. ')
    logging.info(f'Job successful!')
    """





# growth rate r=(R-1)/T,  generation time T, abgeschätzt auf 4.8 (tweets Althaus)
def growth_rate(R, T=4.8):
    return (R-1)/T


# add growth rate columns to the data frame
def add_growth_rate(df, T=4.8):
    df['Mittlere growth rate'] = growth_rate(df['median_R_mean'], T)
    df['Obere Grenze growth rate'] = growth_rate(df['median_R_highHPD'], T)
    df['Untere Grenze growth rate'] = growth_rate(df['median_R_lowHPD'], T)
    return df


# verdopplung macht sinn wenn R>1 (equivalent: r>0), sonst verdoppelung=None
def verdoppelung(r):
    return np.where(r > 0, np.log(2)/r, None)


# add verdoppelung columns to the data frame
def add_verdoppelung(df, T=4.8):
    df = add_growth_rate(df, T)
    df['Mittlere Verdoppelung'] = verdoppelung(df['Mittlere growth rate'])
    df['Obere Grenze Verdoppelung'] = verdoppelung(df['Obere Grenze growth rate'])
    df['Untere Grenze Verdoppelung'] = verdoppelung(df['Untere Grenze growth rate'])
    return df


# export csv file with added growth rate and doubling time columns
def return_data(filename, df, T=4.8):
    df = add_verdoppelung(df, T)
    df.to_csv(filename)








