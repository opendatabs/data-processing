"""
sources:    Formulas:
            https://royalsociety.org/-/media/policy/projects/set-c/set-covid-19-R-estimates.pdf
            Estimate of generation time T:
            https://twitter.com/C_Althaus/status/1327572765784858625
            https://twitter.com/C_Althaus/status/1327567433142558725/photo/1
            data source:
            https://github.com/covid-19-Re/dailyRe-Data/blob/master/CHE-estimates.csv
"""
import credentials
import os
import requests
import numpy as np
import pandas as pd


# growth rate r=(R-1)/T,  generation time T, abgeschätzt auf 4.8 (tweets Althaus)
def growth_rate(R, T=4.8):
    return (R-1)/T


# add growth rate columns to the data frame
def add_growth_rate(df, T=4.8):
    df['Mittlere growth rate'] = growth_rate(df['Mittlere effektive Reproduktionszahl'], T)
    df['Obere Grenze growth rate'] = growth_rate(df['Obere Grenze der effektiven Reproduktionszahl'], T)
    df['Untere Grenze growth rate'] = growth_rate(df['Untere Grenze der effektiven Reproduktionszahl'], T)
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


if __name__ == '__main__':
    print(f"Getting today's data url...")
    url = 'https://raw.githubusercontent.com/covid-19-Re/dailyRe-Data/master/CHE-estimates.csv'
    req = requests.get(url, proxies=credentials.proxies)
    with open(os.path.join(credentials.path, credentials.file_name), 'wb') as f:
        f.write(req.content)
    print(f'Reading current csv into data frame...')
    df_R = pd.read_csv(credentials.file_name)
    print(f"Rename columns...")
    df_R.rename(columns={'median_R_mean': 'Mittlere effektive Reproduktionszahl', 'median_R_highHPD': 'Obere Grenze der effektiven Reproduktionszahl', 'median_R_lowHPD': 'Untere Grenze der effektiven Reproduktionszahl'}, inplace=True)
    print(f"Extending file...")
    export_file_name = os.path.join(credentials.path, credentials.file_name_new)
    return_data(export_file_name, df=df_R, T=4.8)






