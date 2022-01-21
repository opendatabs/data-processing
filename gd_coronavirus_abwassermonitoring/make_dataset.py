"""
sources:
- population 2020 for normalisation: https://www.bfs.admin.ch/bfs/de/home/statistiken/bevoelkerung/stand-entwicklung/raeumliche-verteilung.assetdetail.18344310.html
- COVID data BS: https://data.bs.ch/
- COVID data BL: credentials.path_BL
- Abwasserdaten: credentials.path_proben
"""

import pandas as pd
from datetime import datetime
from gd_coronavirus_abwassermonitoring import credentials
import common
import logging

pop_BL = 66953
pop_BS = 196735

def main():
    df_BL = make_dataframe_BL()
    df_Abwasser = make_dataframe_abwasserdaten()
    df_all = df_Abwasser.merge(df_BL, how='right')
    df_BS = make_dataframe_BS()
    df_all = df_all.merge(df_BS, how='right')
    df_all = calculate_columns(df_all)
    df_all.to_csv(credentials.path_export_file)
    logging.info('Job successful!')


def make_column_dt(df, column):
    df[column] = pd.to_datetime(df[column])

def make_dataframe_BL():
    logging.info(f"import and transfrom BL data")
    path = credentials.path_BL
    df_BL = pd.read_csv(path, encoding="ISO-8859-1")
    # remove Gemeinde and Inc_7d
    df_BL.drop(columns=["Gemeinde", "Inc_7d"], inplace=True)
    # add suffix BL for all but date column
    df_BL = df_BL.add_suffix('_BL')
    df_BL.rename(columns={'Datum_BL':'Datum'}, inplace=True)
    # make datetime column
    make_column_dt(df_BL, "Datum")
    # sum over date to get total of all together per date
    df_BL = df_BL.groupby(by=["Datum"], as_index=False).sum()
    return df_BL

def make_dataframe_abwasserdaten():
    logging.info("import and transform abwasserdaten")
    path = credentials.path_proben
    df_Abwasser = pd.read_excel(path, sheet_name="Proben", usecols="A, B, N, O, AC, AD, AJ, AK", skiprows=range(6))
    # rename date column and change format
    df_Abwasser.rename(columns={'Abwasser von Tag':'Datum'}, inplace=True)
    return df_Abwasser

def make_dataframe_BS():
    logging.info(f"import, transform and merge BS data")
    # get number of cases and 7d inz.
    req = common.requests_get("https://data.bs.ch/api/v2/catalog/datasets/100108/exports/json?order_by=test_datum&select=test_datum&select=faelle_bs&select=inzidenz07_bs")
    # todo: Add .raise_for_status() after each request to make sure an error is raised on http errors
    req.raise_for_status()
    file = req.json()
    df_zahlen_BS = pd.DataFrame.from_dict(file)
    df_zahlen_BS.rename(columns={'test_datum': 'Datum'}, inplace=True)
    make_column_dt(df_zahlen_BS, "Datum")
    # get hosp, ips, deceased and isolated BS
    req = common.requests_get("https://data.bs.ch/api/v2/catalog/datasets/100073/exports/json?order_by=timestamp&select=timestamp&select=current_hosp&select=current_icu&select=ndiff_deceased&select=current_isolated")
    file = req.json()
    df_hosp = pd.DataFrame.from_dict(file)
    df_hosp.rename(columns={'timestamp': 'Datum'}, inplace=True)
    df_hosp['Datum'] = pd.to_datetime(df_hosp['Datum']).dt.date
    make_column_dt(df_hosp, "Datum")
    # get positivity rate
    req = common.requests_get("https://data.bs.ch/api/v2/catalog/datasets/100094/exports/json?order_by=datum&select=datum&select=positivity_rate_percent")
    file = req.json()
    df_pos_rate = pd.DataFrame.from_dict(file)
    df_pos_rate.rename(columns={'datum': 'Datum'}, inplace=True)
    make_column_dt(df_pos_rate, "Datum")
    # get Effektive mittlere Reproduktionszahl, with estimate_type:Cori_slidingWindow and data_type=Confirmed cases
    req = common.requests_get("https://data.bs.ch/api/v2/catalog/datasets/100110/exports/json?refine=region:BS&refine=estimate_type:Cori_slidingWindow&refine=data_type:Confirmed+cases&order_by=date&select=date&select=median_r_mean")
    file = req.json()
    df_repr = pd.DataFrame.from_dict(file)
    df_repr.rename(columns={'date': 'Datum'}, inplace=True)
    make_column_dt(df_repr, "Datum")
    # join the datasets
    dfs = [df_zahlen_BS, df_hosp, df_pos_rate, df_repr]
    # dfs = [df_zahlen_BS, df_hosp]
    df_BS = pd.concat([df.set_index('Datum') for df in dfs], axis=1, join='outer').reset_index()
    # take date from 1 July 2021
    df_BS = df_BS[df_BS['Datum'] >= datetime(2021, 7, 1)]
    return df_BS

def calculate_columns(df):
    logging.info(f"calculate and add columns")
    df["pos_rate_BL"] = df["Anz_pos_BL"]/(df["Anz_pos_BL"]+df["Anz_neg_BL"]) * 100
    df["sum_7t_BL"] = df["Anz_pos_BL"].rolling(window=7).sum()
    df["7t_inz_BL"] = df["sum_7t_BL"]/pop_BL * 100000
    df["daily_cases_BS+BL"] = df["Anz_pos_BL"] + df["faelle_bs"]
    df["hospitalized_BS+BL"] = df["Anz_hosp_BL"] + df["current_hosp"]
    df["IC_BS+BL"] = df["Anz_icu_BL"] + df["current_icu"]
    df["death_BS+BL"] = df["Anz_death_BL"] + df["ndiff_deceased"]
    df["7t_inz_BS+BL"] = (df["7t_inz_BL"] * pop_BL + df["inzidenz07_bs"] * pop_BS)/(pop_BS + pop_BL)
    df["pos_rate_BS+BL"] = (df["pos_rate_BL"] * pop_BL + df["positivity_rate_percent"] * pop_BS)/(pop_BS + pop_BL)
    return df

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
