"""
sources:
- population 2020 for normalisation:
    https://www.bfs.admin.ch/bfs/de/home/statistiken/bevoelkerung/stand-entwicklung/raeumliche-verteilung.assetdetail.18344310.html
- COVID data BS: https://data.bs.ch/
- COVID data BL: credentials.path_BL
- Abwasserdaten: credentials.path_proben
"""

import os
import pandas as pd
from datetime import datetime
from gd_coronavirus_abwassermonitoring import credentials
from functools import reduce
import common
import logging
from common import change_tracking as ct


pop_BL = 66953
pop_BS = 196735


def main():
    df_bl = make_dataframe_bl()
    df_abwasser = make_dataframe_abwasserdaten()
    df_all = df_abwasser.merge(df_bl, how='outer')
    df_bs_2021 = make_dataframe_bs_2021_to_2023()
    df_bs_2023 = make_dataframe_bs_from_2023()
    df_bs = pd.concat([df_bs_2021, df_bs_2023])
    df_all = df_all.merge(df_bs, how='outer')
    df_all = calculate_columns(df_all)
    # change date format for json file
    df_all['Datum'] = df_all['Datum'].dt.strftime('%Y-%m-%d')
    df_all.to_csv(credentials.path_export_file, index=False)

    # make public dataset, remove empty rows
    df_public = df_all[["7-TageMEDIAN of E, N1, N2 pro Tag & 100'000 Pers.", "7t_median_BS+BL"]].dropna(how='all')
    df_datum = df_all[["Datum"]]
    df_public = df_datum.join(df_public, how='right')
    df_public.to_csv(credentials.path_export_file_public, index=False)

    if ct.has_changed(credentials.path_export_file):
        common.upload_ftp(credentials.path_export_file, credentials.ftp_server, credentials.ftp_user,
                          credentials.ftp_pass, 'gd_kantonslabor/covid19_abwassermonitoring')
        ct.update_hash_file(credentials.path_export_file)
        logging.info("push data to ODS realtime API")
        logging.info("push for dataset 100167")
        push_url = credentials.ods_live_realtime_push_url
        push_key = credentials.ods_live_realtime_push_key
        common.ods_realtime_push_df(df_all, url=push_url, push_key=push_key)
    if ct.has_changed(credentials.path_export_file_public):
        common.upload_ftp(credentials.path_export_file_public, credentials.ftp_server, credentials.ftp_user,
                          credentials.ftp_pass, 'gd_kantonslabor/covid19_abwassermonitoring')
        ct.update_hash_file(credentials.path_export_file_public)
        logging.info("push for dataset 100187")
        push_url2 = credentials.ods_live_realtime_push_url2
        push_key2 = credentials.ods_live_realtime_push_key2
        common.ods_realtime_push_df(df_public, url=push_url2, push_key=push_key2)


# Realtime API bootstrap data for dataset 100167:
# {
#     "datum": "2021-07-11",
#     "ba_nr": "Ba210336",
#     "mean_e_n1_n2_qty_l_ww": "0.5",
#     "7_tagemedian_of_e_n1_n2_mean": "0.5",
#     "n1_n2_e_normalized_pmmov": "0.5",
#     "7_tagemedian_of_pmmov_normalized_data": "0.5",
#     "e_n1_n2_pro_tag_und_100_000_pers_prorheno_260_449_einw": "0.5",
#     "7_tagemedian_of_e_n1_n2_pro_tag_100_000_pers": "0.5",
#     "anz_neg_bl": "1",
#     "anz_pos_bl": "1",
#     "anz_hosp_bl": "1",
#     "anz_icu_bl": "1",
#     "anz_death_bl": "1",
#     "anz_iso_bl": "1",
#     "faelle_bs": "1",
#     "inzidenz07_bs": "0.5",
#     "current_isolated": "1",
#     "current_hosp": "1",
#     "current_icu": "1",
#     "ndiff_deceased": "1",
#     "positivity_rate_percent": "0.5",
#     "median_r_mean": "0.5",
#     "pos_rate_bl": "0.5",
#     "sum_7t_bl": "1",
#     "7t_inz_bl": "0.5",
#     "daily_cases_bs_bl": "1",
#     "hospitalized_bs_bl": "1",
#     "ic_bs_bl": "1",
#     "death_bs_bl": "1",
#     "7t_inz_bs_bl": "0.5",
#     "pos_rate_bs_bl": "0.5",
#     "isolierte_bs_bl": "1",
#     "ratio_isolierte_daily_cases": "0.5",
#     "7t_median_bl": "0.5",
#     "7t_median_bs": "0.5",
#     "7t_median_bs_bl": "0.5"
# }

# Realtime API boostrap data for dataset 100187
#
# {
#     "datum": "2021-07-11",
#     "7_tagemedian_of_e_n1_n2_pro_tag_100_000_pers": "0.5",
#     "7t_median_bs_bl": "0.5"
# }

def make_column_dt(df, column):
    df[column] = pd.to_datetime(df[column])


def make_dataframe_bl():
    logging.info(f"import and transfrom BL data")
    path = credentials.path_BL
    df_bl = pd.read_csv(path, encoding="ISO-8859-1")
    # remove Gemeinde and Inc_7d
    df_bl.drop(columns=["Gemeinde", "Inc_7d"], inplace=True)
    # add suffix BL for all but date column
    df_bl = df_bl.add_suffix('_BL')
    df_bl.rename(columns={'Datum_BL': 'Datum'},
                 inplace=True)
    # make datetime column
    make_column_dt(df_bl, "Datum")
    # sum over date to get total of all together per date
    df_bl = df_bl.groupby(by=["Datum"], as_index=False).sum()
    return df_bl


def make_dataframe_abwasserdaten():
    logging.info("import and transform abwasserdaten")
    path = credentials.path_proben
    df_abwasser = pd.read_excel(path, sheet_name="Proben", usecols="A, B, N, O, AD, AE, AK, AL", skiprows=range(6))
    # rename date column and change format
    df_abwasser.rename(columns={'Abwasser von Tag': 'Datum'}, inplace=True)
    return df_abwasser


def make_dataframe_bs_from_2023():
    logging.info(f"import and transform BS data")
    path = credentials.path_BS
    df_bs = pd.read_excel(path)
    # only keep columns Testdatum [Benötigte Angaben], Serotyp/Subtyp [Erreger]
    df_bs = df_bs[['Fall ID [Fall]', 'Testdatum [Benötigte Angaben]', 'Laborresultat [Testresultat]']]
    new_names = {'Fall ID [Fall]': 'ID',
                 'Testdatum [Benötigte Angaben]': 'Datum',
                 'Laborresultat [Testresultat]': 'Resultat'}
    df_bs.rename(columns=new_names, inplace=True)
    make_column_dt(df_bs, "Datum")
    df_bs = df_bs.sort_values(by='Datum').drop_duplicates(subset=['ID'], keep='first').drop(columns=['ID'])
    df_bs = df_bs[df_bs['Datum'] >= datetime(2023, 1, 1)]
    df_bs = df_bs.pivot_table(index='Datum', columns='Resultat', aggfunc='size', fill_value=0).resample('D').asfreq().fillna(0)
    df_bs = df_bs.reset_index()
    df_bs.columns.name = None
    df_bs = df_bs.drop(columns=['nicht bestimmbar']).rename(columns={'positiv': 'faelle_bs'})
    df_bs['inzidenz07_bs'] = df_bs["faelle_bs"].rolling(window=7).sum()/pop_BS * 100000
    return df_bs


def make_dataframe_bs_2021_to_2023():
    logging.info(f"import, transform and merge BS data")
    # get number of cases and 7d inz.
    req = common.requests_get("https://data.bs.ch/api/v2/catalog/datasets/100108/exports/"
                              "json?order_by=test_datum&select=test_datum&select=faelle_bs&select=inzidenz07_bs")
    file = req.json()
    df_zahlen_bs = pd.DataFrame.from_dict(file)
    df_zahlen_bs.rename(columns={'test_datum': 'Datum'}, inplace=True)
    make_column_dt(df_zahlen_bs, "Datum")
    # get hosp, ips, deceased and isolated BS
    req = common.requests_get("https://data.bs.ch/api/v2/catalog/datasets/100073/exports/"
                              "json?order_by=timestamp&select=timestamp&select=current_hosp"
                              "&select=current_icu&select=ndiff_deceased&select=current_isolated")
    file = req.json()
    df_hosp = pd.DataFrame.from_dict(file)
    df_hosp.rename(columns={'timestamp': 'Datum'}, inplace=True)
    df_hosp['Datum'] = pd.to_datetime(df_hosp['Datum']).dt.date
    make_column_dt(df_hosp, "Datum")
    # get positivity rate
    req = common.requests_get("https://data.bs.ch/api/v2/catalog/datasets/100094/exports/"
                              "json?order_by=datum&select=datum&select=positivity_rate_percent")
    file = req.json()
    df_pos_rate = pd.DataFrame.from_dict(file)
    df_pos_rate.rename(columns={'datum': 'Datum'}, inplace=True)
    make_column_dt(df_pos_rate, "Datum")
    # get Effektive mittlere Reproduktionszahl, with estimate_type:Cori_slidingWindow and data_type=Confirmed cases
    req = common.requests_get("https://data.bs.ch/api/v2/catalog/datasets/100110/exports/"
                              "json?refine=region:BS&refine=estimate_type:Cori_slidingWindow"
                              "&refine=data_type:Confirmed+cases&order_by=date&select=date&select=median_r_mean")
    file = req.json()
    df_repr = pd.DataFrame.from_dict(file)
    df_repr.rename(columns={'date': 'Datum'}, inplace=True)
    make_column_dt(df_repr, "Datum")
    # join the datasets
    dfs = [df_zahlen_bs, df_hosp, df_pos_rate, df_repr]
    df_bs = reduce(lambda left, right: pd.merge(left, right, on=['Datum'], how='outer'), dfs)
    # take date from 1 July 2021 and before 1 January 2023
    df_bs = df_bs[(df_bs['Datum'] >= datetime(2021, 7, 1)) & (df_bs['Datum'] < datetime(2023, 1, 1))]
    return df_bs


def calculate_columns(df):
    logging.info(f"calculate and add columns")
    df = df.loc[df['Datum'].notnull()]
    df = df.drop_duplicates(subset=['Datum'], keep='first')
    df = df.set_index('Datum').resample('D').asfreq()
    df = df.reset_index()
    df["pos_rate_BL"] = df["Anz_pos_BL"]/(df["Anz_pos_BL"]+df["Anz_neg_BL"]) * 100
    df["sum_7t_BL"] = df["Anz_pos_BL"].rolling(window=7).sum()
    df["7t_inz_BL"] = df["sum_7t_BL"]/pop_BL * 100000
    df["daily_cases_BS+BL"] = df["Anz_pos_BL"].fillna(0) + df["faelle_bs"].fillna(0)
    df.loc[df["Anz_pos_BL"].isna() & df["faelle_bs"].isna(), "daily_cases_BS+BL"] = None
    df["hospitalized_BS+BL"] = df["Anz_hosp_BL"].fillna(0) + df["current_hosp"].fillna(0)
    df.loc[df["Anz_hosp_BL"].isna() & df["current_hosp"].isna(), "hospitalized_BS+BL"] = None
    df["IC_BS+BL"] = df["Anz_icu_BL"].fillna(0) + df["current_icu"].fillna(0)
    df.loc[df["Anz_icu_BL"].isna() & df["current_icu"].isna(), "IC_BS+BL"] = None
    df["death_BS+BL"] = df["Anz_death_BL"].fillna(0) + df["ndiff_deceased"].fillna(0)
    df.loc[df["Anz_death_BL"].isna() & df["ndiff_deceased"].isna(), "death_BS+BL"] = None
    df["7t_inz_BS+BL"] = (df["7t_inz_BL"].fillna(0) * pop_BL + df["inzidenz07_bs"].fillna(0) * pop_BS)/(pop_BS + pop_BL)
    df.loc[df["7t_inz_BL"].isna() & df["inzidenz07_bs"].isna(), "7t_inz_BS+BL"] = None
    df["pos_rate_BS+BL"] = (df["pos_rate_BL"].fillna(0) * pop_BL + df["positivity_rate_percent"].fillna(0) * pop_BS)/(pop_BS + pop_BL)
    df.loc[df["pos_rate_BL"].isna() & df["positivity_rate_percent"].isna(), "pos_rate_BS+BL"] = None
    df["isolierte_BS+BL"] = df["Anz_Iso_BL"].fillna(0) + df["current_isolated"].fillna(0)
    df.loc[df["Anz_Iso_BL"].isna() & df["current_isolated"].isna(), "isolierte_BS+BL"] = None
    df["Ratio_Isolierte/daily_cases"] = df["isolierte_BS+BL"]/df["daily_cases_BS+BL"]
    df["7t_median_BL"] = df["Anz_pos_BL"].rolling(window=7).median()
    df["7t_median_BS"] = df["faelle_bs"].rolling(window=7).median()
    df["7t_median_BS+BL"] = df["daily_cases_BS+BL"].rolling(window=7).median()
    return df


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info('Job successful!')
