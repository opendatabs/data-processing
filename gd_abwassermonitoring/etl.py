import pandas as pd
from datetime import datetime
from gd_abwassermonitoring import credentials
from functools import reduce
import common
import logging
from common import change_tracking as ct


def make_column_dt(df, column):
    df[column] = pd.to_datetime(df[column])


def make_dataframe_bl():
    logging.info(f"import and transform BL data")
    path = '/Users/hester/PycharmProjects/data-processing/gd_abwassermonitoring/data/Falldaten-BL/Abwassermonitoring_Influenza.csv'
    df_bl = pd.read_csv(path, encoding="ISO-8859-1")
    # remove Gemeinde
    df_bl.drop(columns=["Gemeinde"], inplace=True)
    # add suffix BL for all but date column
    df_bl = df_bl.add_suffix('_BL')
    df_bl.rename(columns={'Testdatum_BL': 'Datum'},
                 inplace=True)
    # make datetime column
    make_column_dt(df_bl, "Datum")
    # sum over date to get total of all together per date
    df_bl = df_bl.groupby(by=["Datum"], as_index=False).sum()
    return df_bl


def make_dataframe_bs():
    logging.info(f"import and transform BS data")
    path = '/Users/hester/PycharmProjects/data-processing/gd_abwassermonitoring/data/Falldaten-BS/ISM_export_influenza.xlsx'
    df_bs = pd.read_excel(path)
    # only keep columns Testdatum [Benötigte Angaben], Serotyp/Subtyp [Erreger]
    df_bs = df_bs[['Testdatum [Benötigte Angaben]', 'Serotyp/Subtyp [Erreger]']]
    df_bs = df_bs.pivot_table(index='Testdatum [Benötigte Angaben]', columns='Serotyp/Subtyp [Erreger]', aggfunc='size', fill_value=0)
    df_bs.columns.name = None
    return df_bs


def make_dataframe_abwasser():
    logging.info("import and transform sewage data")
    path = '/Users/hester/PycharmProjects/data-processing/gd_abwassermonitoring/data/Abwasserdaten/Probenraster CoroWWmonitoring.xlsx'
    df_abwasser = pd.read_excel(path, header=2, usecols="A,F:AB")
    return df_abwasser