import pandas as pd
from datetime import datetime
from gd_abwassermonitoring import credentials
from functools import reduce
import common
import logging
from common import change_tracking as ct


def main():
    df = merge_dataframes()
    df = calculate_columns(df)
    df.to_csv('100302.csv', index=False)
    return df

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
    new_names = {'Testdatum [Benötigte Angaben]': 'Datum',
                 'Serotyp/Subtyp [Erreger]': 'Type'}
    df_bs.rename(columns=new_names, inplace=True)
    df_bs = df_bs.pivot_table(index='Datum', columns='Type', aggfunc='size', fill_value=0)
    df_bs.columns.name = None
    df_bs = df_bs.add_prefix('Anz_pos_')
    df_bs = df_bs.add_suffix('_BS')
    df_bs.rename(columns={'Anz_pos_Datum_BS': 'Datum'},
                 inplace=True)
    return df_bs


def make_dataframe_abwasser():
    logging.info("import and transform sewage data")
    path = '/Users/hester/PycharmProjects/data-processing/gd_abwassermonitoring/data/Abwasserdaten/Probenraster CoroWWmonitoring.xlsx'
    df_abwasser = pd.read_excel(path, header=2, usecols="A,B,F:AB,AP")
    logging.info(f'Remove text from numerical columns')
    numerical_columns = ['InfA (gc/PCR)', 'InfB (gc/PCR)',
       'InfA (gc/PCR)2', 'InfB (gc/PCR)2', 'RSV (gc/PCR)', 'InfA (gc/L)',
       'InfB (gc/L)', 'RSV (gc/L)', 'InfA (gc/L) 7-d median',
       'InfB (gc/L) 7-d median', 'RSV (gc/L) 7-d median',
       "InfA (gc /100'000 P)", "InfB (gc/100'000 P)", "RSV (gc /100'000 P)",
       "InfA (gc/100'000 P) 7-d median", "InfB (gc/100'000 P) 7-d median",
       "RSV (gc/100'000 P) 7-d median", "InfA (gc/PMMoV)", "InfB (gc/PMMoV)",
       'RSV (gc /PMMoV)', 'InfA (gc/PMMoV) 7-d median',
       'InfB (gc/PMMoV) 7-d median', 'RSV (gc/PMMoV) 7-d median',
       'monthly RSV cases (USB/UKBB, in- & outpatients) ']  # Add your column names here
    for column in numerical_columns:
        df_abwasser[column] = pd.to_numeric(df_abwasser[column], errors='coerce')

    return df_abwasser


def merge_dataframes():
    df_bs = make_dataframe_bs()
    df_bl = make_dataframe_bl()
    df_abwasser = make_dataframe_abwasser()
    merged_df = pd.merge(df_abwasser, df_bs, on='Datum', how='outer')
    merged_df = pd.merge(merged_df, df_bl, on='Datum', how='outer')
    return merged_df


def calculate_columns(df):
    df['InfA_BS+BL'] = df['Anz.pos.A_BL'] + df['Anz_pos_A_BS']
    df['InfB_BS+BL'] = df['Anz.pos.B_BL'] + df['Anz_pos_B_BS']
    df["7t_median_InfA"] = df['InfA_BS+BL'].rolling(window=7).median()
    df["7t_median_InfB"] = df['InfB_BS+BL'].rolling(window=7).median()
    return df


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    df = main()
