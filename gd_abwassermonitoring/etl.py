import pandas as pd
from gd_abwassermonitoring import credentials
import common
import logging
from common import change_tracking as ct


def main():
    df = merge_dataframes()
    df = calculate_columns(df)
    df['Datum'] = df['Datum'].dt.strftime('%Y-%m-%d')
    df.to_csv(credentials.path_export_file, index=False)
    # if ct.has_changed(credentials.path_export_file):
    #     common.upload_ftp(credentials.path_export_file, credentials.ftp_server, credentials.ftp_user,
    #                       credentials.ftp_pass, 'gd_kantonslabor/abwassermonitoring')
    #     ct.update_hash_file(credentials.path_export_file)
    #     logging.info("push data to ODS realtime API")
    #     logging.info("push for dataset 100302")
    #     push_url = credentials.ods_live_realtime_push_url
    #     push_key = credentials.ods_live_realtime_push_key
    #     common.ods_realtime_push_df(df, url=push_url, push_key=push_key)
    return df

def make_column_dt(df, column):
    df[column] = pd.to_datetime(df[column])


def make_dataframe_bl():
    logging.info(f"import and transform BL data")
    path = credentials.path_BL
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
    path = credentials.path_BS
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
    path = credentials.path_proben
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
    df['InfA_BS+BL'] = df['Anz.pos.A_BL'].fillna(0) + df['Anz_pos_A_BS'].fillna(0)
    df['InfB_BS+BL'] = df['Anz.pos.B_BL'].fillna(0) + df['Anz_pos_B_BS'].fillna(0)
    df["7t_median_InfA"] = df['InfA_BS+BL'].rolling(window=7).median()
    df["7t_median_InfB"] = df['InfB_BS+BL'].rolling(window=7).median()
    return df


# Realtime API bootstrap data for dataset 100302:
#
{"Datum": "2021-08-12",
"Sample Ba-Nr.": "dito wie 15.4.",
"InfA (gc/PCR)": "1.0",
"InfB (gc/PCR)": "1.0",
"InfA (gc/PCR)2": "43.5",
"InfB (gc/PCR)2": "233.0",
"RSV (gc/PCR)": "287.0",
"InfA (gc/L)": "17400.0",
"InfB (gc/L)": "93200.0",
"RSV (gc/L)": "114800.0",
"InfA (gc/L) 7-d median": "33000.0",
"InfB (gc/L) 7-d median": "143600.0",
"RSV (gc/L) 7-d median": "114800.0",
"InfA (gc /100'000 P)": "528021378465.64966",
"InfB (gc/100'000 P)": "2828252440976.928",
"RSV (gc /100'000 P)": "3483727255623.9414",
"InfA (gc/100'000 P) 7-d median": "608804679610.9795",
"InfB (gc/100'000 P) 7-d median": "3739980264850.316",
"RSV (gc/100'000 P) 7-d median": "4243964845324.804",
"InfA (gc/PMMoV)": "7.357293868921777",
"InfB (gc/PMMoV)": "0.0003940803382663848",
"RSV (gc /PMMoV)": "0.0004854122621564482",
"InfA (gc/PMMoV) 7-d median": "7.357293868921777",
"InfB (gc/PMMoV) 7-d median":" 0.0004042848141146818",
"RSV (gc/PMMoV) 7-d median": "0.0004516698172652804",
"monthly RSV cases (USB/UKBB, in- & outpatients) ": "1",
"Anz_pos_A_BS": "1",
"Anz_pos_B_BS": "1",
"Anz_pos_H1_BS": "1",
"Anz.pos.A_BL": "1",
"Anz.pos.B_BL": "1",
"Anz.pos.all_BL": "1",
"InfA_BS+BL": "1",
"InfB_BS+BL": "1",
"7t_median_InfA": "1",
"7t_median_InfB": "1"}



if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    df = main()
