import pandas as pd
from gd_abwassermonitoring import credentials
import common
import logging
from common import change_tracking as ct
import os
from datetime import datetime


def main():
    df_all = merge_dataframes()
    df_all['Datum'] = df_all['Datum'].dt.strftime('%Y-%m-%d')
    df_all.to_csv(credentials.path_export_file, index=False)
    if ct.has_changed(credentials.path_export_file):
        common.upload_ftp(credentials.path_export_file, credentials.ftp_server, credentials.ftp_user,
                          credentials.ftp_pass, 'gd_kantonslabor/abwassermonitoring')
        ct.update_hash_file(credentials.path_export_file)
        logging.info("push data to ODS realtime API")
        logging.info("push for dataset 100302")
        push_url = credentials.ods_live_realtime_push_url
        push_key = credentials.ods_live_realtime_push_key
        common.ods_realtime_push_df(df_all, url=push_url, push_key=push_key)


def make_column_dt(df, column):
    df[column] = pd.to_datetime(df[column])


def make_dataframe_bl():
    logging.info(f"import and transform BL data")
    path = credentials.path_BL
    last_changed = os.path.getmtime(path)
    last_changed = str(datetime.fromtimestamp(last_changed).date())
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
    return df_bl, last_changed


def make_dataframe_bs():
    logging.info(f"import and transform BS data")
    path = credentials.path_BS
    df_bs = pd.read_excel(path)
    last_changed = os.path.getmtime(path)
    last_changed = str(datetime.fromtimestamp(last_changed).date())
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
    return df_bs, last_changed


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
                         'monthly RSV cases (USB/UKBB, in- & outpatients) ']
    for column in numerical_columns:
        df_abwasser[column] = pd.to_numeric(df_abwasser[column], errors='coerce')
    return df_abwasser


def add_all_dates(df_bs, date_bs, df_bl, date_bl):
    logging.info('add all dates so that 7d median will be calculated for all')
    updated_until = max(date_bl, date_bs)
    df_bs = df_bs[df_bs.index <= updated_until]
    df_bl = df_bl[df_bl['Datum'] <= updated_until]
    df_infl = pd.merge(df_bs, df_bl, on='Datum', how='outer')
    date_start = str(df_infl['Datum'].min().date())
    date_range = pd.date_range(start=date_start, end=updated_until, freq='D')
    df_infl = df_infl.set_index('Datum')
    df_infl = df_infl.reindex(date_range)
    df_infl = df_infl.reset_index()
    df_infl = df_infl.rename(columns={'index': 'Datum'})
    return df_infl


def make_df_infl_bs_bl():
    df_bs, date_bs = make_dataframe_bs()
    df_bl, date_bl = make_dataframe_bl()
    df_infl = add_all_dates(df_bs, date_bs, df_bl, date_bl)
    df_infl = calculate_columns(df_infl)
    return df_infl


def make_dataframe_rsv():
    path_fortlaufend = credentials.path_RSV_USB_fortlaufend
    df_fortlaufend = pd.read_csv(path_fortlaufend, sep=';')
    df_fortlaufend = df_fortlaufend.rename(columns={'DATUM_RSV_NACHWEIS_KALENDERWOCHE': 'KW'})['KW']
    # Group by "KW" and save the count into "Anz_pos_RSV_USB"
    df_fortlaufend = df_fortlaufend.value_counts().reset_index()
    df_fortlaufend = df_fortlaufend.rename(columns={'count': 'KW_Anz_pos_RSV_USB'})
    df_fortlaufend['KW'] = df_fortlaufend['KW'].str.replace('-KW', '_')
    path_retro = credentials.path_RSV_USB_retrospektiv
    df_retro = pd.read_excel(path_retro)
    df_retro = df_retro.rename(columns={'RSV positiv (Anzahl)': 'KW_Anz_pos_RSV_USB'})
    df_rsv = pd.concat([df_retro, df_fortlaufend]).reset_index(drop=True)
    # Extend df to have every value exist 7 times and create a column with the values 1 to 7
    df_rsv = df_rsv.loc[df_rsv.index.repeat(7)].reset_index(drop=True)
    df_rsv['weekday'] = df_rsv.groupby('KW').cumcount()
    df_rsv['Datum'] = pd.to_datetime(df_rsv['KW'].astype(str) + '_' + df_rsv['weekday'].astype(str),
                                     format='%Y_%W_%w')
    df_rsv['KW'] = df_rsv['KW'].str.split('_').str[1]
    # Calculate mean by dividing by 7
    df_rsv['7t_mean_RSV'] = df_rsv['KW_Anz_pos_RSV_USB'] / 7
    df_rsv = df_rsv.drop(columns=['weekday'])
    return df_rsv


def merge_dataframes():
    df_infl = make_df_infl_bs_bl()
    df_rsv = make_dataframe_rsv()
    df_abwasser = make_dataframe_abwasser()
    merged_df = pd.merge(df_abwasser, df_infl, on='Datum', how='outer')
    merged_df = pd.merge(merged_df, df_rsv, on='Datum', how='left')
    return merged_df


def calculate_columns(df):
    df['InfA_BS+BL'] = df['Anz.pos.A_BL'].fillna(0) + df['Anz_pos_A_BS'].fillna(0)
    df.loc[df['Anz.pos.A_BL'].isna() & df['Anz_pos_A_BS'].isna(), 'InfA_BS+BL'] = None
    df['InfB_BS+BL'] = df['Anz.pos.B_BL'].fillna(0) + df['Anz_pos_B_BS'].fillna(0)
    df.loc[df['Anz.pos.B_BL'].isna() & df['Anz_pos_B_BS'].isna(), 'InfB_BS+BL'] = None
    df["7t_median_InfA"] = df['InfA_BS+BL'].rolling(window=7).median()
    df["7t_median_InfB"] = df['InfB_BS+BL'].rolling(window=7).median()
    return df

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
