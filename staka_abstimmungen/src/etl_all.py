import pandas as pd
import os
import glob
import pathlib


path_files = os.path.join(pathlib.Path(__file__).parents[1], 'data/data-processing-output')

def main():
    df_all = process_files()
    df_all = construct_dataset(df_all)
    return df_all

def process_files():
    df = pd.DataFrame()
    files_details = get_files_details()
    files_kennzahlen = get_files_kennzahlen()
    for file in files_details:
        df_file = pd.read_csv(file)
        df = pd.concat([df, df_file])
    df = join_wahllokale(df)
    for file in files_kennzahlen:
        df_file = pd.read_csv(file)
        df = pd.concat([df, df_file])
    return df


def construct_dataset(df):
    df['auf_ebene'] = ['Gemeinde' if x else 'Wahllokal' for x in pd.isna(df['Wahllok_name'])]
    # add BS-ID
    df['year'] = (df['Abst_Datum'].astype('datetime64[ns]')).dt.year
    dict_laufnr = make_dict_date_laufnr()
    df['lauf_nr_termin'] = [dict_laufnr[datum] for datum in df['Abst_Datum']]
    df['bs_id'] = df['year'].astype(str) + '0' + df['lauf_nr_termin'] + '0' + df['Abst_ID'].astype(str)
    #df['bs_id'] = [bs_id[i] if x == "kantonal" else '' for i, x in df['Abst_Art'].items()]
    path_export = os.path.join(pathlib.Path(__file__).parents[1], 'data/export', 'abstimmungen.csv')
    df.to_csv(path_export, index=False)
    return df


def get_files_kennzahlen():
    pattern_kennzahlen = 'Abstimmungen_??????????.csv'
    file_list = glob.glob(os.path.join(path_files, pattern_kennzahlen))
    return file_list


def get_files_details():
    pattern_details = 'Abstimmungen_Details_??????????.csv'
    file_list = glob.glob(os.path.join(path_files, pattern_details))
    return file_list


def join_wahllokale(df):
    path_wahllokale = os.path.join(path_files, 'wahllokale.csv')
    df_wahllokale = pd.read_csv(path_wahllokale, sep=';')
    df = pd.merge(df_wahllokale, df, left_on='Wahllok_Name', right_on='Wahllok_name')
    df = df.drop(columns='Wahllok_Name')
    return df


def get_dates():
    file_list = get_files_kennzahlen()
    list_dates = []
    for file in file_list:
        date_str = os.path.basename(file).split("_", 1)[1][:10]
        list_dates.append(date_str)
    list_dates.sort()
    return list_dates


def make_dict_date_laufnr():
    dates = get_dates()
    dict = {}
    year = '2020'
    lauf_nr_termin = 2
    for date in dates:
        if date[0:4] == year:
            dict[date] = str(lauf_nr_termin)
        else:
            year = date[0:4]
            lauf_nr_termin = 1
            dict[date] = str(lauf_nr_termin)
        lauf_nr_termin += 1
    return dict

