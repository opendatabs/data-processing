import pandas as pd
import os
import glob
import pathlib


path_files = os.path.join(pathlib.Path(__file__).parents[1], 'data/data-processing-output')

def get_dates():
    pattern_kennzahlen = 'Abstimmungen_??????????.csv'
    file_list = glob.glob(os.path.join(path_files, pattern_kennzahlen))
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
            dict[date] = lauf_nr_termin
        else:
            year = date[0:4]
            lauf_nr_termin = 1
            dict[date] = lauf_nr_termin
        lauf_nr_termin += 1
    return dict


path_kennzahlen = os.path.join(pathlib.Path(__file__).parents[1], 'data/data-processing-output', 'Abstimmungen_2022-09-25.csv')
path_details = os.path.join(pathlib.Path(__file__).parents[1], 'data/data-processing-output', 'Abstimmungen_Details_2022-09-25.csv')

df_kennzahlen = pd.read_csv(path_kennzahlen)
df_details = pd.read_csv(path_details)


df_all = pd.concat([df_details, df_kennzahlen], ignore_index=True)

df_all['auf_ebene'] = ['Gemeinde' if x else 'Wahllokal' for x in pd.isna(df_all['Wahllok_name'])]


# add BS-ID
df_all['year'] = (df_all['Abst_Datum'].astype('datetime64[ns]')).dt.year
lauf_nr_termin = '03'
bs_id = df_all['year'].astype(str) + lauf_nr_termin + '0' + df_all['Abst_ID'].astype(str)
df_all['bs_id'] = [bs_id[i] if x == "kantonal" else '' for i, x in df_all['Abst_Art'].items()]
path_export = os.path.join(pathlib.Path(__file__).parents[1], 'data/export', 'abstimmungen.csv')
df_all.to_csv(path_export, index=False)
