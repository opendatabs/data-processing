import pandas as pd
import os
import pathlib


path_kennzahlen = os.path.join(pathlib.Path(__file__).parents[1], 'data/data-processing-output', 'Abstimmungen_2022-09-25.csv')
path_details = os.path.join(pathlib.Path(__file__).parents[1], 'data/data-processing-output', 'Abstimmungen_Details_2022-09-25.csv')

df_kennzahlen = pd.read_csv(path_kennzahlen)
df_details = pd.read_csv(path_details)


df_all = pd.concat([df_details, df_kennzahlen], ignore_index=True)

df_all['auf_ebene'] = ['Gemeinde' if x else 'Wahllokal' for x in pd.isna(df_all['Wahllok_name'])]


# add BS-ID
df_all['year'] = (df_all['Abst_Datum'].astype('datetime64[ns]')).dt.year
lauf_nr_termin = '03'
df_all['bs_id'] = df_all['year'].astype(str) + lauf_nr_termin + '0' + df_all['Abst_ID'].astype(str)

path_export = os.path.join(pathlib.Path(__file__).parents[1], 'data/export', 'abstimmungen.csv')
df_all.to_csv(path_export, index=False)
