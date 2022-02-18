import pandas as pd
from aue_fischereistatistik import credentials
import common
import logging
from common import change_tracking as ct


columns = ['Fischereikarte', 'Fangbüchlein_retourniert', 'Datum', 'Monat', 'Year','Gewässercode', 'Fischart', 'Gewicht',
           'Länge', 'Nasenfänge', 'Kesslergrundel', 'Schwarzmundgrundel', 'Nackthalsgrundel',
           'Abfluss_Rhein_über_1800m3', 'Bemerkungen']

df = pd.DataFrame(columns=columns)

path = credentials.path_2020
df_2020 = pd.read_csv(path, encoding='utf-8', keep_default_na=False)
pd.set_option('display.max_columns', None)
print(df_2020)