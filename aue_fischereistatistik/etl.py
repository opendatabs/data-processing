import pandas as pd
from aue_fischereistatistik import credentials
import common
import logging
from common import change_tracking as ct

pd.set_option('display.max_columns', None)

columns = ['Fischereikarte', 'Fangbüchlein_retourniert', 'Datum', 'Monat', 'Jahr','Gewässercode', 'Fischart', 'Gewicht',
           'Länge', 'Nasenfänge', 'Kesslergrundel', 'Schwarzmundgrundel', 'Nackthalsgrundel',
           'Abfluss_Rhein_über_1800m3', 'Bemerkungen']

df = pd.DataFrame(columns=columns)


for year in range(2010,2021):
    year = str(year)
    path = f'{credentials.path_csv}/fangstatistik_{year}.csv'
    df_year = pd.read_csv(path, encoding='utf-8', keep_default_na=False)
    df_year['Jahr'] = year
    # Month as a zero-padded decimal number
    # probably remove column Bemerkungen
    # remove column Datum? Else need to put all in same format (day?)
    # replace 0 by empty string in some columns
    df = pd.concat([df, df_year])

