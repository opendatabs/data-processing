import pandas as pd
from aue_fischereistatistik import credentials
import common
import logging
from common import change_tracking as ct
import locale

pd.set_option('display.max_columns', None)
# datetime in German
locale.setlocale(locale.LC_TIME, 'de_DE.UTF-8')

columns = ['Fischereikarte', 'Fangbüchlein_retourniert', 'Datum', 'Monat', 'Jahr','Gewässercode', 'Fischart', 'Gewicht',
           'Länge', 'Nasenfänge', 'Kesslergrundel', 'Schwarzmundgrundel', 'Nackthalsgrundel',
           'Abfluss_Rhein_über_1800m3', 'Bemerkungen']

df = pd.DataFrame(columns=columns)


for year in range(2010,2021):
    year = str(year)
    path = f'{credentials.path_csv}/fangstatistik_{year}.csv'
    df_year = pd.read_csv(path, encoding='utf-8', keep_default_na=False)
    df_year['Jahr'] = year

    # Complete month column all in same format
    # need to correct in month column: 'juli' 'juö' 'ap' '3' 'mai' '0' ''
    # Month as a zero-padded decimal number



    # probably remove column Bemerkungen (or remove all personal info)
    # remove column Datum? Else need to put all in same format (day?)
    # replace 0 by empty string in some columns
    df = pd.concat([df, df_year])
