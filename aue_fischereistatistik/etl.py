import pandas as pd
from aue_fischereistatistik import credentials
import common
import logging
from common import change_tracking as ct
import locale
from datetime import datetime

pd.set_option('display.max_columns', None)
pd.set_option( 'display.max_rows', None)
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
    df_year['Datum'].replace('0','', inplace=True)
    if (df_year['Monat'] == '').all():
        # remove empty space from datum column
        df_year['Datum'] = df_year['Datum'].str.strip()
        # remove point/komma at end of entries in Datum column if it's there
        df_year['Datum_new'] = df_year['Datum'].apply(lambda x: x[:-1] if (x != '' and (x[-1] == '.' or x[-1] ==',')) else x)
        df_year['Monat'] = pd.to_datetime(df_year['Datum_new'], format='%d.%m', errors='coerce').dt.strftime('%m')
    else:
        # Complete month column all in same format
        # need to correct in month column: 'juli' 'juö' 'ap' '3' 'mai' '0' ''
        df_year['Monat'].replace('0', '', inplace=True)
        df_year['Monat'].replace('juli', 'Juli', inplace=True)
        df_year['Monat'].replace('ap', 'April', inplace=True)
        df_year['Monat'].replace('mai', 'Mai', inplace=True)
        df_year['Monat'].replace('3', 'März', inplace=True)
        df_year['Monat'].replace('juö', 'Juli', inplace=True)

        # change month names to zero-padded decimal numbers
        df_year['Monat'] = df_year['Monat'].apply(
            lambda x: datetime.strptime(x, '%B') if type(x) == str and x != '' else pd.NaT)
        df_year['Monat'] = df_year['Monat'].dt.strftime('%m')
    df = pd.concat([df, df_year])



# probably remove column Bemerkungen (or remove all personal info)
# remove column Datum? Else need to put all in same format (day?)
# replace 0 by empty string in some columns



