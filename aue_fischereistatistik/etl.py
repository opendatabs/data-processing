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
    # replace '0' with empty string
    # Question: In Gewässercode: 0=unbekannt?
    df_year[['Datum', 'Monat', 'Fischart',  'Gewicht',
             'Länge','Abfluss_Rhein_über_1800m3', 'Bemerkungen']]\
        =  df_year[['Datum', 'Monat', 'Fischart',  'Gewicht',
                    'Länge','Abfluss_Rhein_über_1800m3', 'Bemerkungen']].replace('0','')

    # make month column complete/in same format and add day column
    if (df_year['Monat'] == '').all():
        # remove empty space from datum column
        df_year['Datum'] = df_year['Datum'].str.strip()
        # remove point/komma at end of entries in Datum column if it's there
        df_year['Datum'] = df_year['Datum'].apply(lambda x: x[:-1] if (x != '' and (x[-1] == '.' or x[-1] ==',')) else x)
        df_year['Monat'] = pd.to_datetime(df_year['Datum'], format='%d.%m', errors='coerce').dt.strftime('%m')
        # add day column
        df_year['Tag'] = pd.to_datetime(df_year['Datum'], format='%d.%m', errors='coerce').dt.strftime('%d')
    else:
        # Complete month column all in same format
        # need to correct in month column: 'juli' 'juö' 'ap' '3' 'mai' '0' ''
        df_year['Monat'].replace('juli', 'Juli', inplace=True)
        df_year['Monat'].replace('ap', 'April', inplace=True)
        df_year['Monat'].replace('mai', 'Mai', inplace=True)
        df_year['Monat'].replace('3', 'März', inplace=True)
        df_year['Monat'].replace('juö', 'Juli', inplace=True)
        # change month names to zero-padded decimal numbers
        df_year['Monat'] = df_year['Monat'].apply(
            lambda x: datetime.strptime(x, '%B') if type(x) == str and x != '' else pd.NaT)
        df_year['Monat'] = df_year['Monat'].dt.strftime('%m')
        # add day column
        if year == '2012':
            df_year['Tag'] = pd.to_datetime(df_year['Datum'], format='%d.%m.', errors='coerce').dt.strftime('%d')
        else:
            df_year['Tag'] = df_year['Datum']
    df = pd.concat([df, df_year])
# filter columns for export
df = df[['Jahr', 'Monat', 'Tag', 'Gewässercode', 'Fischart', 'Gewicht',
           'Länge', 'Nasenfänge', 'Kesslergrundel', 'Schwarzmundgrundel', 'Nackthalsgrundel',
           'Abfluss_Rhein_über_1800m3']]
df.to_csv(f'{credentials.base_path_local}/fangstatistik.csv')



# probably remove column Bemerkungen (or remove all personal info)





