import pandas as pd
from aue_fischereistatistik import credentials
import common
import logging
from common import change_tracking as ct


columns = ['Fischereikarte', 'Fangbüchlein_retourniert', 'Datum', 'Monat', 'Gewässercode', 'Fischart', 'Gewicht',
           'Länge', 'Nasenfänge', 'Kesslergrundel', 'Schwarzmundgrundel', 'Nackthalsgrundel',
           'Abfluss_Rhein_über_1800m3', 'Bemerkungen']

df = pd.DataFrame(columns=columns)
