import pandas as pd
from datetime import datetime
import os
import credentials

datafilename = 'OGD-Daten.CSV'
print('Reading data file form ' + os.path.join(credentials.path_orig, datafilename) + '...')
datafile = credentials.path_orig + datafilename
data = pd.read_csv(datafile, sep=';', na_filter=False, encoding='cp1252', dtype={
    'Probentyp': 'category',
    'Probenahmestelle': 'category',
    'X-Coord':'category',
    'Y-Coord': 'category',
    'Entnahmezeit': 'category',
    'Probenahmedauer': 'category',
    'Reihenfolge': 'category',
    'Gruppe': 'category',
    'Auftragnr': 'category',
    'Probennr': 'category',
    'Resultatnummer': 'string',
    'Automatische Auswertung': 'category'
    # '': 'category',
})

# replacing spaces with '_' in column names
data.columns = [column.replace(" ", "_") for column in data.columns]

gew_rhein_rues_fest = data.query('Probenahmestelle == "GEW_RHEIN_RUES" and Probentyp == "FESTSTOFF"')
gew_rhein_rues_wasser = data.query('Probenahmestelle == "GEW_RHEIN_RUES" and Probentyp == "WASSER"')
oberflaechengew = data.query('Probentyp == "WASSER" and '
                             'Probenahmestelle != "GEW_RHEIN_RUES" and '
                             'Probenahmestelle.str.contains("GEW_")')
grundwasser = data.query('Probenahmestelle.str.contains("F_")')


gew_rhein_rues_fest.to_csv("gew_rhein_rues_fest.csv", sep=';', encoding='utf-8', index=False)
gew_rhein_rues_wasser.to_csv("gew_rhein_rues_wasser.csv", sep=';', encoding='utf-8', index=False)
oberflaechengew.to_csv("oberflaechengew.csv", sep=';', encoding='utf-8', index=False)
grundwasser.to_csv("grundwasser.csv", sep=';', encoding='utf-8', index=False)