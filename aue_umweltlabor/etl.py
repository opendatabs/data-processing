import pandas as pd
import os
import sys
import numpy as np
from shutil import copy2
import time
import common
from aue_umweltlabor import credentials


# get name of object, see https://stackoverflow.com/a/592891
def namestr(obj, namespace):
    return [name for name in namespace if namespace[name] is obj]


datafilename = 'OGD-Daten.CSV'
datafile_with_path = os.path.join(credentials.path_orig, datafilename)

no_file_copy = False
if 'no_file_copy' in sys.argv:
    no_file_copy = True
    print('Proceeding without copying files to or from working folder...')
else:
    print("Copying file " + datafile_with_path + " to directory " + credentials.path_work)
    copy2(datafile_with_path, credentials.path_work + datafilename)

print('Reading data file from ' + credentials.path_work + datafilename + '...')
datafile = credentials.path_work + datafilename
data = pd.read_csv(datafile, sep=';', na_filter=False, encoding='cp1252', dtype={
    'Probentyp': 'category',
    'Probenahmestelle': 'category',
    'X-Coord': 'category',
    'Y-Coord': 'category',
    'Probenahmedauer': 'category',
    'Reihenfolge': 'category',
    'Gruppe': 'category',
    'Auftragnr': 'category',
    'Probennr': 'category',
    'Resultatnummer': 'string',
    'Automatische Auswertung': 'category'
})

print('Calculating new columns...')
# replacing spaces with '_' in column names
data.columns = [column.replace(" ", "_") for column in data.columns]
# create new columns
data['Probenahmedatum_date'] = pd.to_datetime(data['Probenahmedatum'], format='%d.%m.%Y', errors='coerce')
data['Probenahmejahr'] = data['Probenahmedatum_date'].dt.year
data.Probenahmejahr = data.Probenahmejahr.fillna(0).astype({'Probenahmejahr': int})
# create new column that contains only numeric values of column "Wert"
data['Wert_cleaned_for_num'] = data['Wert'].str.replace('<', '')
data['Wert_num'] = pd.to_numeric(data['Wert_cleaned_for_num'], errors='coerce')
data = data.drop(columns=['Wert_cleaned_for_num'])

print('Create independent datasets:')
gew_rhein_rues_fest = data.query('Probenahmestelle == "GEW_RHEIN_RUES" and Probentyp == "FESTSTOFF"')
gew_rhein_rues_wasser = data.query('Probenahmestelle == "GEW_RHEIN_RUES" and Probentyp == "WASSER"')
oberflaechengew = data.query('Probentyp == "WASSER" and '
                             'Probenahmestelle != "GEW_RHEIN_RUES" and '
                             'Probenahmestelle.str.contains("GEW_")')
grundwasser = data.query('Probenahmestelle.str.contains("F_")')
generated_datasets = [gew_rhein_rues_wasser, gew_rhein_rues_fest, oberflaechengew, grundwasser]

current_filename = 'gew_rhein_rues_wasser_truncated'
print('Creating dataset ' + current_filename + "...")
latest_year = gew_rhein_rues_wasser['Probenahmejahr'].max()
years = [latest_year, latest_year - 1]
gew_rhein_rues_wasser_truncated = gew_rhein_rues_wasser[gew_rhein_rues_wasser.Probenahmejahr.isin(years)]
generated_datasets.append(gew_rhein_rues_wasser_truncated)

year_gew_rhein_rues_wasser = {}
all_years = gew_rhein_rues_wasser['Probenahmejahr'].unique()
for year in all_years:
    year_gew_rhein_rues_wasser[year] = gew_rhein_rues_wasser[gew_rhein_rues_wasser.Probenahmejahr.eq(year)]
    current_filename = 'gew_rhein_rues_wasser_' + str(year)
    print('Creating dataset ' + current_filename + "...")
    # create variable name for current year
    dataset_name = 'gew_rhein_rues_wasser_' + str(year)
    globals()[dataset_name] = year_gew_rhein_rues_wasser[year]
    generated_datasets.append(globals()[dataset_name])
    # year_gew_rhein_rues_wasser.to_csv(current_filename, sep=';', encoding='utf-8', index=False)

generated_filenames = []
for dataset in reversed(generated_datasets):
    current_filename = namestr(dataset, globals())[0] + '.csv'
    generated_filenames.append(current_filename)
    print("Exporting dataset to " + current_filename + '...')
    dataset.to_csv(credentials.path_work + current_filename, sep=';', encoding='utf-8', index=False)

if not no_file_copy:
    ftp_server = credentials.ftp_server
    ftp_user = credentials.ftp_user
    ftp_pass = credentials.ftp_pass

    files_to_upload = generated_filenames
    files_to_upload.append(datafilename)
    for filename in files_to_upload:
        common.upload_ftp(credentials.path_work + filename, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, '')

    print('Publishing ODS datasets...')
    ods_dataset_uids = ['da_20e9bc', 'da_uxt6fk', 'da_q78iuw', 'da_reclv8']
    for datasetuid in ods_dataset_uids:
        common.publish_ods_dataset(datasetuid, credentials)
        print('Waiting 1 minute before sending next publish request to ODS...')
        time.sleep(60)


print('Job successful.')
