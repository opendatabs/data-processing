import pandas as pd
import os
import glob
import zipfile
import credentials

# Read dataset
filename = credentials.path_orig + 'ogd_datensaetze.csv'
data = pd.read_csv(filename, sep=';', encoding='cp1252')

# Iterate over entries
for line in data:
    test = 0

    # Construct folder path
    #path = credentials.path_orig + data.iloc[1]['ordnerpfad'].replace('\\', '/')
    path = credentials.path_orig + line['ordnerpfad']
    # Get files from folder
    files = os.listdir(path)

    # How many unique shp files are there?
    shpfiles = glob.glob(os.path.join(path, '*.shp'))

    # For each shp file:
    for shpfile in shpfiles:
        # [Transform Shape to WGS-84]

        # Create zip file containing all necessary files for each Shape
        shppath, shpfilename = os.path.split(shpfile)
        shpfilename_noext, shpext = os.path.splitext(shpfilename)
        zipf = zipfile.ZipFile(os.path.join(path, shpfilename_noext + '.zip'), 'w')
        # Include all files with shpfile's name
        files_to_zip = glob.glob(os.path.join(path, shpfilename_noext + '.*'))
        for file_to_zip in files_to_zip:
            # Do not add the zip file into the zip file...
            if not file_to_zip.endswith('.zip'):
                zipf.write(file_to_zip, os.path.split(file_to_zip)[1])
        zipf.close()

        # Load metadata from geocat.ch
        # Add entry in harvester file
        # FTP upload file

    # No shp file: find out filename
    if len(shpfiles) == 0:
        # Load metadata from geocat.ch
        # Add entry in harvester file
        # FTP upload file

# FTP upload harvester csv file




