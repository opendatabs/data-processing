import pandas as pd
import os
import glob
import zipfile
import requests
import subprocess
import shlex
import json
import credentials

# Read dataset
filename = credentials.path_orig + 'ogd_datensaetze.csv'
data = pd.read_csv(filename, sep=';', encoding='cp1252')

# Iterate over entries
for index, row in data.iterrows():
    # Construct folder path
    # path = credentials.path_orig + data.iloc[1]['ordnerpfad'].replace('\\', '/')
    path = credentials.path_orig + row['ordnerpfad'].replace('\\', '/')

    # Exclude raster data for the moment - we don't have them yet
    if row['art'] == 'Vektor':
        # Get files from folder
        files = os.listdir(path)

        # How many unique shp files are there?
        shpfiles = glob.glob(os.path.join(path, '*.shp'))


        # For each shp file:
        for shpfile in shpfiles:
            # test
            if ('' + row['ordnerpfad']).endswith('BauStrassenWaldlinien'):

                # [Transform Shape to WGS-84]

                # Create zip file containing all necessary files for each Shape
                shppath, shpfilename = os.path.split(shpfile)
                shpfilename_noext, shpext = os.path.splitext(shpfilename)
                #zipf = zipfile.ZipFile(os.path.join(path, shpfilename_noext + '.zip'), 'w')
                # create local subfolder mirroring mounted drive
                folder = shppath.replace(credentials.path_orig, '')
                # todo: create subfolders
                zipfilepath = os.path.join(os.getcwd(), folder, shpfilename_noext + '.zip')
                print('Cretaing zip file ' + zipfilepath)
                zipf = zipfile.ZipFile(zipfilepath, 'w')
                # Include all files with shpfile's name
                files_to_zip = glob.glob(os.path.join(path, shpfilename_noext + '.*'))
                for file_to_zip in files_to_zip:
                    # Do not add the zip file into the zip file...
                    if not file_to_zip.endswith('.zip'):
                        zipf.write(file_to_zip, os.path.split(file_to_zip)[1])
                zipf.close()

                # Load metadata from geocat.ch
                # See documentation at https://www.geocat.admin.ch/de/dokumentation/csw.html
                # For unknown reasons (probably proxy-related), requests always returns http error 404, so we have to revert to launnching curl in a subprocess
                # curl -X GET "https://www.geocat.ch/geonetwork/srv/api/0.1/records/289b9c0c-a1bb-4ffc-ba09-c1e41dc7138a" -H "accept: application/json" -H "Accept: application/xml" -H "X-XSRF-TOKEN: a1284e46-b378-42a4-ac6a-d48069e05494"
                # resp = requests.get('https://www.geocat.ch/geonetwork/srv/api/0.1/records/2899c0c-a1bb-4ffc-ba09-c1e41dc7138a', params={'accept': 'application/json'}, proxies={'https': credentials.proxy})
                # resp = requests.get('https://www.geocat.ch/geonetwork/srv/api/0.1/records/2899c0c-a1bb-4ffc-ba09-c1e41dc7138a', headers={'accept': 'application/xml, application/json'}, proxies={'https': credentials.proxy})
                # cmd = 'curl -X GET "https://www.geocat.ch/geonetwork/srv/api/0.1/records/289b9c0c-a1bb-4ffc-ba09-c1e41dc7138a" -H "accept: application/json" -H "accept: application/json" -k'
                # args = shlex.split(cmd)
                geocat_uid = row['geocat'].rsplit('/', 1)[-1]
                metadata_file = geocat_uid + '.json'
                cmd = 'curl -X GET "https://www.geocat.ch/geonetwork/srv/api/0.1/records/289b9c0c-a1bb-4ffc-ba09-c1e41dc7138a" -H "accept: application/json" -H "accept: application/json" -k > ' + os.getcwd() + '/' + metadata_file
                resp = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE)
                with open('data.txt') as json_file:
                    metadata = json.load(json_file)
                # resptxt = str(resp.stdout)
                # json.loads(resptxt)

                print(resp.stdout)
                # Add entry to harvester file

                # FTP upload file

        # No shp file: find out filename
        if len(shpfiles) == 0:
            test = 0
            # Load metadata from geocat.ch
            # Add entry to harvester file
            # FTP upload file

# FTP upload harvester csv file




