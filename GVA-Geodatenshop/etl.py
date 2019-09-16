import pandas as pd
from datetime import datetime
import os
import glob
import zipfile
import subprocess
import json
import common
import credentials

datafilename = 'ogd_datensaetze.csv'
print('Reading data file form ' + os.path.join(credentials.path_orig, datafilename) + '...')
filename = credentials.path_orig + 'ogd_datensaetze.csv'
data = pd.read_csv(filename, sep=';', encoding='cp1252')
metadata_for_ods = []

print('Iterating over datasets...')
for index, row in data.iterrows():
    # Construct folder path
    # path = credentials.path_orig + data.iloc[1]['ordnerpfad'].replace('\\', '/')
    path = credentials.path_orig + row['ordnerpfad'].replace('\\', '/')
    print ('Checking ' + path + '...')

    # Exclude raster data for the moment - we don't have them yet
    if row['art'] == 'Vektor':
        # Get files from folder
        files = os.listdir(path)

        # How many unique shp files are there?
        shpfiles = glob.glob(os.path.join(path, '*.shp'))
        print (str(len(shpfiles)) + ' shp files in ' + path )

        # For each shp file:
        for shpfile in shpfiles:
            # Make sure only the currently necessary datasets are imported
            # if ('Statistische Raumeinheiten' in ('' + row['titel'])) or ('Amtliche Vermessung Basel-Stadt' in ('' + row['titel'])):  # or ('' + row['ordnerpfad']).endswith('BauStrassenWaldlinien'):
            required_topics = ['AF\\Abfuhrtermine', 'AF\\Abfuhrzonen', 'AL\\Alterspflegeheime', 'AS\\Akutspitaeler', 'BA\\Baumbestand', 'BA\\Faellliste', 'BI\\InteressanteOrte', 'BS\\Gebaeudeadressen', 'BS\\PLZOrtschaft', 'BW\\Allmendbewilligungen', 'DF\\Defibrillatoren', 'EB\\Erdbebenmikrozonierung', 'EL\\Elternberatung', 'ES\\Entsorgungsstellen', 'GO\\GueteklassenOeV', 'HS\\Hundesignalisation', 'KJ\\KinderJugendangebote', 'KK\\KatholischeKirchenkreise', 'LN\\OeVHaltestellen', 'LN\\OeVLiniennetz', 'LN\\OeVTeilhaltestellen', 'MN\\Durchgangsstrassen', 'MN\\KSRiehenBettingen', 'MN\\StrassentypenWege', 'NI\\Naturinventar', 'NK\\Invasive_Neophyten', 'NR\\NaturinventarRiehen', 'OG\\OeffentlGrundeigentum', 'OR\\OeffentlicherRaum', 'OW\\SportBewegung', 'PW\\PolitischeWahlkreise', 'QT\\Quartiertreffpunkte', 'RC\\Recyclingstellen', 'SC\\Schulstandorte', 'SE\\Siedlungsentwicklung', 'SG\\SanitaereAnlagen', 'SN\\Strassennamen', 'SO\\Schulstandorte', 'SS\\Schulwegsicherheit', 'ST\\Nettogeschossflaeche', 'SX\\Adressen', 'SX\\Dachkanten', 'SX\\Fernwaerme', 'SX\\Photovoltaik', 'SX\\Solarthermie', 'TK\\TagesheimeKitas', 'VO\\Velorouten_Alltag', 'VO\\Velorouten_touristisch', 'VO\\Velorouten_TRP', 'VO\\Velostadtplan', 'VR\\Begegnungszonen', 'VR\\Fussgaengerzonen', 'VR\\Tempo30Zonen', 'VR\\VKIPerimeter', 'VZ\\Verkehrszaehldaten', 'WE\\Bezirk', 'WE\\Block', 'WE\\Blockseite', 'WE\\Wohnviertel']
            if (str(row['ordnerpfad'])) in required_topics:
                # Create zip file containing all necessary files for each Shape
                shppath, shpfilename = os.path.split(shpfile)
                shpfilename_noext, shpext = os.path.splitext(shpfilename)
                # zipf = zipfile.ZipFile(os.path.join(path, shpfilename_noext + '.zip'), 'w')
                # create local subfolder mirroring mounted drive
                folder = shppath.replace(credentials.path_orig, '')
                folder_flat = folder.replace('/', '__'). replace('\\', '__')
                zipfilepath_relative = os.path.join('data', folder_flat + '__' + shpfilename_noext + '.zip')
                zipfilepath = os.path.join(os.getcwd(), zipfilepath_relative)
                print('Creating zip file ' + zipfilepath)
                zipf = zipfile.ZipFile(zipfilepath, 'w')
                print('Finding Files to add to zip')
                # Include all files with shpfile's name
                files_to_zip = glob.glob(os.path.join(path, shpfilename_noext + '.*'))
                for file_to_zip in files_to_zip:
                    # Do not add the zip file into the zip file...
                    if not file_to_zip.endswith('.zip'):
                        zipf.write(file_to_zip, os.path.split(file_to_zip)[1])
                zipf.close()

                # Upload zip file to ftp server
                ftp_remote_dir = 'harvesters/GVA/data'
                # todo: uncomment to enable shp file uploading again
                common.upload_ftp(zipfilepath_relative, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, ftp_remote_dir)

                # Load metadata from geocat.ch
                # See documentation at https://www.geocat.admin.ch/de/dokumentation/csw.html
                # For unknown reasons (probably proxy-related), requests always returns http error 404, so we have to revert to launching curl in a subprocess
                # curl -X GET "https://www.geocat.ch/geonetwork/srv/api/0.1/records/289b9c0c-a1bb-4ffc-ba09-c1e41dc7138a" -H "accept: application/json" -H "Accept: application/xml" -H "X-XSRF-TOKEN: a1284e46-b378-42a4-ac6a-d48069e05494"
                # resp = requests.get('https://www.geocat.ch/geonetwork/srv/api/0.1/records/2899c0c-a1bb-4ffc-ba09-c1e41dc7138a', params={'accept': 'application/json'}, proxies={'https': credentials.proxy})
                # resp = requests.get('https://www.geocat.ch/geonetwork/srv/api/0.1/records/2899c0c-a1bb-4ffc-ba09-c1e41dc7138a', headers={'accept': 'application/xml, application/json'}, proxies={'https': credentials.proxy})
                # cmd = 'curl -X GET "https://www.geocat.ch/geonetwork/srv/api/0.1/records/289b9c0c-a1bb-4ffc-ba09-c1e41dc7138a" -H "accept: application/json" -H "accept: application/json" -k'
                # args = shlex.split(cmd)

                # In some geocat URLs there's a tab character, remove it.
                geocat_uid = row['geocat'].rsplit('/', 1)[-1].replace('\t', '')
                metadata_file = 'metadata' + '/' + geocat_uid + '.json'
                cmd = 'curl -X GET "https://www.geocat.ch/geonetwork/srv/api/0.1/records/' + geocat_uid + '" -H "accept: application/json" -H "accept: application/json" -k > ' + os.getcwd() + '/' + metadata_file
                resp = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE)
                print('Processing geocat.ch metadata file ' + metadata_file + '...')
                with open(metadata_file, 'r', encoding='cp1252') as json_file:
                    json_string = json_file.read()
                    metadata = json.loads(json_string)

                    # Geocat dataset descriptions are in lists if given in multiple languages. Let's assume that the German text is always the first element in the list.
                    descriptionTextGroup = metadata['gmd:identificationInfo']['che:CHE_MD_DataIdentification']['gmd:abstract']['gmd:PT_FreeText']['gmd:textGroup']
                    description = descriptionTextGroup[0]['gmd:LocalisedCharacterString']['#text'] if isinstance(descriptionTextGroup, list) else descriptionTextGroup['gmd:LocalisedCharacterString']['#text']

                    modified = datetime.strptime(str(row['dateaktualisierung']), '%Y%m%d').date().strftime("%Y-%m-%d")
                    # Add entry to harvester file
                    metadata_for_ods.append({
                        'name':  geocat_uid + ':' + shpfilename_noext,
                        'title': row['titel'].replace(':', ': ') + ': ' + shpfilename_noext if len(shpfiles) > 1 else row['titel'].replace(':', ': '),
                        'description': description,
                        # gmd: identificationInfo.che: CHE_MD_DataIdentification.gmd:abstract.gco: CharacterString.# text
                        'dcat_ap_ch.domain': 'geoinformation-kanton-basel-stadt',
                        'dcat_ap_ch.rights': 'NonCommercialAllowed-CommercialAllowed-ReferenceRequired',
                        # License has to be set manually for the moment, since we cannot choose one of the predefined ones through this harvester type
                        # 'license': 'https://www.geo.bs.ch/nutzung/nutzungsbedingungen.html',
                        # 'attributions': 'https://www.geo.bs.ch/nutzung/nutzungsbedingungen.html',
                        # For some datasets, keyword is a list
                        # 'keyword': isinstance(metadata["gmd:identificationInfo"]["che:CHE_MD_DataIdentification"]["gmd:descriptiveKeywords"][0]["gmd:MD_Keywords"]["gmd:keyword"], list)
                        # if metadata["gmd:identificationInfo"]["che:CHE_MD_DataIdentification"]["gmd:descriptiveKeywords"][0]["gmd:MD_Keywords"]["gmd:keyword"][0]["gco:CharacterString"]["#text"]
                        # else metadata["gmd:identificationInfo"]["che:CHE_MD_DataIdentification"]["gmd:descriptiveKeywords"][0]["gmd:MD_Keywords"]["gmd:keyword"]["gco:CharacterString"]["#text"],
                        # 'publisher': metadata['gmd:contact']['che:CHE_CI_ResponsibleParty']["gmd:positionName"]["gco:CharacterString"]['#text'],
                        'publisher': row['kontakt_dienststelle'], # + ' Basel-Stadt',
                        # 'dcat.created': metadata['gmd:identificationInfo']['che:CHE_MD_DataIdentification']['gmd:citation']['gmd:CI_Citation']['gmd:date']['gmd:CI_Date']['gmd:date']['gco:Date']['#text'],
                        'dcat.issued': modified,
                        'modified': modified,
                        'language': 'de',
                        'source_dataset': 'https://data-bs.ch/opendatasoft/harvesters/GVA/' + zipfilepath_relative,
                    })

        # No shp file: find out filename
        if len(shpfiles) == 0:
            test = 0
            # Load metadata from geocat.ch
            # Add entry to harvester file
            # FTP upload file

# Save harvester file
ods_metadata = pd.DataFrame().append(metadata_for_ods, ignore_index=True, sort=False)
ods_metadata_filename = 'Opendatasoft_Export_GVA.csv'
ods_metadata.to_csv(ods_metadata_filename, index=False, sep=';' )

# FTP upload file
print('Uploading ODS harvester file to FTP Server...')
common.upload_ftp(ods_metadata_filename, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'harvesters/GVA')


print('Job successful.')




