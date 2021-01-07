# Rhein Wasserstand, Pegel und Abfluss
https://data.bs.ch/explore/dataset/100089

- Script etl_https.py is executed in short intervals:
    - Pulls csv data from the 2 data sources of https://www.hydrodaten.admin.ch/de/2289.html through authenticated https.
    - Merges and transforms the two csv into on csv file. 
    - Uploads the csv to the realtime folder on the ftp server.
- Cron job in https://github.com/opendatabs/data-bs.ch/blob/master/cronjobs/bafu_hydrodaten/cronjobs.sh is executed on the ftp server. It moves this file to the archive folder shortly before midnight. 
- ODS reads both archive and realtime folder via FTP dataset source. 
- Because no files in the archive folder are ever modified, only files added: Only the added file is parsed by ODS. 
- In the realtime folder there's only ever one fairly small file, thus ODS parses this pretty fast. 