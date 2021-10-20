# Smart Climate Feinstaubmessungen
https://data.bs.ch/explore/dataset/100081

- There are two ODS sources: 
    - An archive csv with all data up to the day before manual archive creation
    - ODS Realtime Push data source
- If the dataset is deleted in ODS, or data is lost for some other reason, do the following:   
    - Delete and recreate the realtime data source
    - Recreate the archive csv, upload to the FTP server, then recreate the archive data source in ODS as described below. 
- The archive csv is created by: 
    - Downloading all daily csv files via FTP from the folder https://data-bs.ch/lufthygiene/pm25/archive/ to c:/dev/workspace/data-processing/lufthygiene_pm25/data/
    - Running Python script initial_load.py
    - Uploading the created archive csv to the FTP Server
    - Updating the ODS data source to point to the newly created archive. 
    
Why? We cannot simply use the FTP folder as ODS data source, because the data is in a wide format and must be melted first. 