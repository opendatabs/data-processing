# Documentation for the Job "staka_abstimmungen"

## What does this job do?
- The Airflow DAG is runs regularly (e.g. every 2 minutes, see https://github.com/opendatabs/docker-airflow/blob/master/dags/staka_abstimmungen.py) and calls etl_auto.sh, which in turn starts the Python program src/etl.py
- etl.py does the following: 
  - It reads control.csv, which contains one line per Abstimmungs-Sonntag, with the following columns (all timestampos are in "Europe/Zurich" timezone: 
    - `Active`: Should this line be taken into account
    - `Abstimmungs_datum`: Date of the Abstimmungs-Sonntag for this line
    - `Ignore_changes_before`: Don't do anything before this timestamp
    - `Embargo`: Do not make any data public before this timestamp
    - `Ignore_changes_after`: Don't do anything after this timestamp
    - `ODS_id_Kennzahlen_Live`: Id of the LIVE Kennzahlen dataset on https://data.bs.ch  
    - `ODS_id_Details_Live`: Id of the LIVE Details dataset on https://data.bs.ch  
    - `ODS_id_Kennzahlen_Test`: Id of the TEST Kennzahlen dataset on https://data.bs.ch
    - `ODS_id_Details_Test`: Id of the LIVE Details dataset on https://data.bs.ch
  - If it is time to do something, the job checks if new data files have been received on the server. 
  - If new data is found, two separate csv files are calculated and uploaded to the FTP server: 'Abstimmungen_YYYY-MM-DD.csv' and 'Abstimmungen_Details_YYYY-MM-DD.csv'
  - If the timestamp set in parameter `Embargo` has passed, the two live datasets read from parameters `ODS_id_Kennzahlen_Live` and `ODS_id_Details_Live` are published and their general access policy is set to `public`. 
  
## Manual steps to do before each Abstimmungs-Sonntag: 
- On https://data.bs.ch: 
  - Duplicate the two datasets filled during the previous Abstimmungs-Sonntag, change their general access policy to `restricted`. 
  - Change id, title and other metadata of these new datasets as needed. 
- Open `control.csv` in a text editor (do not use Excel, it might break the timestamp data format): 
  - Create a new line for the next Abstimmungs-Sonntag by copying the previous one and changing all the dates. 
  - Set parameter `active` on the line of all previous Abstimmungs-Sonntage to `False`. Only the line for the upcoming one should be set to `True`.
  - Fill in the ids of the newly created two datasets in this line.
  - Save `control.csv`. 
- Wait until the Airflow job has successfully run. 
- If there was new data (e.g. test data), check if the two new csv files are uploaded to the FTP server. 
- Retrieve the ids of all 4 datasets for the upcoming Abstimmungs-Sonntag from the active line in `control.csv` and, for each one, change the source csv to the respective newly created csv on the FTP Server. 
- Run all the tests to make sure the data has been correctly parsed and uploaded. 