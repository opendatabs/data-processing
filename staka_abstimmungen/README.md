# Documentation for the Job "staka_abstimmungen"

## What does this job do?
- The Airflow DAG is runs regularly (e.g. every 2 minutes, see [https://github.com/opendatabs/docker-airflow/blob/master/dags/staka_abstimmungen.py](https://github.com/opendatabs/dags-airflow2/blob/main/staka_abstimmungen.py)) and calls etl_auto.sh, which in turn starts the Python program src/etl.py
- etl.py does the following: 
  - It reads control.csv, which contains one line per Abstimmungs-Sonntag, with the following columns (all timestampos are in "Europe/Zurich" timezone: 
    - `Active`: Should this line be taken into account
    - `Abstimmungs_datum`: Date of the Abstimmungs-Sonntag for this line
    - `Ignore_changes_before`: Don't do anything before this timestamp
    - `Embargo`: Do not make any data public before this timestamp
    - `Ignore_changes_after`: Don't do anything after this timestamp
  - If it is time to do something, the job checks if new data files have been received on the server. 
  - If new data is found, two separate csv files are calculated and uploaded to the FTP server: 'Abstimmungen_YYYY-MM-DD.csv' and 'Abstimmungen_Details_YYYY-MM-DD.csv'
  - The data is uploaded via the realtime push API from ODS to the test datasets:
    - https://data.bs.ch/explore/dataset/100343/
    - https://data.bs.ch/explore/dataset/100344/
  - If the timestamp set in parameter `Embargo` has passed, the data is additionally pushed to the live datasets:
    - https://data.bs.ch/explore/dataset/100345/
    - https://data.bs.ch/explore/dataset/100346/
  
## Manual steps to do before each Abstimmungs-Sonntag: 
- Open `{File Server Root}\PD\PD-StatA-FST-OGD-DataExch\StatA\Wahlen-Abstimmungen\control.csv` in a text editor (do not use Excel, it might break the timestamp data format): 
  - Create a new line for the next Abstimmungs-Sonntag by copying the previous one and changing all the dates. 
  - Set parameter `active` on the line of all previous Abstimmungs-Sonntage to `False`. Only the line for the upcoming one should be set to `True`.
  - Save `control.csv`. 
- Wait until the Airflow job has successfully run. 
- If there was new data (e.g. test data), check if the two new csv files are uploaded to the FTP server. 
- Run all the tests to make sure the data has been correctly parsed and uploaded. 
