import pandas as pd
import common
from lufthygiene_pm25 import credentials

"""
To do if a sensor has been replaced with a new one that has a different entry for "station":

First: Add a line in the etl.py file to rename the station name back to the old one, i.e. df.rename(columns={'bl_StJohann2': 'St.Johann'}, inplace=True)

Second: If data with the new sensor name has already been pushed to the dataportal: define the variables new_sensor and station in change_sensor_name.py and then run this file. 

"""

new_sensor = "bl_StJohann2"
station = "St.Johann"


# obtain all entries with the new sensor name that have already been pushed to the data portal
req = common.requests_get(f'https://data.bs.ch/api/v2/catalog/datasets/100081/exports/json?where=station%20%3D%20%20%22{new_sensor}%22&limit=-1&offset=0&timezone=UTC')
file = req.json()
df = pd.DataFrame.from_dict(file)


# transform df s.t. it corresponds to the realtime API bootstrap... (see scheme in etl.py)
df['timestamp'] = df['zeitstempel']
df['Zeit'] = pd.to_datetime(df.timestamp, format='%Y-%m-%d %H:%M:%S%z').dt.strftime('%d.%m.%Y %H:%M:%S')
df = df[["Zeit","timestamp", "pm_2_5", "station"]]


# re-push with station name changed back to original station name since some data is maybe not in the latest csv file anymore
df_new = df.copy()
df_new["station"] = station

payload = df_new.to_json(orient="records")
r = common.requests_post(url=credentials.ods_live_push_api_url, data=payload, verify=False)
r.raise_for_status()


# remove the records with station = new_sensor
payload = df.to_json(orient="records")
r = common.requests_post(url=credentials.ods_live_delete_api_url, data=payload, verify=False)
r.raise_for_status()
