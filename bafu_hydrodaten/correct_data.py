import common
import pandas as pd
import urllib3
from bafu_hydrodaten import credentials

# Push correct data received from BAFU to ODS via realtime API

df_pegel = pd.read_csv(
    "/Users/jonasbieri/PycharmProjects/data-processing/bafu_hydrodaten/data/BAFU-2289-2-1_07.03.2021-00_00_00+0100_17.03.2021-00_00_00+0100.csv",
    sep="\t",
)
df_abfluss = pd.read_csv(
    "/Users/jonasbieri/PycharmProjects/data-processing/bafu_hydrodaten/data/BAFU-2289-10-11_07.03.2021-00_00_00+0100_17.03.2021-00_00_00+0100.csv",
    sep="\t",
)
df = df_pegel.merge(df_abfluss, how="outer", on="Date Time [GMT+1]")
df = df.rename(
    columns={
        "Date Time [GMT+1]": "datum_zeit",
        "Value [m3/s]": "abfluss",
        "Value [m ü. M.]": "pegel",
    }
)

df["timestamp_dt"] = pd.to_datetime(
    df.datum_zeit, format="%d.%m.%Y %H:%M:%S"
).dt.tz_localize("Etc/GMT-1")
# in the following line we created the wrong dates since Python only takes the first date to infer the format...!
# df['timestamp_dt'] = pd.to_datetime(df.datum_zeit, infer_datetime_format=True).dt.tz_localize('Etc/GMT-1')
df["timestamp"] = df.timestamp_dt.dt.strftime("%Y-%m-%dT%H:%M:%S%z")

realtime_df = df[["timestamp", "pegel", "abfluss"]]
payload = realtime_df.to_json(orient="records")
print(f"Pushing {realtime_df.timestamp.count()} rows to ODS realtime API...")
# print(f'Pushing the following data to ODS: {json.dumps(json.loads(payload), indent=4)}')
urllib3.disable_warnings()

r = common.requests_post(
    url=credentials.ods_live_push_api_url, data=payload, verify=False
)
r.raise_for_status()
print(r.json())

# oh no, the date was set incorrectly (month and day were mixed up by Python with 'infer_datetime_format=True' above), so we have to delete the pushed data.
# see https://help.opendatasoft.com/platform/en/publishing_data/03_scheduling_updates/scheduling_updates.html#delete-data

# delete_data_url = credentials.ods_live_push_api_url.replace('/push/?pushkey=', '/delete/?pushkey=')
# r = common.requests_post(url=delete_data_url, data=payload, verify=False)
# r.raise_for_status()
# print(r.json())
