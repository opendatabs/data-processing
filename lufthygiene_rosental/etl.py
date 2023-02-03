import pandas as pd
import io
import common
import logging
from lufthygiene_rosental import credentials



url = 'https://data-bs.ch/lufthygiene/Rosental-Mitte/online/airmet_bs_rosental_pm25_aktuell'
logging.info(f'Downloading data from {url}...')
r = common.requests_get(url)
r.raise_for_status()
s = r.text
df = pd.read_csv(io.StringIO(s), sep=';')
df.to_csv('rosental_pm25.csv', index=False)
df = df.dropna(subset=['Anfangszeit'])
df['timestamp'] = pd.to_datetime(df.Anfangszeit, format='%d.%m.%Y %H:%M:%S').dt.tz_localize('Europe/Zurich', ambiguous=True, nonexistent='shift_forward')


if len(df) == 0:
    logging.info('No rows to push to ODS... ')
else:
    logging.info(f'Melting dataframe...')
    ldf = df.melt(id_vars=['Anfangszeit', 'timestamp'], var_name='station', value_name='pm_2_5')
    logging.info(f'Dropping rows with empty pm25 value...')
    ldf['pm_2_5'] = pd.to_numeric(ldf['pm_2_5'], errors='coerce')
    ldf = ldf.dropna(subset=['pm_2_5'])
    row_count = ldf.timestamp.count()
    if row_count == 0:
        logging.info(f'No rows to push to ODS... ')
    else:
        logging.info(f'Pushing {row_count} rows to ODS realtime API...')
        ldf.timestamp = ldf.timestamp.dt.strftime('%Y-%m-%d %H:%M:%S%z')
        payload = ldf.to_json(orient="records")
        r = common.requests_post(url=credentials.ods_live_push_api_url, data=payload, verify=False)
        r.raise_for_status()

    logging.info('Job successful!')


 # Realtime API bootstrap data:
 #        {
 #            "Anfangszeit": "13.08.2020 00:30:00",
 #            "timestamp": "2020-08-12T22:30:00+00:00",
 #            "station": "Feldbergstrasse",
 #            "pm_2_5": 8.8
 #        }

# if __name__ == "__main__":
#     print(f'Executing {__file__}...')
#     main()

