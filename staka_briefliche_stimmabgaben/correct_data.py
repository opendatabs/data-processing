import pandas as pd
import common
from staka_briefliche_stimmabgaben import credentials

# remove data that has been imported with the wrong Abstimmungsdatum

datum = "2022-09-24"

# obtain all entries with the wrong date in 100223
req = common.requests_get(
    f'https://data.bs.ch/api/v2/catalog/datasets/100223/exports/json?refine=datum_urnengang:{datum}&limit=-1&offset=0&timezone=UTC',
    auth=(credentials.username, credentials.password))
file = req.json()
df = pd.DataFrame.from_dict(file)

# remove the records with the wrong date in 100223
payload = df.to_json(orient="records")
delete_url = credentials.ods_live_realtime_delete_url_publ
push_key = credentials.ods_live_realtime_push_key_publ
r = common.requests_post(url=delete_url, data=payload, params={'pushkey': push_key})
r.raise_for_status()
