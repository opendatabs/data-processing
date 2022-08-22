import pandas as pd
from staka_abstimmungen import credentials
import common
import os
import glob
from datetime import datetime
import locale
import numpy as np

# datetime in German
# MAC:
locale.setlocale(locale.LC_TIME, 'de_DE.UTF-8')
# Windows:
# locale.setlocale(
#     category=locale.LC_ALL,
#     locale="German"  # Note: do not use "de_DE" as it doesn't work
# )


columns = ['tag', 'datum', 'eingang_pro_tag', 'eingang_kumuliert', 'stimmbeteiligung']
dtypes = {'datum': 'datetime64'
 }


df_stimmabgaben = pd.read_excel(credentials.local_stimmabgaben,
                                sheet_name=0,
                                header=None,
                                names=columns,
                                skiprows=6,
                                dtype=dtypes
                                )

print(df_stimmabgaben)

# push_url = credentials.ods_live_realtime_push_url
# push_key = credentials.ods_live_realtime_push_key
# common.ods_realtime_push_df(df_stimmabgaben, url=push_url, push_key=push_key)


pattern = '????????_Eingang_Stimmabgaben*.xlsx'
data_file_names = []

file_list = glob.glob(os.path.join(credentials.path_local, pattern))
if len(file_list) > 0:
    latest_file = max(file_list, key=os.path.getmtime)
    data_file_names.append(os.path.basename(latest_file))
date_abst = data_file_names[0].split("_", 1)[0]
date_abst = datetime.strptime(date_abst, '%Y%m%d')

df_stimmabgaben['tage_bis_abst'] = [(date_abst - d0).days for d0 in df_stimmabgaben['datum']]

df_stimmabgaben['datum_str'] = df_stimmabgaben['datum'].dt.strftime('%A %-d %B')

df_stimmabgaben['stimmbeteiligung'] = 100 * df_stimmabgaben['stimmbeteiligung']
df_stimmabgaben['stimmbeteiligung_str'] = [str(round(x, 1)) + ' %' if not np.isnan(x) else '' for x in df_stimmabgaben['stimmbeteiligung']]
print(df_stimmabgaben)

df_stimmabgaben.to_csv("briefliche_stimmabgaben.csv",index=False)

