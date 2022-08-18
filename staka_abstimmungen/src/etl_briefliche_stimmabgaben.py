import pandas as pd
from staka_abstimmungen import credentials
import common

columns = ['tag', 'datum', 'eingang_pro_tag', 'eingang_kumuliert', 'stimmbeteiligung']


df_stimmabgaben = pd.read_excel(credentials.local_stimmabgaben,
                                sheet_name=0,
                                header=None,
                                names=columns,
                                skiprows=6)

push_url = credentials.ods_live_realtime_push_url
push_key = credentials.ods_live_realtime_push_key
common.ods_realtime_push_df(df_stimmabgaben, url=push_url, push_key=push_key)