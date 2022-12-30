import pandas as pd
import os
import numpy as np
from common import change_tracking as ct


filename = os.path.join(os.path.dirname(__file__), 'data/gefahrenstufen', 'warn_levels.xml')
if ct.has_changed(filename, method='modification_date'):
    df = pd.read_xml(filename)

    stations = [2289, 2106, 2199]

    df = df[df['EDV'].isin(stations)]

    df[['WL1', 'WL2', 'WL3', 'WL4']] = df[['WL1', 'WL2', 'WL3', 'WL4']].astype(np.int64)

    df['gefahrenstufe_1'] = '< ' + df['WL1'].astype(str)
    df['gefahrenstufe_2'] = df['WL1'].astype(str) + ' - ' + df['WL2'].astype(str)
    df['gefahrenstufe_3'] = df['WL2'].astype(str) + ' - ' + df['WL3'].astype(str)
    df['gefahrenstufe_4'] = df['WL3'].astype(str) + ' - ' + df['WL4'].astype(str)
    df['gefahrenstufe_5'] = '>' + df['WL4'].astype(str)

    df['station_id'] = station_str = df['EDV'].astype(str)
    df['station_name'] = df['Name']
    df['link'] = 'https://www.hydrodaten.admin.ch/de/' + station_str + '.html'

    df_export = df[['station_id', 'station_name', 'WL1', 'WL2', 'WL3', 'WL4', 'gefahrenstufe_1',
                    'gefahrenstufe_2', 'gefahrenstufe_3', 'gefahrenstufe_4',
                    'gefahrenstufe_5', 'link']]

    export_filename = os.path.join(os.path.dirname(__file__), 'data/gefahrenstufen', 'gefahrenstufen.csv')
    df_export.to_csv(export_filename, index=False)
    ct.update_mod_timestamp_file(filename)
