import common
import logging
from tba_baustellen import credentials
import os
from datetime import datetime
from requests.auth import HTTPBasicAuth
import pandas as pd


def main():
    r = common.requests_get(url=credentials.url, auth=HTTPBasicAuth(credentials.user, credentials.pw))
    if len(r.text) == 0:
        logging.error('No data retrieved from API!')
        raise RuntimeError('No data retrieved from API.')
    else:
        df = pd.read_json(r.text)
        df_export = df[['id', 'projekt_name', 'projekt_beschrieb', 'projekt_info', 'projekt_link',
                        'datum_bis', 'datum_von', 'dokument1', 'dokument2', 'dokument3']]
        df_export.datum_von = pd.to_datetime(df_export['datum_von'], format='%d.%m.%Y', errors='raise').dt.strftime('%Y-%m-%d')
        df_export.datum_bis = pd.to_datetime(df_export['datum_bis'], format='%d.%m.%Y', errors='raise').dt.strftime('%Y-%m-%d')
        df_export['allmendbewilligungen'] = "https://data.bs.ch/explore/dataset/100018/table/?refine.belgartbez=Baustelle&q=begehrenid=" + df_export.id.astype(str)

        url = f'https://data.bs.ch/explore/dataset/100018/download'
        params = {
            'format': 'shp',
            'timezone': 'Europe/Zurich',
            'refine.belgartbez': 'Baustelle',
            # TODO: Weitere params hinzufügen
            'apikey': credentials.api_key
        }
        r2 = common.requests_get(url, params=params)

        # TODO: Shapefile downloaden und in GeoDataFrame umwandeln
        # TODO: Geoshapes von allen gleichen Baustellen joinen
        # TODO: Merge mit df_export um Spalte mit Geoshapes zu erstellen

        # data_allm = []
        # for index, row in df_export.iterrows():
        #     r2 = common.requests_get(f"https://data.bs.ch/api/explore/v2.1/catalog/datasets/100018/records?where=belgartbez%20like%20%22Baustelle%22%20and%20begehrenid%20%3D%{row.id}&limit=20")
        #     bew_count = r2.json()['total_count']
        #     if bew_count == 1:
        #         data_allm.append([row.id, r2.json()['results'][0]['geo_shape']['geometry']])
        #     else:
        #         logging.error(f'Number of results from ODS API Call <> 1: {bew_count}')
        #         raise RuntimeError(f'Number of results from ODS API Call <> 1: {bew_count}')

        curr_dir = os.path.dirname(os.path.realpath(__file__))
        export_filename = f"{curr_dir}/data/baustellen.csv"
        df_export.to_csv(export_filename, index=False)
        common.update_ftp_and_odsp(export_filename, 'tba/baustellen', '100359')


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info('Job successful!')
