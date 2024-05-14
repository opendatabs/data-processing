import common
import logging
from tba_baustellen import credentials
import os
from requests.auth import HTTPBasicAuth
import pandas as pd
import geopandas as gpd
import zipfile
import io
# import matplotlib.pyplot as plt


CURR_DIR = os.path.dirname(os.path.realpath(__file__))


def download_spatial_descriptors(url):
    params = {
            'format': 'shp',
            'refine.belgartbez': 'Baustelle',
            'apikey': credentials.api_key,
        }
    r2 = common.requests_get(url, params=params)
    z = zipfile.ZipFile(io.BytesIO(r2.content))
    # Extrahiere alle Dateien in einen temporären Ordner
    export_filename = f"{CURR_DIR}/data/100018"
    z.extractall(f"{CURR_DIR}/data/100018")
    # Das Shapefile in ein GeoDataFrame laden
    gdf = gpd.read_file(export_filename, encoding='utf-8')
    gdf.to_crs('EPSG:2056')
    # Gruppieren von Polygone nach "begehrenid" und Zusammenfügen der Polygone
    grouped_gdf = gdf.groupby('begehrenid')['geometry'].agg(lambda x: x.unary_union).reset_index()
    # Beispiel zum Testen
    # shape = grouped_gdf[grouped_gdf['begehrenid'] == 9077159]
    # ax = shape.plot(edgecolor='black',color='green', alpha=0.5, figsize=(10, 10))
    # ax.set_title('Polygone mit GeoPandas gezeichnet')
    # plt.show()
    return grouped_gdf


def main():
    r = common.requests_get(url=credentials.url, auth=HTTPBasicAuth(credentials.user, credentials.pw))
    if len(r.text) == 0:
        logging.error('No data retrieved from API!')
        raise RuntimeError('No data retrieved from API.')
    else:
        df = pd.read_json(r.text)
        print(df.head())
        df_export = df[['id', 'projekt_name', 'projekt_beschrieb', 'projekt_info', 'projekt_link',
                        'datum_bis', 'datum_von', 'dokument1', 'dokument2', 'dokument3']]
        df_export.datum_von = pd.to_datetime(df_export['datum_von'], format='%d.%m.%Y', errors='raise').dt.strftime('%Y-%m-%d')
        df_export.datum_bis = pd.to_datetime(df_export['datum_bis'], format='%d.%m.%Y', errors='raise').dt.strftime('%Y-%m-%d')
        df_export['allmendbewilligungen'] = "https://data.bs.ch/explore/dataset/100018/table/?refine.belgartbez=Baustelle&q=begehrenid=" + df_export.id.astype(str)
    df_allm = download_spatial_descriptors('https://data.bs.ch/explore/dataset/100018/download')
    # alle Daten rausnehmen
    df_export = df_export.merge(df_allm, how='left', left_on='id', right_on='begehrenid')
    df_export = df_export.drop(columns=['begehrenid'])
    export_filename = f"{CURR_DIR}/data/baustellen.csv"
    df_export.to_csv(export_filename, index=False)
    common.update_ftp_and_odsp(export_filename, 'tba/baustellen', '100359')


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info('Job successful!')
