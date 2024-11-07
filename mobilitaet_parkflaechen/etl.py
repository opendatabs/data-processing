import geopandas as gpd
import pandas as pd
import ods_publish.etl_id as odsp
import datetime
import logging
import zipfile
import common
import os
import io
from io import StringIO
from requests.auth import HTTPBasicAuth
from mobilitaet_parkflaechen import credentials
from common import change_tracking as ct


def download_spatial_descriptors(ods_id):
    url_to_shp = f'https://data.bs.ch/explore/dataset/{ods_id}/download/?format=shp'
    r = common.requests_get(url_to_shp)
    # Unpack zip file
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall(os.path.join(credentials.data_path, ods_id))
    # Read shapefile
    path_to_shp = os.path.join(credentials.data_path, ods_id, f'{ods_id}.shp')
    gdf = gpd.read_file(path_to_shp, encoding='utf-8')
    return gdf.to_crs('EPSG:2056')  # Change to a suitable projected CRS


def create_diff_files(path_to_new):
    logging.info('Creating diff files...')
    # Load last version of the file
    df_new = pd.read_csv(path_to_new)
    path_to_last = os.path.join(credentials.data_path, 'parkflaechen_last_version.csv')
    if os.path.exists(path_to_last):
        df_last = pd.read_csv(path_to_last)
        # Find new rows if any
        new_rows = ct.find_new_rows(df_last, df_new, 'id')
        if len(new_rows) > 0:
            path_export = os.path.join(credentials.data_path, 'diff_files',
                                       f'parkflaechen_new_{datetime.date.today()}.csv')
            new_rows.to_csv(path_export, index=False)
        # Find modified rows if any
        deprecated_rows, updated_rows = ct.find_modified_rows(df_last, df_new, 'id')
        if len(deprecated_rows) > 0:
            path_export = os.path.join(credentials.data_path, 'diff_files',
                                       f'parkflaechen_deprecated_{datetime.date.today()}.csv')
            deprecated_rows.to_csv(path_export, index=False)
            path_export = os.path.join(credentials.data_path, 'diff_files',
                                       f'parkflaechen_updated_{datetime.date.today()}.csv')
            updated_rows.to_csv(path_export, index=False)
        # Find deleted rows if any
        deleted_rows = ct.find_deleted_rows(df_last, df_new, 'id')
        if len(deleted_rows) > 0:
            path_export = os.path.join(credentials.data_path, 'diff_files',
                                       f'parkflaechen_deleted_{datetime.date.today()}.csv')
            deleted_rows.to_csv(path_export, index=False)
    # Save new version of the file as the last version
    df_new.to_csv(path_to_last, index=False)


def main():
    r = common.requests_get(url=credentials.url, auth=HTTPBasicAuth(credentials.user, credentials.pw))
    if len(r.text) == 0:
        logging.error('No data retrieved from API!')
        raise RuntimeError('No data retrieved from API.')
    else:
        data = StringIO(r.text.replace('SRID=4326;', ''))
        df = pd.read_csv(data, sep=';', index_col=False)
        # Save df into geopandas dataframe and take column geometry as geometry
        gdf = gpd.GeoDataFrame(df, geometry=gpd.GeoSeries.from_wkt(df['geometry']))
        gdf.set_geometry('geometry', inplace=True)
        gdf = gdf.set_crs('EPSG:4326')
        gdf = gdf.to_crs('EPSG:2056')  # Change to a suitable projected CRS
        logging.info("Calculate PLZ, Wohnviertel and Wohnbezirk for each parking lot based on centroid...")
        gdf['centroid'] = gdf['geometry'].centroid
        gdf_plz = download_spatial_descriptors('100016')
        gdf['plz'] = gdf['centroid'].apply(lambda x: gdf_plz[gdf_plz.contains(x)]['plz'].values[0])
        gdf_viertel = download_spatial_descriptors('100042')
        gdf['wov_id'] = gdf['centroid'].apply(lambda x: gdf_viertel[gdf_viertel.contains(x)]['wov_id'].values[0])
        gdf['wov_name'] = gdf['centroid'].apply(lambda x: gdf_viertel[gdf_viertel.contains(x)]['wov_name'].values[0])
        gdf_bezirke = download_spatial_descriptors('100039')
        gdf['bez_id'] = gdf['centroid'].apply(lambda x: gdf_bezirke[gdf_bezirke.contains(x)]['bez_id'].values[0])
        gdf['bez_name'] = gdf['centroid'].apply(lambda x: gdf_bezirke[gdf_bezirke.contains(x)]['bez_name'].values[0])

        # Filter on PLZ that start with 40 (Basel)
        gdf = gdf[gdf['plz'].str.startswith('40')]

        gdf['geometry'] = gdf['geometry'].to_crs('EPSG:4326')
        gdf['centroid'] = gdf['centroid'].to_crs('EPSG:4326')

        logging.info("Merge with tarif_subzonen to decode tarif codes...")
        df_tarif = pd.read_csv(os.path.join(credentials.data_path, 'tarif_subzonen.csv'))
        gdf = gdf.merge(df_tarif, left_on='tarif_code', right_on='TARIF_C1', how='left')
        gdf = gdf.rename(columns={'SOPFG_GEB': 'sopfg_geb', 'GEBPFLICHT': 'gebpflicht',
                                  'MAXPARKZ': 'maxparkz', 'KEINL': 'keinl'}).drop(columns=['TARIF_C1'])
        gdf['tarif_gebiet'] = gdf['tarif_code'].str[:1]

        columns_of_interest = ['id', 'anzahl_parkfelder', 'id_typ', 'typ', 'tarif_gebiet', 'sopfg_geb',
                               'tarif_id', 'tarif_code', 'gebpflicht', 'maxparkz', 'keinl',
                               'plz', 'wov_id', 'wov_name', 'bez_id', 'bez_name', 'strasse']
        gdf_export = gdf[columns_of_interest]
        path_export = os.path.join(credentials.data_path, 'export', '100329_parkflaechen.csv')
        logging.info(f'Exporting data to {path_export}...')
        gdf_export.to_csv(path_export, index=False)
        if ct.has_changed(path_export):
            common.upload_ftp(path_export, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass,
                              'mobilitaet/parkflaechen')
            odsp.publish_ods_dataset_by_id('100329')
            ct.update_hash_file(path_export)
        path_to_new = os.path.join(credentials.data_path, 'parkflaechen_new_version.csv')
        gdf.to_csv(path_to_new, index=False)
        if ct.has_changed(path_to_new):
            create_diff_files(path_to_new)
            ct.update_hash_file(path_to_new)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info('Job successful!')
