import geopandas as gpd
import pandas as pd
import numpy as np
import ods_publish.etl_id as odsp
import datetime
import logging
import zipfile
import common
import os
import io
from mobilitaet_parkflaechen import credentials
from common import change_tracking as ct


# Copied from ../kapo_ordnungsbussen/src/etl.py
def list_directories(list_path):
    folder_path = credentials.data_orig_path
    directories = [f for f in os.listdir(folder_path) if os.path.isdir(os.path.join(folder_path, f))]
    with open(list_path, 'w') as file:
        for item in directories:
            file.write(item + '\n')
    list_directories = [x for x in directories if x not in []] # List folders that should be ignored here
    list_directories.sort()
    return list_directories


def download_spatial_descriptors(ods_id):
    url_to_shp = f'https://data.bs.ch/explore/dataset/{ods_id}/download/?format=shp&timezone=Europe/Berlin&lang=de'
    r = common.requests_get(url_to_shp)
    # Unpack zip file
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall(os.path.join(credentials.data_path, ods_id))
    # Read shapefile
    path_to_shp = os.path.join(credentials.data_path, ods_id, f'{ods_id}.shp')
    gdf = gpd.read_file(path_to_shp, encoding='utf-8')
    return gdf.to_crs('EPSG:2056')  # Change to a suitable projected CRS


def main():
    list_path = os.path.join(credentials.data_orig_path, 'list_directories.txt')
    directories = list_directories(list_path)
    if True or ct.has_changed(list_path):
        newest_folder = max(directories, key=lambda d: datetime.datetime.strptime(d, '%Y-%m-%d'))
        logging.info(f'Newest folder is {newest_folder}')
        path_to_shp = os.path.join(credentials.data_orig_path, newest_folder, 'Parkflaechen_vollstaendig.shp')
        logging.info(f'Reading {path_to_shp}...')
        gdf = gpd.read_file(path_to_shp)
        gdf['AKTUELL'] = np.where((gdf['GILT_BIS'].isnull()) | (pd.to_datetime(gdf['GILT_BIS']) > datetime.datetime.now()),
                                  'Ja', 'Nein')

        logging.info("Calculate PLZ, Wohnviertel and Wohnbezirk for each parking lot based on centroid...")
        gdf['centroid'] = gdf['geometry'].centroid
        gdf_plz = download_spatial_descriptors('100016')
        gdf['PLZ'] = gdf['centroid'].apply(lambda x: gdf_plz[gdf_plz.contains(x)]['plz'].values[0])
        gdf_viertel = download_spatial_descriptors('100042')
        gdf['WOV_ID'] = gdf['centroid'].apply(lambda x: gdf_viertel[gdf_viertel.contains(x)]['wov_id'].values[0])
        gdf['WOV_NAME'] = gdf['centroid'].apply(lambda x: gdf_viertel[gdf_viertel.contains(x)]['wov_name'].values[0])
        gdf_bezirke = download_spatial_descriptors('100039')
        gdf['BEZ_ID'] = gdf['centroid'].apply(lambda x: gdf_bezirke[gdf_bezirke.contains(x)]['bez_id'].values[0])
        gdf['BEZ_NAME'] = gdf['centroid'].apply(lambda x: gdf_bezirke[gdf_bezirke.contains(x)]['bez_name'].values[0])

        gdf['GEO_SHAPE'] = gdf['geometry'].to_crs('EPSG:4326')
        gdf['CENTROID'] = gdf['centroid'].to_crs('EPSG:4326')

        path_export = os.path.join(credentials.data_path, 'export', '100329_parkflaechen.csv')
        logging.info(f'Exporting data to {path_export}...')
        if ct.has_changed(path_export):
            gdf.to_csv(path_export, index=False)
            common.upload_ftp(path_export, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass,
                              'mobilitaet/parkflaechen')
            odsp.publish_ods_dataset_by_id('100329')
            ct.update_hash_file(path_export)
        ct.update_hash_file(list_path)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()