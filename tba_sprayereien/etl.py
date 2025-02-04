import common
import logging
from tba_sprayereien import credentials
import os
from requests.auth import HTTPBasicAuth
import pandas as pd
import geopandas as gpd
from io import StringIO


def main():
    r = common.requests_get(url=credentials.url, auth=HTTPBasicAuth(credentials.user, credentials.pw))
    if len(r.text) == 0:
        logging.error('No data retrieved from API!')
        raise RuntimeError('No data retrieved from API.')
    else:
        df = pd.read_json(r.text)
        gdf = gpd.GeoDataFrame(df, geometry=gpd.GeoSeries.from_wkt(df['geometry']))
        gdf.set_geometry('geometry', inplace=True)
        gdf = gdf.set_crs('EPSG:2056')
        gdf = gdf.to_crs('EPSG:4326')
        logging.info("Calculate PLZ, Wohnviertel and Wohnbezirk for each parking lot based on centroid...")
        gdf_plz = download_spatial_descriptors('100016')
        gdf['plz'] = gdf['geometry'].apply(lambda x: get_first_value(x, gdf_plz, 'plz'))
        gdf_viertel = download_spatial_descriptors('100042')
        gdf['wov_id'] = gdf['geometry'].apply(lambda x: get_first_value(x, gdf_viertel, 'wov_id'))
        gdf['wov_name'] = gdf['geometry'].apply(lambda x: get_first_value(x, gdf_viertel, 'wov_name'))
        gdf_bezirke = download_spatial_descriptors('100039')
        gdf['bez_id'] = gdf['geometry'].apply(lambda x: get_first_value(x, gdf_bezirke, 'bez_id'))
        gdf['bez_name'] = gdf['geometry'].apply(lambda x: get_first_value(x, gdf_bezirke, 'bez_name'))
        # Removing the microseconds
        gdf['erfassungszeit'] = gdf['erfassungszeit'].str.split('.').str[0]
        # Adding the time zone
        gdf['erfassungszeit'] = pd.to_datetime(gdf['erfassungszeit']).dt.tz_localize('Europe/Zurich')
        path_all = os.path.join(credentials.data_path, 'sprayereien.csv')
        gdf.to_csv(path_all, index=False)
        # Split geometry into lon and lat
        gdf['geometry'] = gdf['geometry'].to_crs('EPSG:2056')
        gdf['lon'] = gdf['geometry'].x
        gdf['lat'] = gdf['geometry'].y
        # Rasterize coordinates
        logging.info("Rasterizing coordinates and getting rid of data we don't want to have published...")
        offset_lon = 2608700
        offset_lat = 1263200
        raster_size = 50
        gdf['raster_lat'] = ((gdf.lat - offset_lat) // raster_size) * raster_size + offset_lat
        gdf['raster_lon'] = ((gdf.lon - offset_lon) // raster_size) * raster_size + offset_lon
        columns_of_interest = ['id', 'erfassungszeit', 'spray_typ', 'plz', 'wov_id', 'wov_name', 'bez_id', 'bez_name',
                               'raster_lat', 'raster_lon']
        gdf = gdf[columns_of_interest]
        path_export = os.path.join(credentials.data_path, 'export', '100389_sprayereien.csv')
        gdf.to_csv(path_export, index=False)
        common.update_ftp_and_odsp(path_export, 'tba/sprayereien', '100389')


def download_spatial_descriptors(ods_id):
    url = f'https://data.bs.ch/api/explore/v2.1/catalog/datasets/{ods_id}/exports/geojson'
    r = common.requests_get(url)
    gdf = gpd.read_file(StringIO(r.text))
    return gdf.to_crs('EPSG:4326')


def get_first_value(x, gdf, column_name):
    matches = gdf[gdf.contains(x)][column_name]
    if not matches.empty:
        return matches.iloc[0]
    else:
        return None  # Or another default value, e.g. np.nan


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info('Job successful!')
    
