from datetime import datetime
import os

import geopandas as gpd
import pandas as pd

import common
import io
import logging
import zipfile
from stadtreinigung_sauberkeitsindex import credentials
from requests.auth import HTTPBasicAuth
from charset_normalizer import from_path
import shutil

def main():
    r = common.requests_get(url=credentials.url, auth=HTTPBasicAuth(credentials.user, credentials.pw))
    if len(r.text) == 0:
        logging.error('No data retrieved from API!')
        raise RuntimeError('No data retrieved from API.')
    
    curr_dir = os.path.dirname(os.path.realpath(__file__))
    export_filename = f"{curr_dir}/data/data_{datetime.now().strftime('%Y-%m')}.csv"
    with open(export_filename, 'w') as file:
        file.write(r.text)
    df = add_datenstand(export_filename)
    df.to_csv(export_filename, encoding='cp1252', index=False)
    common.update_ftp_and_odsp(export_filename, 'stadtreinigung/sauberkeitsindex/roh', '100288')

    aggregate_and_upload_to_ftp(curr_dir=curr_dir)

def add_datenstand(path_csv):
    result = from_path(path_csv)
    enc = result.best().encoding
    df = pd.read_csv(path_csv, encoding=enc, sep=';')
    df['datenstand'] = pd.to_datetime(os.path.basename(path_csv).split('_')[1].split('.')[0])
    return df

def delete_tmp_dir(tmp_path):
    logging.debug(f"Attempting to delete temporary directory: {tmp_path}")
    
    if not os.path.isdir(tmp_path):
        logging.info(f"Directory does not exist, nothing to delete: {tmp_path}")
        return

    try:
        shutil.rmtree(tmp_path)
        logging.debug(f"Temporary directory deleted successfully: {tmp_path}")
    except Exception as e:
        logging.error(f"Error deleting temporary directory {tmp_path}: {e}")
        raise

def create_aggregated_csv(curr_dir: str, filename_aggregated_csv: str, tmp_dir_name: str, data_agg_dir_name: str):
    tmp_path = os.path.join(curr_dir, tmp_dir_name)
    data_agg_path = os.path.join(curr_dir, data_agg_dir_name)

    logging.debug(f"Temporary directory path: {tmp_path}")
    logging.debug(f"Data aggregation directory path: {data_agg_path}")

    try:
        os.makedirs(tmp_path, exist_ok=False)
        logging.debug(f"Created temporary directory: {tmp_path}")
    except FileExistsError:
        logging.warning(f"Temporary directory already exists: {tmp_path}. Deleting and recreating.")
        delete_tmp_dir(tmp_path=tmp_path)
        os.makedirs(tmp_path, exist_ok=True)
        logging.debug(f"Recreated temporary directory: {tmp_path}")

    # Download CSV files
    logging.debug("Downloading CSV files from FTP.")
    common.download_ftp(
        files=[],
        server=common.credentials.ftp_server,
        user=common.credentials.ftp_user,
        password=common.credentials.ftp_pass,
        remote_path='stadtreinigung/sauberkeitsindex/roh',
        local_path=tmp_path,
        pattern='data_*.csv'
    )
    logging.debug("CSV files downloaded successfully.")

    # Find all CSV files in tmp_path
    csv_files = [file for file in os.listdir(tmp_path) if file.endswith(".csv")]
    logging.debug(f"Found {len(csv_files)} CSV files to process.")

    # Parse filenames to extract the actual data month and year
    file_info = []
    for file in csv_files:
        date_str = os.path.basename(file).split('_')[1].split('.')[0]  # e.g., '2024-03'
        file_date = datetime.strptime(date_str, '%Y-%m')
        
        # Adjust the data date by subtracting one month
        data_date = file_date - pd.DateOffset(months=1)
        data_year = data_date.year
        data_month = data_date.month
        
        file_info.append({'file': file, 'date': data_date, 'year': data_year, 'month': data_month})
        logging.debug(f"Processed file info: {file} - Year: {data_year}, Month: {data_month}")

    # Group files by their respective quarters based on the actual data month
    quarters = {}
    for info in file_info:
        quarter = (info['year'], (info['month'] - 1) // 3 + 1)
        if quarter not in quarters:
            quarters[quarter] = []
        quarters[quarter].append(info)
    
    logging.debug(f"Grouped files into {len(quarters)} quarters: {list(quarters.keys())}")

    # Ensure that each quarter has all three months of data
    complete_quarters = {q: files for q, files in quarters.items() if len(files) == 3}
    logging.debug(f"Found {len(complete_quarters)} complete quarters: {list(complete_quarters.keys())}")

    # Download spatial descriptors
    ods_id = '100042'
    url_to_shp = f'https://data.bs.ch/explore/dataset/{ods_id}/download/?format=shp'
    r = common.requests_get(url_to_shp)
    # Unzip zip file
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall(tmp_path)
    logging.debug(f"Extracted shapefiles to temporary directory: {tmp_path}")
    
    # Read shapefile
    path_to_shp = os.path.join(tmp_path, f'{ods_id}.shp')
    gdf = gpd.read_file(path_to_shp, encoding='utf-8')
    gdf_viertel_full = gdf.to_crs('EPSG:2056')  # Change to a suitable projected CRS
    
    # Create gdf_viertel without Riehen and Bettingen
    gdf_viertel = gdf_viertel_full[~gdf_viertel_full['wov_name'].isin(['Riehen', 'Bettingen'])]
    
    logging.debug("Loaded and reprojected spatial data. Removed Riehen and Bettingen from gdf_viertel.")

    # Aggregate SKI (CCI) values by Wohnviertel and Quarter
    results = []
    for quarter, files in complete_quarters.items():
        logging.debug(f"Processing quarter: {quarter[0]}-{quarter[1]}")
        quarter_data = []
        for file_info in files:
            logging.debug(f"Processing file: {file_info['file']} for {file_info['year']}-{file_info['month']:02d}")
            df = pd.read_csv(os.path.join(tmp_path, file_info['file']), encoding='cp1252', sep=',')
            gdf = gpd.GeoDataFrame(df, geometry=gpd.GeoSeries.from_wkt(df['geometry']))
            gdf = gdf.set_crs('EPSG:2056')
            gdf['centroid'] = gdf['geometry'].centroid

            def get_wov_name(centroid):
                containing = gdf_viertel[gdf_viertel.contains(centroid)]
                if not containing.empty:
                    return containing['wov_name'].values[0]
                else:
                    nearest = gdf_viertel.geometry.distance(centroid).idxmin()
                    return gdf_viertel.loc[nearest, 'wov_name']

            gdf['wov_name'] = gdf['centroid'].apply(get_wov_name)

            quarter_data.append(gdf)
            logging.debug(f"Processed {len(gdf)} records for {file_info['year']}-{file_info['month']:02d}")
        
        if not quarter_data:
            logging.warning(f"No data found for quarter {quarter}. This is unusual, as if there is no data, "
                            f"we should never get to this point. Skipping...")
            continue

        quarter_gdf = pd.concat(quarter_data, ignore_index=True)
        
        aggregated = quarter_gdf.groupby('wov_name')['cci'].mean().reset_index()
        
        # Calculate the overall average 'cci' for the entire city
        overall_avg = quarter_gdf['cci'].mean()
        gesamt_row = pd.DataFrame({
            'wov_name': ['gesamtes Stadtgebiet'],
            'cci': [overall_avg],
            'Quartal': [f"{quarter[0]}-{quarter[1]}"]
        })
        
        # Append the 'gesamtes Stadtgebiet' row to the aggregated DataFrame
        aggregated = pd.concat([aggregated, gesamt_row], ignore_index=True)
        
        aggregated['Quartal'] = f"{quarter[0]}-{quarter[1]}"
        results.append(aggregated)
        logging.info(f"Completed aggregation for quarter {quarter}")

    if results:
        # Create a DataFrame with columns "Wohnviertel", "Quartal", "SKI"
        final_df = pd.concat(results, ignore_index=True)
        final_df.columns = ['Wohnviertel', 'SKI', 'Quartal']
        final_df = final_df[['Wohnviertel', 'Quartal', 'SKI']]
        logging.debug(f"Created final DataFrame with {len(final_df)} rows.")
        
        # Write the aggregated DataFrame to a CSV file with the specified header
        output_file = os.path.join(curr_dir, data_agg_path, filename_aggregated_csv)
        final_df.to_csv(output_file, index=False)
        logging.info(f"Wrote aggregated data to {output_file}")
    else:
        logging.warning("No complete quarters found. No output file created.")

    # Delete tmp directory
    delete_tmp_dir(tmp_path=tmp_path)

def aggregate_and_upload_to_ftp(curr_dir):
    logging.debug("Starting aggregate_and_upload_to_ftp.")
    
    filename_aggregated_csv = 'aggregated_ski_by_quarter.csv'
    tmp_dir_name = 'tmp_nt3478ws83w87' # Random name to make it very unlikely that someone else creates the same tmp_dir
    data_agg_dir_name = 'data_agg'
    
    logging.debug(f"Creating aggregated CSV: {filename_aggregated_csv}")
    create_aggregated_csv(
        curr_dir=curr_dir,
        filename_aggregated_csv=filename_aggregated_csv,
        tmp_dir_name=tmp_dir_name,
        data_agg_dir_name=data_agg_dir_name
    )
    logging.debug("Aggregated CSV created successfully.")
    
    aggregated_csv_path = os.path.join(curr_dir, data_agg_dir_name, filename_aggregated_csv)
    logging.debug(f"Aggregated CSV path: {aggregated_csv_path}")
    
    logging.debug("Uploading aggregated CSV to FTP.")
    try:
        common.update_ftp_and_odsp(
            aggregated_csv_path,
            'stadtreinigung/sauberkeitsindex/quartal',
            '100362'
        )
        logging.info("Aggregated CSV uploaded to FTP successfully.")
    except Exception as e:
        logging.error(f"Failed to upload aggregated CSV to FTP: {e}")
        raise

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info(f'Executing {__file__}...')
    main()
    logging.info('Job successful!')
