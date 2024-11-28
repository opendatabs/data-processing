import os
import io
import json
import pandas as pd

import common
from common import change_tracking as ct
from mobilitaet_verkehrszaehldaten import credentials


def create_json_files_for_dashboard(df, filename, dest_path):
    """
    Creates JSON files for the dashboard based on the provided DataFrame.

    Parameters:
    - df (pd.DataFrame): The input DataFrame containing the data.
    - filename (str): The name of the file to process.
    - dest_path (str): The destination path where JSON files will be saved.
    """
    # Define categories based on filename
    categories = {
        'MIV_Speed.csv': ['Total', '<20', '20-30', '30-40', '40-50', '50-60', '60-70',
                          '70-80', '80-90', '90-100', '100-110', '110-120', '120-130', '>130'],
        'MIV_Class_10_1.csv': ['Total', 'MR', 'PW', 'PW+', 'Lief', 'Lief+', 'Lief+Aufl.',
                               'LW', 'LW+', 'Sattelzug', 'Bus', 'andere'],
        'Velo_Fuss_Count.csv': ['Total']
    }

    # Create a separate dataset per site and traffic type
    all_sites = df.Zst_id.unique()
    for site in all_sites:
        for traffic_type in ['MIV', 'Velo', 'Fussgänger']:
            site_data = df[df.Zst_id.eq(site) & df.TrafficType.eq(traffic_type)]

            if site_data.empty:
                continue

            # Determine subfolder based on traffic type and filename
            if traffic_type == 'Fussgänger':
                subfolder = 'Fussgaenger'
            elif filename == 'MIV_Speed.csv':
                subfolder = 'MIV_Speed'
            else:
                subfolder = traffic_type

            # Save the original site data
            current_filename = os.path.join(dest_path, 'sites', subfolder, f'{str(site)}.csv')
            print(f'Saving {current_filename}...')
            site_data.to_csv(current_filename, sep=';', encoding='utf-8', index=False)

            if True or ct.has_changed(current_filename):
                common.upload_ftp(current_filename, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass,
                                  f'verkehrszaehl_dashboard/data/{subfolder}')

                # Add Direction_LaneName column
                site_data['Direction_LaneName'] = (
                        site_data['DirectionName'].astype(str) + '#' + site_data['LaneName'].astype(str)
                )
                # Convert Date to string format like '2022-01-01'
                site_data['Date'] = pd.to_datetime(site_data['Date'], format='%d.%m.%Y').dt.strftime('%Y-%m-%d')

                # Perform aggregations
                aggregate_hourly(site_data, categories, dest_path, subfolder, site, filename)
                aggregate_daily(site_data, categories, dest_path, subfolder, site, filename)
                aggregate_time_period(site_data, categories, dest_path, subfolder, site, filename, period='Month')
                aggregate_time_period(site_data, categories, dest_path, subfolder, site, filename, period='Year')

                ct.update_hash_file(current_filename)

    # Calculate DTV per ZST and traffic type
    df_locations = download_locations()
    if 'MIV' in filename:
        df_dtv = calculate_dtv_zst_miv(df, df_locations, filename)
        if 'MIV_Speed' in filename:
            current_filename = os.path.join(dest_path, 'dtv_MIV_Speed.json')
        else:
            current_filename = os.path.join(dest_path, 'dtv_MIV.json')
        print(f'Saving {current_filename}...')
        save_as_list_of_lists(df_dtv, current_filename)
        common.upload_ftp(current_filename, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass,
                          f'verkehrszaehl_dashboard/data')
        os.remove(current_filename)
    else:
        df_dtv_velo, df_dtv_fuss = calculate_dtv_zst_velo_fuss(df, df_locations)
        current_filename_velo = os.path.join(dest_path, 'dtv_Velo.json')
        print(f'Saving {current_filename_velo}...')
        save_as_list_of_lists(df_dtv_velo, current_filename_velo)
        common.upload_ftp(current_filename_velo, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass,
                          f'verkehrszaehl_dashboard/data')
        os.remove(current_filename_velo)
        current_filename_fuss = os.path.join(dest_path, 'dtv_Fussgaenger.json')
        print(f'Saving {current_filename_fuss}...')
        save_as_list_of_lists(df_dtv_fuss, current_filename_fuss)
        common.upload_ftp(current_filename_fuss, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass,
                          f'verkehrszaehl_dashboard/data')
        os.remove(current_filename_fuss)


def aggregate_hourly(site_data, categories, dest_path, subfolder, site, filename):
    """
    Performs hourly aggregation and saves the data.

    Parameters:
    - site_data (pd.DataFrame): DataFrame containing site data.
    - categories (dict): Dictionary mapping filenames to category lists.
    - dest_path (str): Destination path for saving files.
    - subfolder (str): Subfolder name.
    - site (str/int): Site identifier.
    - filename (str): Name of the file being processed.
    """
    df_to_merge = site_data[['Date', 'Weekday']].drop_duplicates()

    for category in categories[filename]:
        # Calculate the total counts per hour for each date, direction, and lane
        df_to_pivot = site_data[['Date', 'Direction_LaneName', 'HourFrom', category]].copy()
        df_agg = df_to_pivot.pivot_table(
            index=['Date', 'Direction_LaneName'],
            values=category,
            columns='HourFrom',
            aggfunc='sum'
        ).reset_index()
        df_agg = df_agg.merge(df_to_merge, on='Date')
        df_agg[['DirectionName', 'LaneName']] = df_agg['Direction_LaneName'].str.split('#', expand=True)
        df_agg = df_agg.drop(columns=['Direction_LaneName'])

        # Save the hourly data
        current_filename_hourly = os.path.join(dest_path, 'sites', subfolder, f'{str(site)}_{category}_hourly.csv')
        print(f'Saving {current_filename_hourly}...')
        df_agg.to_csv(current_filename_hourly, sep=';', encoding='utf-8', index=False)
        common.upload_ftp(current_filename_hourly, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass,
                          f'verkehrszaehl_dashboard/data/{subfolder}')
        os.remove(current_filename_hourly)


def aggregate_daily(site_data, categories, dest_path, subfolder, site, filename):
    """
    Performs daily aggregation and saves the data.

    Parameters:
    - site_data (pd.DataFrame): DataFrame containing site data.
    - categories (dict): Dictionary mapping filenames to category lists.
    - dest_path (str): Destination path for saving files.
    - subfolder (str): Subfolder name.
    - site (str/int): Site identifier.
    - filename (str): Name of the file being processed.
    """
    # Calculate the daily counts per weekday for each date, direction, and lane
    df_to_group = site_data[['Date', 'Direction_LaneName'] + categories[filename]].copy()
    df_to_merge = site_data[['Date', 'Week', 'Weekday', 'Year']].drop_duplicates()
    df_agg = df_to_group.groupby(['Date', 'Direction_LaneName'])[categories[filename]].sum().reset_index()
    df_agg = df_agg[df_agg['Total'] > 0]
    df_agg = df_agg.merge(df_to_merge, on='Date')
    df_agg[['DirectionName', 'LaneName']] = df_agg['Direction_LaneName'].str.split('#', expand=True)
    df_agg = df_agg.drop(columns=['Direction_LaneName'])

    # Save the daily data
    current_filename_daily = os.path.join(dest_path, 'sites', subfolder, f'{str(site)}_daily.csv')
    print(f'Saving {current_filename_daily}...')
    df_agg.to_csv(current_filename_daily, sep=';', encoding='utf-8', index=False)
    common.upload_ftp(current_filename_daily, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass,
                      f'verkehrszaehl_dashboard/data/{subfolder}')
    os.remove(current_filename_daily)


def aggregate_time_period(site_data, categories, dest_path, subfolder, site, filename, period):
    """
    Aggregates data over a specified time period ('Month' or 'Year').

    Parameters:
    - site_data (pd.DataFrame): DataFrame containing site data.
    - categories (dict): Dictionary mapping filenames to category lists.
    - dest_path (str): Destination path for saving files.
    - subfolder (str): Subfolder name.
    - site (str/int): Site identifier.
    - filename (str): Name of the file being processed.
    - period (str): 'Month' or 'Year' indicating the aggregation period.
    """
    if period == 'Month':
        group_cols = ['Year', 'Month', 'Direction_LaneName']
        file_suffix = 'monthly'
    elif period == 'Year':
        group_cols = ['Year', 'Direction_LaneName']
        file_suffix = 'yearly'
    else:
        raise ValueError("Period must be 'Month' or 'Year'")

    df_to_group = site_data[group_cols + ['DateTimeFrom'] + categories[filename]].copy()
    df_agg = df_to_group.groupby(group_cols)[categories[filename]].sum().reset_index()
    df_agg = df_agg[df_agg['Total'] > 0]

    df_measures = df_to_group.groupby(group_cols)['DateTimeFrom'].nunique().reset_index()
    df_measures.rename(columns={'DateTimeFrom': 'NumMeasures'}, inplace=True)
    df_measures = df_measures[df_measures['NumMeasures'] > 0]
    df_agg = df_agg.merge(df_measures, on=group_cols)
    for col in categories[filename]:
        df_agg[col] = df_agg[col] / df_agg['NumMeasures'] * 24
    df_agg[['DirectionName', 'LaneName']] = df_agg['Direction_LaneName'].str.split('#', expand=True)
    df_agg = df_agg.drop(columns=['Direction_LaneName'])

    # Save the aggregated data
    current_filename = os.path.join(dest_path, 'sites', subfolder, f'{str(site)}_{file_suffix}.csv')
    print(f'Saving {current_filename}...')
    df_agg.to_csv(current_filename, sep=';', encoding='utf-8', index=False)
    common.upload_ftp(current_filename, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass,
                      f'verkehrszaehl_dashboard/data/{subfolder}')
    os.remove(current_filename)


def download_locations():
    """
    Downloads location data and returns a DataFrame with the locations.

    Returns:
    - pd.DataFrame: DataFrame containing location data.
    """
    url_to_locations = 'https://data.bs.ch/explore/dataset/100038/download/'
    params = {
        'format': 'csv',
        'timezone': 'Europe/Zurich',
        'klasse': 'Dauerzaehlstelle'
    }
    r = common.requests_get(url_to_locations, params=params)
    df_locations = pd.read_csv(io.StringIO(r.text), sep=';', encoding='utf-8')
    # Expand 'zweck' to several lines if there is a '+'
    df_locations['zweck'] = df_locations['zweck'].str.split('+')
    df_locations = df_locations.explode('zweck')
    # Replace 'Velo/Moto' with 'Velo' and 'Fuss' with 'Fussgänger'
    df_locations['zweck'] = df_locations['zweck'].str.replace('Velo/Moto', 'Velo')
    df_locations['zweck'] = df_locations['zweck'].str.replace('Fuss', 'Fussgänger')
    # Save 'id_zst' as string
    df_locations['id_zst'] = df_locations['id_zst'].astype(str)
    return df_locations[['id_zst', 'zweck', 'geo_point_2d', 'name', 'gemeinde', 'klasse', 'kombiniert',
                         'art', 'arme', 'fahrstreif', 'typ', 'strtyp', 'betriebnah', 'betriebzus']]


def calculate_dtv_zst_miv(df, df_locations, filename):
    """
    Calculates DTV per ZST for MIV data.

    Parameters:
    - df (pd.DataFrame): The input DataFrame containing the data.
    - df_locations (pd.DataFrame): DataFrame containing location data.
    - dest_path (str): The destination path where files will be saved.
    - filename (str): The name of the file to process.

    Returns:
    - pd.DataFrame: DataFrame containing DTV data.
    """
    aggregation_dict = {
        'MIV_Speed.csv': ['Total', '<20', '20-30', '30-40', '40-50', '50-60', '60-70',
                          '70-80', '80-90', '90-100', '100-110', '110-120', '120-130', '>130'],
        'MIV_Class_10_1.csv': ['Total', 'MR', 'PW', 'PW+', 'Lief', 'Lief+', 'Lief+Aufl.',
                               'LW', 'LW+', 'Sattelzug', 'Bus', 'andere']
    }
    if filename in aggregation_dict:
        columns = aggregation_dict[filename]
        df_tv = df.groupby(['Zst_id', 'Date', 'TrafficType'])[columns].sum().reset_index()
        # Remove rows with Total = 0
        df_tv = df_tv[df_tv['Total'] > 0]
        df_dtv = df_tv.groupby(['Zst_id', 'TrafficType'])[columns].mean().reset_index()

        df_count = df.groupby(['Zst_id', 'TrafficType'])['DateTimeFrom'].count().reset_index()
        df_count = df_count[df_count['DateTimeFrom'] > 0]
        df_dtv = df_dtv.merge(df_count, on=['Zst_id', 'TrafficType'], how='left')
        df_dtv.rename(columns={'DateTimeFrom': 'NumMeasures'}, inplace=True)
        # Merge with locations
        df_dtv = df_dtv.merge(df_locations, left_on=['Zst_id', 'TrafficType'],
                              right_on=['id_zst', 'zweck'], how='left').drop(columns=['id_zst', 'zweck'])
        return df_dtv


def calculate_dtv_zst_velo_fuss(df, df_locations):
    """
    Calculates DTV per ZST for Velo and Fussgänger data.

    Parameters:
    - df (pd.DataFrame): The input DataFrame containing the data.
    - df_locations (pd.DataFrame): DataFrame containing location data.
    - dest_path (str): The destination path where files will be saved.
    - filename (str): The name of the file to process.

    Returns:
    - tuple: A tuple containing two DataFrames for Velo and Fussgänger data.
    """
    df_tv = df.groupby(['Zst_id', 'Date', 'TrafficType'])['Total'].sum().reset_index()
    # Remove rows with Total = 0
    df_tv = df_tv[df_tv['Total'] > 0]
    df_dtv = df_tv.groupby(['Zst_id', 'TrafficType'])['Total'].mean().reset_index()
    # Remove rows with NaN values
    df_dtv = df_dtv.dropna()

    df_count = df.groupby(['Zst_id', 'TrafficType'])['DateTimeFrom'].count().reset_index()
    df_count = df_count[df_count['DateTimeFrom'] > 0]
    df_dtv = df_dtv.merge(df_count, on=['Zst_id', 'TrafficType'], how='left')
    df_dtv.rename(columns={'DateTimeFrom': 'NumMeasures'}, inplace=True)
    # Merge with locations
    df_dtv = df_dtv.merge(df_locations, left_on=['Zst_id', 'TrafficType'],
                          right_on=['id_zst', 'zweck'], how='left').drop(columns=['id_zst', 'zweck'])

    df_dtv_velo = df_dtv[df_dtv['TrafficType'] == 'Velo']
    df_dtv_fuss = df_dtv[df_dtv['TrafficType'] == 'Fussgänger']
    df_dtv_fuss['TrafficType'] = 'Fussgaenger'  # Correcting the TrafficType for consistency
    return df_dtv_velo, df_dtv_fuss


def save_as_list_of_lists(df, filename):
    """
    Saves a DataFrame as a JSON file in a list-of-lists format.

    Parameters:
    - df (pd.DataFrame): The DataFrame to save.
    - filename (str): The output JSON file path.
    """
    # Convert datetime columns to string format
    df = df.copy()
    for col in df.select_dtypes(include=['datetime64[ns]', 'datetime64', 'datetimetz']):
        df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S') 

    # Convert DataFrame to list of lists
    data_as_list = [df.columns.tolist()] + df.to_numpy().tolist()

    # Save as JSON
    with open(filename, 'w', encoding='utf-8') as json_file:
        json.dump(data_as_list, json_file, ensure_ascii=False, indent=4)
    print(f"Saved {filename} in list-of-lists format.")

    
