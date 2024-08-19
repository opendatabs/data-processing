from bs4 import BeautifulSoup
import pandas as pd
import common
import logging
import os
from jfs_gartenbaeder import credentials
from datetime import datetime


def main():
    # URL the website
    url = "https://www.ed-baeder.ch/"

    # Access website
    response = common.requests_get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    # Extract data
    data = []
    for row in soup.find_all('tr'):
        cols = row.find_all('td')
        if len(cols) == 3:
            # Extract name of the bath, temperature and time
            name = cols[0].text.strip()
            temp = cols[1].text.strip()
            time = cols[2].text.strip()

            # Add the data to the list
            data.append([name, temp, time])

    # Creat data Frame
    df_aktuell = pd.DataFrame(data, columns=['Name', 'Temperatur', 'Zeitpunkt'])

    # List of desired swimming pools
    desired_pools = [
        "Bachgraben Sportbad",
        "Bachgraben Familienbad",
        "Hallenbad Eglisee",
        "Eglisee Familienbad",
        "Eglisee Frauenbad",
        "St. Jakob Sportbad",
        "St. Jakob Familienbad"
    ]
    coordinates = {
        "Bachgraben Sportbad": '47.561690, 7.556763',
        "Bachgraben Familienbad": '47.562072, 7.557569',
        "Hallenbad Eglisee": '47.570491, 7.614780',
        "Eglisee Familienbad": '47.570688, 7.613675',
        "Eglisee Frauenbad": '47.570848, 7.615430',
        "St. Jakob Sportbad": '47.539657, 7.620622',
        "St. Jakob Familienbad": '47.538282, 7.620919'
    }
    # Filtering the data frame rows
    df_aktuell = df_aktuell[df_aktuell['Name'].apply(lambda x: any(pool in x for pool in desired_pools))]
    # Map coordinates to names
    df_aktuell['Koordinaten'] = df_aktuell['Name'].map(coordinates)
    # Extract only the numbers from the 'Temperatur' column
    df_aktuell['Temperatur'] = df_aktuell['Temperatur'].str.extract(r'(\d+)').astype(float)
    df_aktuell.loc[7, 'Zeitpunkt'] = df_aktuell.loc[8, 'Zeitpunkt']
    # Apply the function to the 'Zeitpunkt' column
    df_aktuell['Zeitpunkt'] = pd.to_datetime(df_aktuell['Zeitpunkt'].apply(convert_datetime)).dt.tz_localize(
        'Europe/Zurich')
    df_aktuell = df_aktuell.dropna()
    df_aktuell['Zeitpunkt_Job'] = pd.to_datetime(datetime.now()).tz_localize('Europe/Zurich')
    path_export = os.path.join(credentials.path_new, '100388_gartenbaeder_temp_live.csv')
    df_aktuell.to_csv(path_export, index=False)
    common.update_ftp_and_odsp(path_export, '/jfs/gartenbaeder', '100388')

    # Download the whole time series from the FTP server and merge it with the current data
    common.download_ftp(['100384_gartenbaeder_temp_alle.csv'], common.credentials.ftp_server,
                        common.credentials.ftp_user, common.credentials.ftp_pass,
                        '/jfs/gartenbaeder', credentials.path_new, '')
    df = pd.read_csv(os.path.join(credentials.path_new, '100384_gartenbaeder_temp_alle.csv'))
    df['Koordinaten'] = df['Name'].map(coordinates)
    df = pd.concat([df, df_aktuell])
    df = df.drop_duplicates()
    path_export = os.path.join(credentials.path_new, '100384_gartenbaeder_temp_alle.csv')
    df.to_csv(path_export, index=False)
    common.update_ftp_and_odsp(path_export, '/jfs/gartenbaeder', '100384')


def convert_datetime(datum_str):
    try:
        datum = datetime.strptime(datum_str[4:-4], "%d.%m.%Y, %H:%M")
        datum = datum.strftime("%Y-%m-%dT%H:%M:%S")
        return datum
    except ValueError:
        return pd.NaT


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info('Job successful!')
