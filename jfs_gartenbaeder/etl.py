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
    df = pd.DataFrame(data, columns=['Name', 'Temperatur', 'Zeitpunkt'])

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

    # Filtering the data frame rows
    df = df[df['Name'].apply(lambda x: any(pool in x for pool in desired_pools))]
    # Extract only the numbers from the 'Temperatur' column
    df['Temperatur'] = df['Temperatur'].str.extract(r'(\d+)').astype(int)
    df.loc[7,'Zeitpunkt'] = df.loc[8,'Zeitpunkt'] 
    # Apply the function to the 'Zeitpunkt' column
    df['Zeitpunkt'] = df['Zeitpunkt'].apply(convert_datetime)
    path_export = os.path.join(credentials.path_new, '100384_gartenbaeder_tempe.csv')
    df.to_csv(path_export, index=False)
    common.update_ftp_and_odsp(path_export, '/jfs/gartenbaeder', '100384')


def convert_datetime(datum_str):
    try:
        datum = datetime.strptime(datum_str, "%a. %d.%m.%Y, %H:%M Uhr")
        datum = datum.strftime("%Y-%m-%d T%H:%M:%S")
        return datum
    except ValueError:
        # Return the original value if it cannot be converted
        return datum_str

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info('Job successful!')