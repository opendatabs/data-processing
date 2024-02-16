import os
import json
import logging
import pandas as pd
from requests.auth import HTTPBasicAuth

import common
import common.change_tracking as ct
from staatskalender import credentials

# References:
# https://docs.onegovcloud.ch/api/


def main():
    token = get_token()
    args_for_uploads = [get_agencies(token), get_people(token), get_memberships(token)]

    # Upload everything into FTP-Server and update the dataset on data.bs.ch
    for args_for_upload in args_for_uploads:
        common.update_ftp_and_odsp(*args_for_upload)


def get_token():
    res_auth = common.requests_get('https://staatskalender.bs.ch/api/authenticate',
                                   auth=HTTPBasicAuth(credentials.access_key, ''))
    res_auth.raise_for_status()
    return res_auth.json()['token']


def get_agencies(token):
    initial_link = 'https://staatskalender.bs.ch/api/agencies?page=0'
    df = iterate_over_pages(initial_link, token)
    # TODO: Some post-processing if needed
    path_export = os.path.join(credentials.data_path, 'export', '100349_staatskalender_organisationen.csv')
    df.to_csv(path_export, index=False)
    return path_export, 'staka/staatskalender', '100349'


def get_people(token):
    initial_link = 'https://staatskalender.bs.ch/api/people?page=0'
    df = iterate_over_pages(initial_link, token)
    # TODO: Some post-processing if needed
    path_export = os.path.join(credentials.data_path, 'export', '100350_staatskalender_personen.csv')
    df.to_csv(path_export, index=False)
    return path_export, 'staka/staatskalender', '100350'


def get_memberships(token):
    initial_link = 'https://staatskalender.bs.ch/api/memberships?page=0'
    df = iterate_over_pages(initial_link, token)
    # TODO: Some post-processing if needed
    path_export = os.path.join(credentials.data_path, 'export', '100351_staatskalender_mitgliedschaften.csv')
    df.to_csv(path_export, index=False)
    return path_export, 'staka/staatskalender', '100351'


def iterate_over_pages(next_link, token):
    df = pd.DataFrame()
    while True:
        logging.info(f'Getting agencies from {next_link}...')
        r = common.requests_get(next_link,
                                auth=HTTPBasicAuth(token, ''))
        r.raise_for_status()
        agencies = r.json()

        processed_data = []
        for item in agencies["collection"]["items"]:
            row = {"href": item["href"]}

            # Process data section
            for data_point in item["data"]:
                row[data_point["name"]] = data_point["value"]

            # Process links section
            for link in item["links"]:
                row[link["rel"]] = link["href"]

            processed_data.append(row)

        df = pd.concat([df, pd.DataFrame(processed_data)])

        next_link = next((link['href'] for link in agencies["collection"]["links"] if link["rel"] == "next"), None)
        if next_link is None:
            break
    return df


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
    logging.info("Job successful!")
