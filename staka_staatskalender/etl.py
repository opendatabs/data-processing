import os
import re
import logging
import pandas as pd
from requests.auth import HTTPBasicAuth

import common
import common.change_tracking as ct
from staka_staatskalender import credentials

# References:
# https://docs.onegovcloud.ch/api/api#agencies-view


def main():
    token = get_token()
    args_for_uploads = [get_agencies(token), get_memberships(token)]

    # Upload everything into FTP-Server and update the dataset on data.bs.ch
    for args_for_upload in args_for_uploads:
        common.update_ftp_and_odsp(*args_for_upload)


def get_token():
    res_auth = common.requests_get('https://staatskalender.bs.ch/api/authenticate',
                                   auth=HTTPBasicAuth(credentials.access_key, ''))
    res_auth.raise_for_status()
    return res_auth.json()['token']


def get_agencies(token):
    # Extract
    initial_link = 'https://staatskalender.bs.ch/api/agencies?page=0'
    df = iterate_over_pages(initial_link, token)
    # Transform
    df['id'] = df['href'].str.extract(r'(\d+)$')
    df['parent_id'] = df['parent'].str.extract(r'(\d+)$')
    df = df.set_index('id')
    df.index = pd.to_numeric(df.index, errors='coerce')
    df['parent_id'] = pd.to_numeric(df['parent_id'], errors='coerce')
    # Create new column children_id with an empty list
    df['children_id'] = df.apply(lambda x: [], axis=1)
    for index, row in df.iterrows():
        df.at[index, 'title_full'] = get_full_path(df, index, 'title')
        if not pd.isna(df.at[index, 'parent']) and df.at[index, 'parent_id'] in df.index:
            df.at[df.at[index, 'parent_id'], 'children_id'].append(index)

    df['children_id'] = df['children_id'].astype(str).str.replace('[', '').str.replace(']', '')
    df['parent_id'] = df['parent_id'].astype(str).str.replace('.0', '')
    # Create urls to Staatskalender
    df['url_website'] = df['href'].str.replace('/api/agencies/', '/organizations?browse=')
    df['filter_by_parent'] = f"https://data.bs.ch/explore/dataset/100349?refine.id={df['parent_id']}"
    df['filtren_by_children'] = f"https://data.bs.ch/explore/dataset/100349?refine.id={df['children_id'].replace(', ', '&refine.id=')}"
    df['geo_location'] = df['geo_location'].str.replace("{'lon': None, 'lat': None, 'zoom': None}", '')
    # Replace , 'zoom':(anything) with }
    df['geo_location'] = df['geo_location'].str.replace(r", 'zoom':.*?}", '}')
    path_export = os.path.join(credentials.data_path, 'export', '100349_staatskalender_organisationen.csv')
    df.to_csv(path_export, index=True)
    return path_export, 'staka/staatskalender', '100349'


# Function to recursively get full title and identifier
def get_full_path(df, index, column_name):
    if index not in df.index:
        return index.astype(str)
    if pd.isna(df.at[index, 'parent']):
        return df.at[index, column_name]
    else:
        return get_full_path(df, df.at[index, 'parent_id'], column_name) + "/" + df.at[index, column_name]


def get_children_id(children_url, token):
    processed_data, _ = from_coll_json_to_dict(children_url, token)
    if len(processed_data) == 0:
        return ''
    # processed data is a list of dictionaries, we need to extract one element and return it as a list
    return str([int(re.search(r'(\d+)$', item['href']).group()) for item in processed_data])


def get_people(token):
    initial_link = 'https://staatskalender.bs.ch/api/people?page=0'
    df = iterate_over_pages(initial_link, token)
    path_export = os.path.join(credentials.data_path, 'export', '100350_staatskalender_personen.csv')
    df.to_csv(path_export, index=False)
    return path_export, 'staka/staatskalender', '100350'


def get_memberships(token):
    initial_link = 'https://staatskalender.bs.ch/api/memberships?page=0'
    df = iterate_over_pages(initial_link, token)
    path_agencies = os.path.join(credentials.data_path, 'export', '100349_staatskalender_organisationen.csv')
    df_agencies = pd.read_csv(path_agencies)
    df = df.merge(df_agencies[['href', 'id', 'title', 'title_full']], left_on='agency', right_on='href', suffixes=('', '_org'))
    df['url_memb_website'] = df['href'].str.replace('/api/memberships/', '/membership/')
    df['url_pers_website'] = df['person'].str.replace('/api/people/', '/person/')
    path_export = os.path.join(credentials.data_path, 'export', '100351_staatskalender_mitgliedschaften.csv')
    df.to_csv(path_export, index=False)
    return path_export, 'staka/staatskalender', '100351'


def iterate_over_pages(next_link, token):
    df = pd.DataFrame()
    while True:
        logging.info(f'Getting data from {next_link}...')
        processed_data, json = from_coll_json_to_dict(next_link, token)

        df = pd.concat([df, pd.DataFrame(processed_data)])

        next_link = next((link['href'] for link in json["collection"]["links"] if link["rel"] == "next"), None)
        if next_link is None:
            break
    return df


def from_coll_json_to_dict(url, token):
    r = common.requests_get(url, auth=HTTPBasicAuth(token, ''))
    r.raise_for_status()
    json = r.json()

    processed_data = []
    for item in json["collection"]["items"]:
        row = {"href": item["href"]}

        # Process data section
        for data_point in item["data"]:
            row[data_point["name"]] = data_point["value"]

        # Process links section
        for link in item["links"]:
            row[link["rel"]] = link["href"]

        processed_data.append(row)

    return processed_data, json


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
    logging.info("Job successful!")
