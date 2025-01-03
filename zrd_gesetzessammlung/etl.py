import logging
import os
import json
import pandas as pd
import requests
import re

import common
from zrd_gesetzessammlung import credentials


def main():
    df_tols = get_texts_of_law()
    path_export = os.path.join(credentials.data_path, 'export', '100354_systematics_with_tols.csv')
    df_tols.to_csv(path_export, index=False)
    common.update_ftp_and_odsp(path_export, 'zrd_gesetzessammlung', '100354')

    df_rc = get_recent_changes(process_all=True)
    path_export = os.path.join(credentials.data_path, 'export', '100355_recent_changes.csv')
    df_rc.to_csv(path_export, index=False)
    common.update_ftp_and_odsp(path_export, 'zrd_gesetzessammlung', '100355')


def get_texts_of_law():
    r = common.requests_get(
        'http://www.lexfind.ch/api/public/entities/6/systematics_with_all_texts_of_law/?active_only=false')
    r.raise_for_status()
    tols = r.json()
    with open(os.path.join(credentials.data_path, 'systematics_with_tols.json'), 'w') as f:
        json.dump(tols, f, indent=2)

    df = pd.DataFrame(tols).T.reset_index().set_index('index')
    df.index = pd.to_numeric(df.index, errors='coerce')
    df['parent'] = pd.to_numeric(df['parent'], errors='coerce')

    for index, row in df.iterrows():
        df.at[index, 'identifier_full'] = get_full_path(df, index, 'identifier')
        df.at[index, 'title_full'] = get_full_path(df, index, 'title')

    # Remove brackets in column children
    df['children'] = df['children'].astype(str).str.replace('[', '').str.replace(']', '')

    # Texts of law are stored as a list of dictionaries in the 'tols' column
    # We need to explode this list of dictionaries into separate rows and columns
    df = df.explode('tols').reset_index()
    df = pd.concat([df.drop(['tols'], axis=1), df['tols'].apply(pd.Series)], axis=1)

    # id is stored as a float, convert to string
    df.loc[df['id'].notna(), 'id'] = df.loc[df['id'].notna(), 'id'].astype(int).astype(str)

    # Get every version for every text of law
    dfs_versions = []
    dfs_tols = []
    for index, row in df[df['id'].notna()].iterrows():
        dfs_versions.append(get_versions(df.at[index, 'id']))
        # last 3 characters are the /de
        base_url = df.at[index, 'original_url_de'].replace('/data/', '/api/de/texts_of_law/')[0:-3]
        dfs_tols.append(get_text_of_law(base_url, df.at[index, 'systematic_number']))
    df_versions = pd.concat(dfs_versions)
    df_tols = pd.concat(dfs_tols)

    # First merge the df_versions with df on the id column
    df = pd.merge(df, df_versions, on='id', how='left', suffixes=('_current', '_version'))
    # Combine the columns of the two dataframes by taking the version value if it exists, otherwise the current value
    for col in df_versions.columns:
        if f"{col}_version" in df.columns:
            df[col] = df[f"{col}_version"].combine_first(df[f"{col}_current"])
            df = df.drop(columns=[f"{col}_current", f"{col}_version"])
    # v_id is stored as a float, convert to string
    df.loc[df['v_id'].notna(), 'v_id'] = df.loc[df['v_id'].notna(), 'v_id'].astype(int).astype(str)

    # Merge the df_tols with df on the systematic_number and version_active_since columns
    df = pd.merge(df, df_tols, on=['systematic_number', 'version_active_since'], how='left')

    # Date columns from %d.%m.%Y to %Y-%m-%d (string)
    date_columns = ['version_active_since', 'family_active_since', 'version_inactive_since', 'version_found_at']
    df = convert_date_columns(df, date_columns)    # Scrap texts of law

    return df


def get_versions(t_id):
    # Convert t_id to integer and then to string
    r = common.requests_get('http://www.lexfind.ch/api/fe/de/texts-of-law/' + t_id + '/with-versions')
    r.raise_for_status()
    versions = r.json()
    df = pd.json_normalize(versions, record_path='versions')
    df = df.explode('dtah_urls').reset_index()
    df = pd.concat([df.drop(['dtah_urls'], axis=1),
                    df['dtah_urls'].apply(pd.Series).add_prefix('tolsv_dtah_')], axis=1)
    df = df.rename(columns={'id': 'v_id'})
    df['id'] = t_id
    df = df.drop(columns=['index'])
    df.columns = df.columns.str.replace('.', '_')
    return df


def get_text_of_law(base_url, systematic_number):
    url = base_url + '/show_as_json'
    r = requests.get(url, proxies=common.credentials.proxies)
    if r.status_code == 404:
        logging.warning(f"JSON with text of law not found for systematic number {systematic_number} and url {url}")
        return pd.DataFrame()
    r.raise_for_status()
    current_json = r.json()
    df = extract_versions_and_dates(current_json, base_url, systematic_number)
    # Extract the HTML content from the json
    for index, row in df.iterrows():
        url_version = df.at[index, 'version_url_de']
        r = requests.get(url_version, proxies=common.credentials.proxies)
        if r.status_code == 404:
            logging.warning(f"Version with text of law not found for systematic number {systematic_number} and url {url_version}")
            df.at[index, 'version_url_de'] = pd.NA
            continue
        r.raise_for_status()
        current_json = r.json()
        if current_json['text_of_law']['selected_version']['json_content']:
            df.at[index, 'text_of_law'] = extract_html_content(current_json['text_of_law']['selected_version']['json_content']['document'])
        else:
            logging.warning(f"HTML content not found for systematic number {systematic_number} and url {url_version}")
            df.at[index, 'version_url_de'] = pd.NA

    df['systematic_number'] = systematic_number
    # Turn the api url into the app url
    df['version_url_de'] = df['version_url_de'].str.replace('api', 'app').str.replace('/show_as_json', '')
    return df


def extract_versions_and_dates(tol_json, base_url, systematic_number):
    """
    Extract urls to tols and activation dates from the given json.
    Returns a pandas DataFrame with columns 'version_url_de' and 'version_active_since'.
    """
    pattern = re.compile(
        r"(?:Aktuelle\s+Version\s+in\s+Kraft\s+seit:|"
        r"Version\s+in\s+Kraft\s+seit:|"
        r"in\s+Kraft\s+seit:)\s*([\d\.]{10})"
    )

    def extract_date(version_dates_str: str) -> str:
        match = pattern.search(version_dates_str)
        return match.group(1) if match else None

    versions = []

    current_ver = tol_json["text_of_law"]["current_version"]
    if current_ver:
        versions.append({
            "version_url_de": f"{base_url}/versions/{current_ver['id']}/show_as_json",
            "version_active_since": extract_date(current_ver["version_dates_str"])
        })

    for old_ver in tol_json["text_of_law"]["old_versions"]:
        versions.append({
            "version_url_de": f"{base_url}/versions/{old_ver['id']}/show_as_json",
            "version_active_since": extract_date(old_ver["version_dates_str"])
        })

    return pd.DataFrame(versions)


# Function to recursively extract HTML content and concatenate it into a single string
def extract_html_content(data):
    html_content = ''
    if isinstance(data, dict):
        for key, value in data.items():
            if key == 'header' or key == 'html_content':
                if isinstance(value, dict):
                    # Getting the 'de' key from the dictionary
                    html_content += value.get('de', '') + '\n'
            else:
                html_content += extract_html_content(value)
    elif isinstance(data, list):
        for item in data:
            html_content += extract_html_content(item)
    return html_content


# Function to recursively get full title and identifier
def get_full_path(df, index, column_name):
    if index == 'nan':
        return ''
    if pd.isna(df.at[index, 'parent']):
        return df.at[index, column_name]
    else:
        return get_full_path(df, df.at[index, 'parent'], column_name) + "/" + df.at[index, column_name]


def get_recent_changes(process_all=False):
    r = common.requests_get('http://www.lexfind.ch/api/fe/de/entities/6/recent-changes')
    r.raise_for_status()
    recent_changes = r.json()
    df = pd.json_normalize(recent_changes, record_path='recent_changes')
    df = process_recent_changes(df)
    common.ods_realtime_push_df(df, credentials.push_url)
    df_recent_changes = df

    while True and process_all:
        domain_suffix = recent_changes['next_batch']
        if domain_suffix is None:
            break
        r = common.requests_get('http://www.lexfind.ch' + domain_suffix)
        r.raise_for_status()
        recent_changes = r.json()
        df = pd.json_normalize(recent_changes, record_path='recent_changes')
        df = process_recent_changes(df)
        common.ods_realtime_push_df(df, credentials.push_url)
        df_recent_changes = pd.concat([df_recent_changes, df])
    return df_recent_changes


def process_recent_changes(df):
    # dta_urls are stored as a list of dictionaries
    df = df.explode('text_of_law.dta_urls').reset_index()
    df = pd.concat([df.drop(['text_of_law.dta_urls'], axis=1),
                    df['text_of_law.dta_urls'].apply(pd.Series).add_prefix('tols_dta_')], axis=1)
    df = df.explode('text_of_law_version.dtah_urls').reset_index()
    df = pd.concat([df.drop(['text_of_law_version.dtah_urls'], axis=1),
                    df['text_of_law_version.dtah_urls'].apply(pd.Series).add_prefix('tolsv_dtah_')], axis=1)
    df.columns = df.columns.str.replace('.', '_')

    date_columns = ['change_date', 'text_of_law_version_info_badge_date',
                    'text_of_law_version_version_active_since', 'text_of_law_version_family_active_since',
                    'text_of_law_version_version_inactive_since', 'text_of_law_version_version_found_at']
    df = convert_date_columns(df, date_columns)
    return df


# Convert date columns from %d.%m.%Y to %Y-%m-%d (string)
def convert_date_columns(df, date_columns):
    df[date_columns] = df[date_columns].apply(lambda x: pd.to_datetime(x, format='%d.%m.%Y', errors='coerce').dt.strftime('%Y-%m-%d'))
    return df


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
    logging.info("Job successful!")
