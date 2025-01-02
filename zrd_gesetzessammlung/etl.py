import logging
import os
import json
import pandas as pd

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
    r = common.requests_get('http://lexfind/api/public/entities/6/systematics_with_all_texts_of_law')
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

    # Texts of law are stored as a list of dictionaries in the 'tols' column
    # We need to explode this list of dictionaries into separate rows and columns
    df = df.explode('tols').reset_index()
    df = pd.concat([df.drop(['tols'], axis=1), df['tols'].apply(pd.Series)], axis=1)

    # Scrap texts of law
    for index, row in df[df['original_url_de'].notna()].iterrows():
        df.at[index, 'text_of_law'] = get_text_of_law(df.at[index, 'original_url_de'])

    # Drop columns that are redundant since they contain no values
    df = df.drop(columns=[0, 'version_inactive_since'])
    # Remove brackets in column children
    df['children'] = df['children'].astype(str).str.replace('[', '').str.replace(']', '')
    return df


def get_text_of_law(url):
    url = url.replace('/de', '/show_as_json').replace('/data/', '/api/de/texts_of_law/')
    r = common.requests_get(url)
    r.raise_for_status()
    text_of_law = r.json()
    if text_of_law['text_of_law']['selected_version']['json_content']:
        return extract_html_content(text_of_law['text_of_law']['selected_version']['json_content']['document'])
    else:
        return ''


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
    # Date columns from %d.%m.%Y to %Y-%m-%d (string)
    date_columns = ['change_date', 'text_of_law_version_info_badge_date',
                    'text_of_law_version_version_active_since', 'text_of_law_version_family_active_since',
                    'text_of_law_version_version_inactive_since', 'text_of_law_version_version_found_at']
    df[date_columns] = df[date_columns].apply(lambda x: pd.to_datetime(x, format='%d.%m.%Y').dt.strftime('%Y-%m-%d'))
    return df


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
    logging.info("Job successful!")
