import logging
import os
import json
import pandas as pd

import common
from zri_gesetzessammlung import credentials


def main():
    df_tols = get_texts_of_law()
    path_export = os.path.join(credentials.data_path, '100354_systematics_with_tols.csv')
    df_tols.to_csv(path_export, index=False)
    common.update_ftp_and_odsp(path_export, 'zrd_gesetzessammlung', '100354')

    get_recent_changes(process_all=True)



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

    # Drop columns that are redundant since they contain no values

    df = df.drop(columns=[0, 'version_inactive_since'])
    # Remove brackets in column children
    df['children'] = df['children'].astype(str).str.replace('[', '').str.replace(']', '')
    return df


# Function to recursively get full title and identifier
def get_full_path(df, index, column_name):
    if index == 'nan':
        return ''
    if pd.isna(df.at[index, 'parent']):
        return df.at[index, column_name]
    else:
        return get_full_path(df, df.at[index, 'parent'], column_name) + "/" + df.at[index, column_name]


def get_recent_changes(process_all=False):
    r = common.requests_get('http://lexfind/api/fe/de/entities/6/recent-changes')
    r.raise_for_status()
    recent_changes = r.json()
    df_rc = pd.json_normalize(recent_changes, record_path='recent_changes')
    common.ods_realtime_push_df(df_rc, credentials.push_url)

    while True and process_all:
        domain_suffix = recent_changes['next_batch']
        r = common.requests_get('http://lexfind' + domain_suffix)
        r.raise_for_status()
        recent_changes = r.json()
        df_rc = pd.json_normalize(recent_changes, record_path='recent_changes')
        common.ods_realtime_push_df(df_rc, credentials.push_url)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
    logging.info("Job successful!")
