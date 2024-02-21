import logging
import os
import json
import pandas as pd

import common
from zri_gesetzessammlung import credentials


def main():
    df_tols = get_texts_of_law()
    df_tols.to_csv(os.path.join(credentials.data_path, 'systematics_with_tols.csv'), index=False)
    df_sys = get_systematics()
    df_sys.to_csv(os.path.join(credentials.data_path, 'systematics_BS.csv'), index=False)
    df_rc = get_recent_changes()
    df_rc.to_csv(os.path.join(credentials.data_path, 'recent_changes_BS.csv'), index=False)


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

    return df


def get_systematics():
    r = common.requests_get('http://lexfind/api/fe/de/entities/6/systematics')
    r.raise_for_status()
    systematics = r.json()
    with open(os.path.join(credentials.data_path, 'systematics_BS.json'), 'w') as f:
        json.dump(systematics, f, indent=2)

    df = pd.DataFrame(systematics).T.reset_index().set_index('index')
    df.index = pd.to_numeric(df.index, errors='coerce')
    df['parent'] = pd.to_numeric(df['parent'], errors='coerce')

    for index, row in df.iterrows():
        df.at[index, 'identifier_full'] = get_full_path(df, index, 'identifier')
        df.at[index, 'title_full'] = get_full_path(df, index, 'title')

    return df


# Function to recursively get full title and identifier
def get_full_path(df, index, column_name):
    if index == 'nan':
        return ''
    if pd.isna(df.at[index, 'parent']):
        return df.at[index, column_name]
    else:
        return get_full_path(df, df.at[index, 'parent'], column_name) + "/" + df.at[index, column_name]


def get_recent_changes():
    r = common.requests_get('http://lexfind/api/fe/de/entities/6/recent-changes')
    r.raise_for_status()
    recent_changes = r.json()
    df_rc = pd.json_normalize(recent_changes, record_path='recent_changes')

    for _ in range(3):
        domain_suffix = recent_changes['next_batch']
        r = common.requests_get('http://lexfind' + domain_suffix)
        r.raise_for_status()
        recent_changes = r.json()
        df_rc = pd.concat((df_rc, pd.json_normalize(recent_changes, record_path='recent_changes')))

    return df_rc


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
    logging.info("Job successful!")
