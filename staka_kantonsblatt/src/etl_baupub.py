import os
import io
import logging
import pandas as pd
import xml.etree.ElementTree as ET
import requests

import common
from staka_kantonsblatt import credentials


# References:
# https://www.amtsblattportal.ch/docs/api/


def main():
    df = get_urls()
    all_data = []

    for index, row in df.iterrows():
        df_content = add_content_to_row(row)
        all_data.append(df_content)

    final_df = pd.concat(all_data, ignore_index=True)  # Concatenate all dataframes
    path_export = os.path.join(credentials.data_path, 'export', '100366_kantonsblatt_bauplikationen.csv')
    final_df.to_csv(path_export, index=False)
    common.update_ftp_and_odsp(path_export, 'staka/kantonsblatt', '100366')


def get_urls():
    # Get urls from metadata already published as a dataset
    url_kantonsblatt_ods = 'https://data.bs.ch/explore/dataset/100352/download/'
    params = {
        'format': 'csv',
        'refine.subrubric': 'BP-BS10'
    }
    headers = {'Authorization': f'apikey {credentials.api_key}'}
    r = common.requests_get(url_kantonsblatt_ods, params=params, headers=headers)
    r.raise_for_status()
    # Save into a list
    return pd.read_csv(io.StringIO(r.content.decode('utf-8')), sep=';')[['id', 'url_xml']]


def add_content_to_row(row):
    content, _ = get_content_from_xml(row['url_xml'])
    df_content = xml_to_dataframe(content)
    row['content'] = ET.tostring(content, encoding='utf-8')
    for col in row.index:
        if col in df_content.columns:
            # Combine existing DataFrame column with value from row, if it exists
            df_content[col] = df_content[col].combine_first(pd.Series([row[col]] * len(df_content)))
        else:
            # Create the column in df_content if it does not exist and fill with the value from the row
            df_content[col] = pd.Series([row[col]] * len(df_content))
    return df_content


def get_content_from_xml(url):
    try:
        r = common.requests_get(url)
        r.raise_for_status()
        xml_content = r.text
        root = ET.fromstring(xml_content)
        content = root.find('content')
        attachments = root.find('attachments')
    except requests.exceptions.HTTPError as err:
        logging.error(f"HTTP error occurred: {err}")
        return None, None
    return content, attachments


def xml_to_dataframe(root):
    def traverse(node, path='', path_dict=None):
        if path_dict is None:
            path_dict = {}

        if list(node):  # If the node has children
            for child in node:
                child_path = f'{path}_{child.tag}' if path else child.tag
                traverse(child, child_path, path_dict)
        else:  # If the node is a leaf
            value = node.text.strip() if node.text and node.text.strip() else ''
            if path in path_dict:
                path_dict[path].append(value)
            else:
                path_dict[path] = [value]

        return path_dict

    path_dict = traverse(root)

    # Find the maximum length of any list in the dictionary to standardize the DataFrame size
    max_len = max(len(v) for v in path_dict.values())  # Find the longest list

    # Expand all lists to this maximum length
    expanded_data = {k: v * max_len if len(v) == 1 else v + [''] * (max_len - len(v)) for k, v in path_dict.items()}

    df = pd.DataFrame(expanded_data)
    return df


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
    logging.info("Job successful!")
