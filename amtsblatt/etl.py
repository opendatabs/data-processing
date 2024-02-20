import os
import io
import logging
import pandas as pd
import datetime
import xml.etree.ElementTree as ET

import common
from amtsblatt import credentials


# References:
# https://www.amtsblattportal.ch/docs/api/


def main():
    df = iterate_over_years()
    df = add_columns(df)
    path_export = os.path.join(credentials.data_path, 'export', '100352_amtsblatt.csv')
    df.to_csv(path_export, index=False)
    common.update_ftp_and_odsp(path_export, 'amtsblatt', '100352')


def iterate_over_years():
    start_year = 2019
    df = pd.DataFrame()
    for year in range(start_year, datetime.datetime.now().year + 1):
        df_year = iterate_over_pages(year)
        df = pd.concat([df, df_year])
    return df


def iterate_over_pages(year):
    base_url = f'https://kantonsblatt.ch/api/v1/publications/csv?publicationStates=PUBLISHED&cantons=BS&publicationDate.start={year}-01-01&publicationDate.end={year}-12-31'
    page = 0
    next_page = f'{base_url}&pageRequest.page={page}'
    df = pd.DataFrame()
    while True:
        logging.info(f'Getting data from {next_page}...')
        r = common.requests_get(next_page)
        r.raise_for_status()
        df_curr_page = pd.read_csv(io.StringIO(r.content.decode('utf-8')), sep=';')
        if df_curr_page.empty:
            break
        df = pd.concat([df, df_curr_page])
        page = page + 1
        # TODO: Also get entries after the 100th page
        if page == 100:
            break
        next_page = f'{base_url}&pageRequest.page={page}'
    return df


def add_columns(df):
    df['url_kantonsblatt'] = df['id'].apply(lambda x: f'https://www.kantonsblatt.ch/#!/search/publications/detail/{x}')
    df['url_pdf'] = df['id'].apply(lambda x: f'https://www.kantonsblatt.ch/api/v1/publications/{x}/pdf')
    df['url_xml'] = df['id'].apply(lambda x: f'https://www.kantonsblatt.ch/api/v1/publications/{x}/xml')
    df['content'] = ''
    df['attachments'] = ''
    for index, row in df.iterrows():
        xml_content = get_content_from_xml(row['url_xml'])
        root = ET.fromstring(xml_content)
        content = root.find('content')
        attach = root.find('attachments')
        df.at[index, 'content'] = ET.tostring(content, encoding='utf-8') if content is not None else ''
        df.at[index, 'attachments'] = ET.tostring(attach, encoding='utf-8') if attach is not None else ''
    return df


def get_content_from_xml(url):
    r = common.requests_get(url)
    r.raise_for_status()
    return r.text


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
    logging.info("Job successful!")
