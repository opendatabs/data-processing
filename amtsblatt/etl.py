import os
import logging
import pandas as pd

import common
from amtsblatt import credentials

# References:
# https://www.amtsblattportal.ch/docs/api/


def main():
    df = iterate_over_pages()
    print(df.head())
    path_export = os.path.join(credentials.data_path, 'export', '100352_amtsblatt.csv')
    df.to_csv(path_export, index=False)
    common.update_ftp_and_odsp(path_export, 'amtsblatt', '100352')


def iterate_over_pages():
    base_url = 'https://kantonsblatt.ch/api/v1/publications/csv?publicationStates=PUBLISHED&cantons=BS'
    page = 0
    next_page = f'{base_url}&pageRequest.page={page}'
    df = pd.DataFrame()
    while True:
        logging.info(f'Getting data from {next_page}...')
        r = common.requests_get(next_page)
        r.raise_for_status()
        curr_page_path = os.path.join(credentials.data_path, f'curr_page.csv')
        with open(curr_page_path, 'wb') as f:
            f.write(r.content)
        df_curr_page = pd.read_csv(curr_page_path, sep=';')
        if df_curr_page.empty:
            break
        df = pd.concat([df, df_curr_page])
        page = page + 1
        # Just found out that 100 does not work, so we stop at 99 for now
        if page == 100:
            break
        next_page = f'{base_url}&pageRequest.page={page}'
    return df


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
    logging.info("Job successful!")
