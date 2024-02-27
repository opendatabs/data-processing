import os
from io import StringIO
import common
from ods_catalog import credentials
import logging


def main():
    # Get the new (published) datasets from ODS
    url_new_datasets = 'https://data.bs.ch/explore/dataset/100055/download/'
    params = {
        'format': 'csv',
        'use_labels_for_header': 'true',
        'refine.visibility': 'domain',
        'refine.publishing_published': 'True'
    }
    headers = {'Authorization': f'apikey {credentials.api_key}'}
    r = common.requests_get(url_new_datasets, params=params, headers=headers)
    r.raise_for_status()
    df = common.pandas_read_csv(StringIO(r.text), sep=';', dtype=str)
    # Push the new datasets to ODS
    path_export = os.path.join(credentials.data_path, '100057_ods_catalog_published.csv')
    df.to_csv(path_export, index=False)
    common.update_ftp_and_odsp(path_export, 'FST-OGD', '100057')


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()