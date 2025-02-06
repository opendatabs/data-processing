import os
import logging
import pandas as pd

import common
from ed_swisslos_sportfonds import credentials


def main():
    df_all = pd.DataFrame()
    for filename in os.listdir(credentials.data_orig_path):
        if not filename.endswith('.xlsx'):
            logging.info(f"Ignoring {filename}; Not an excel file")
            continue
        excel_file_path = os.path.join(credentials.data_orig_path, filename)
        df = pd.read_excel(excel_file_path, skiprows=7, usecols='A,D,E', header=None,
                              names=['beguenstigte', 'unterstuetztes_projekt', 'beitrag'])

        # Drop rows where values are missing
        df = df[pd.notna(df['unterstuetztes_projekt'])]

        # extract string 'Jahr 2021' and then extract the year
        df['jahr'] = pd.read_excel(excel_file_path, usecols='D', skiprows=2, header=None).iloc[0, 0].split(' ')[1]

        df.reset_index(drop=True, inplace=True)
        df_all = pd.concat([df_all, df])

    path_export = os.path.join(credentials.data_path, 'export', '100221_swisslos_sportfonds.csv')
    df_all.to_csv(path_export)
    common.update_ftp_and_odsp(path_export, 'ed/swisslos_sportfonds', '100221')


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info(f'Job successfully completed!')