import os
import logging
import pandas as pd

from jsd_swisslos import credentials

def main():
    file_name = os.path.join(credentials.data_orig_path, '12 Beitraege Swisslos-Fonds BS 2023_Dezember.xls')
    df = pd.read_excel(file_name, skiprows=7, usecols='A:C', header=None, engine='xlrd',
                          names=['beguenstigte', 'unterstuetztes_projekt', 'beitrag'])
    df['bereich'] = ''

    current_bereich = ''

    for index, row in df.iterrows():
        if pd.isna(row['unterstuetztes_projekt']):
            current_bereich = row['beguenstigte']
        else:
            df.at[index, 'bereich'] = current_bereich

    df = df[pd.notna(df['unterstuetztes_projekt']) & pd.notna(df['bereich'])]

    df.reset_index(drop=True, inplace=True)

    path_export = os.path.join(credentials.data_path, 'export', '1003xx_swisslos_fonds.csv')
    df.to_excel(path_export, index=False)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info(f'Job successfully completed!')