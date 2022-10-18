import logging
import pandas as pd
import os
import pathlib


def main():
    data_file = os.path.join(pathlib.Path(__file__).parent, 'data_orig', 'Stadtlast_Update_PD.xlsx')
    df = pd.read_excel(data_file)
    df['timestamp_2021'] = df['Ab-Datum'].dt.to_period('d').astype(str) + 'T' + df['Ab-Zeit'].astype(str)
    df['timestamp_2022'] = df['timestamp_2021'].str.replace('2021', '2022')

    df_export = pd.concat(
        [
            df[['timestamp_2021', 'Stadtlast 2021']].rename(columns={'timestamp_2021': 'timestamp', 'Stadtlast 2021': 'netzlast_kwh'}),
            df[['timestamp_2022', 'Stadtlast 2022']].rename(columns={'timestamp_2022': 'timestamp', 'Stadtlast 2022': 'netzlast_kwh'}),
         ]
    ).dropna(subset=['netzlast_kwh']).reset_index(drop=True)
    export_filename = os.path.join(os.path.dirname(__file__), 'data', 'netzlast.csv')
    df_export.to_csv(export_filename, index=False, sep=';')
    pass


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info('Job successful!')
