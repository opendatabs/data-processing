import logging
import pandas as pd
import openpyxl
import common
from iwb_elektroauto_ladestationen import credentials
from more_itertools import chunked


def main():
    df_metadata = pd.read_excel(credentials.data_file_path, sheet_name=1)
    df_events = pd.read_excel(credentials.data_file_path, sheet_name=0)

    df_test = df_events.merge(df_metadata, left_on='DEVEUI', right_on='DevEUI', how='outer')
    missing_metadata = df_test.query('ParkingField.isnull()').DEVEUI.unique()

    df_merge = df_events.merge(df_metadata, left_on='DEVEUI', right_on='DevEUI', how='left')
    df = df_merge.rename(columns={'MeasurementTImestamp': 'TimeStamp'})[['Addresse', 'Power', 'Location', 'ParkingField', 'TotalParkings', 'Status', 'TimeStamp']]
    df.Status = df.Status.str.upper()

    delete_url = credentials.push_url.replace('/push/', '/delete/')

    chunk_size = 25000
    df_chunks = chunked(df.index, chunk_size)
    for df_chunk_indexes in df_chunks:
        logging.info(f'Submitting a data chunk to ODS...')
        df_chunk = df.iloc[df_chunk_indexes]
        r = common.ods_realtime_push_df(df_chunk, credentials.push_url)
        # r_delete = common.ods_realtime_push_df(df_chunk, delete_url)
    pass


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
