import logging
import os
import pathlib
import pandas as pd
import common
import ods_publish.etl_id as odsp
from common import change_tracking as ct
from iwb_elektroauto_ladestationen import credentials


def main():
    raw_data_file = os.path.join(pathlib.Path(__file__).parent, 'data', 'iwb_elektroauto_ladestationen_raw.csv')
    logging.info(f'Downloading raw data from ods to file {raw_data_file}...')
    r = common.requests_get(f'https://data.bs.ch/api/records/1.0/download?dataset=100149&apikey={credentials.api_key}')
    with open(raw_data_file, 'wb') as f:
        f.write(r.content)
    logging.info(f'Reading raw data csv and performing calculations...')
    df = pd.read_csv(raw_data_file, sep=';')
    df.addresse = df.addresse.str.replace("Zürcherstrasse 140'", 'Zürcherstrasse 140', regex=False)
    df['ts'] = pd.to_datetime(df.timestamp)
    df_groups = df.groupby(['addresse', 'parkingfield']).size().reset_index().rename(columns={0: 'count'})
    dfs = []
    for index, row in df_groups.iterrows():
        dft = df.sort_values(by=['ts']).query('addresse == @row.addresse and parkingfield == @row.parkingfield')
        dft['timestamp_diff'] = dft.ts.diff()
        dft['status_changed'] = dft.status.ne(dft.status.shift(1).bfill()).astype(int)
        dfs.append(dft)
    df_diff = pd.concat(dfs)
    export_columns = ['timestamp', 'address', 'power', 'parkingfield', 'totalparkings', 'status']
    df_status_changes = df_diff.query('status_changed == 1').rename(columns={'addresse': 'address'})[export_columns]
    df_export = df_status_changes

    # loc_file = os.path.join(pathlib.Path(__file__).parent, 'data', 'iwb_elektroauto_ladestationen_locations.csv')
    # logging.info(f'Downloading location data from ods to {loc_file}...')
    # r = common.requests_get(f'https://data.bs.ch/api/records/1.0/download?dataset=100005&apikey={credentials.api_key}')
    # with open(loc_file, 'wb') as f:
    #     f.write(r.content)
    # logging.info(f'Reading location csv and performing calculations...')
    # df_loc = pd.read_csv(loc_file, sep=';')
    # export_columns =  export_columns.append('geo_point_2d')
    # df_export = df_status_changes.merge(df_loc, how='left', left_on='address', right_on='name')[export_columns]

    export_file = os.path.join(pathlib.Path(__file__).parent, 'data', 'iwb_elektroauto_ladestationen_status_changes.csv')
    logging.info(f'Exporting data as csv to {export_file}...')
    df_export.to_csv(export_file, index=False)
    if ct.has_changed(export_file):
        common.upload_ftp(export_file, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'iwb/elektroauto_ladestationen')
        odsp.publish_ods_dataset_by_id('100196')
        ct.update_hash_file(export_file)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info(f'Job successful!')
