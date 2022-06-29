import io
import logging
import os
import pathlib
import pandas as pd
import cchardet as chardet
import unicodedata
import common.change_tracking as ct
import ods_publish.etl_id as odsp
import common
from stata_befragungen import credentials


def main():
    # todo: get zip file and unzip to data_orig, then load data from there.
    # todo: load data from all years
    enc = get_encoding(credentials.data_file)
    logging.info(f'Normalizing unicode data to get rid of &nbsp; (\\xa0)...')
    with open(credentials.data_file, 'r', encoding=enc) as f:
        text = f.read()
        clean_text = unicodedata.normalize("NFKD", text)
    df_data = pd.read_csv(io.StringIO(clean_text), encoding=enc, sep=';')
    df_data_long = df_data.melt(id_vars=['ID'])

    enc = get_encoding(credentials.var_file)
    df_var = pd.read_csv(credentials.var_file, encoding=enc, sep=';')

    df_merge = df_data_long.merge(df_var, how='left', left_on='variable', right_on='FrageName')
    df_merge['Jahr'] = '2019'
    df_export = df_merge[['Jahr', 'ID', 'FrageName', 'FrageLabel', 'FrageLabel_Fortsetzung', 'FrageLabel_Anmerkung', 'value']].rename(columns={'value': 'Antwort'})

    export_filename = os.path.join(pathlib.Path(__file__).parent, 'data', 'Befragung_55_plus.csv')
    df_export.to_csv(export_filename, index=False)
    if ct.has_changed(export_filename):
        common.upload_ftp(export_filename, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'befragungen/55plus')
        odsp.publish_ods_dataset_by_id('100185')
        ct.update_hash_file(export_filename)


def get_encoding(filename):
    logging.info(f'Retrieving encoding...')
    with open(filename, 'rb') as f:
        raw_data = f.read()
        result = chardet.detect(raw_data)
        enc = result['encoding']
    return enc


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info(f'Job successfully completed!')
