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
    root = credentials.import_root_folder
    befragungen = [
        {
            'data_file': os.path.join(root, '55plus/Alle_Jahre/DATENSATZ_55plus_OGD_TEXT.csv'),
            'var_file': os.path.join(root, '55plus/Alle_Jahre/VARIABLEN_2019.csv'),
            'export_file': 'Befragung_55_plus_alle_jahre.csv',
            'ftp_folder': '55plus',
            'jahr': None,
            'ods_id': '100185'
        },
        {
            'data_file': os.path.join(root, '55plus/2019/DATENSATZ2019_OGD_TEXT.csv'),
            'var_file': os.path.join(root, '55plus/2019/VARIABLEN_2019.csv'),
            'export_file': 'Befragung_55_plus_2019.csv',
            'ftp_folder': '55plus',
            'jahr': 2019,
            'ods_id': '100203'
        },
        {
            'data_file': os.path.join(root, '55plus/2015/DATENSATZ_2015_TEXT.csv'),
            'var_file': os.path.join(root, '55plus/2011/VARIABLEN_2011.csv'),
            'export_file': 'Befragung_55_plus_2015.csv',
            'ftp_folder': '55plus',
            'jahr': 2015,
            'ods_id': '100204'
        },
        {
            'data_file': os.path.join(root, '55plus/2011/WORK_DATENSATZ_2011_TEXT.csv'),
            'var_file': os.path.join(root, '55plus/2011/VARIABLEN_2011.csv'),
            'export_file': 'Befragung_55_plus_2011.csv',
            'ftp_folder': '55plus',
            'jahr': 2011,
            'ods_id': '100205'
        }
    ]
    for bef in befragungen:
        process_single_file(data_file=bef['data_file'], var_file=bef['var_file'], export_file=bef['export_file'], ftp_folder=bef['ftp_folder'], ods_id=bef['ods_id'], jahr=bef['jahr'])


def process_single_file(data_file, var_file, export_file, ftp_folder, ods_id, jahr=None):
    # todo: get zip file and unzip to data_orig, then load data from there.
    logging.info(f'Processing survey with data file {data_file}...')
    enc = get_encoding(data_file)
    logging.info(f'Normalizing unicode data to get rid of &nbsp; (\\xa0)...')
    with open(data_file, 'r', encoding=enc) as f:
        text = f.read()
        clean_text = unicodedata.normalize("NFKD", text)
    df_data = pd.read_csv(io.StringIO(clean_text), encoding=enc, sep=';', engine='python')
    if jahr and 'Jahr' not in df_data:
        logging.info(f'Re-setting column Jahr to {jahr}...')
        df_data['Jahr'] = jahr
    df_data_long = df_data.melt(id_vars=['ID', 'Jahr'])
    enc = get_encoding(var_file)
    df_var = pd.read_csv(var_file, encoding=enc, sep=';', engine='python')
    df_merge = df_data_long.merge(df_var, how='left', left_on='variable', right_on='FrageName')
    df_export = df_merge[['Jahr', 'ID', 'FrageName', 'FrageLabel', 'FrageLabel_Fortsetzung', 'FrageLabel_Anmerkung', 'value']].rename(columns={'value': 'Antwort'})
    export_filename = os.path.join(pathlib.Path(__file__).parent, 'data', export_file)
    df_export.to_csv(export_filename, index=False)
    if ct.has_changed(export_filename):
        common.upload_ftp(export_filename, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, f'befragungen/{ftp_folder}')
        odsp.publish_ods_dataset_by_id(ods_id)
        ct.update_hash_file(export_filename)


def get_encoding(filename):
    logging.info(f'Retrieving encoding...')
    with open(filename, 'rb') as f:
        raw_data = f.read()
        result = chardet.detect(raw_data)
        enc = result['encoding']
        logging.info(f'Retrieved encoding {enc}...')
    return enc


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info(f'Job successfully completed!')
