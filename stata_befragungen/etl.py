import io
import logging
import os
import pathlib
import shutil
import pandas as pd
import unicodedata
import common.change_tracking as ct
import ods_publish.etl_id as odsp
import common
from charset_normalizer import from_path
from stata_befragungen import credentials


def main():
    data_orig_root = credentials.import_root_folder
    data_path = os.path.join(pathlib.Path(__file__).parent, 'data')
    datasets = [
        {
            'data_file': os.path.join(data_orig_root, '55plus/2011-2023/datensatz_55plus_2011_2023_OGD_TEXT.csv'),
            'var_file': os.path.join(data_orig_root, '55plus/2011-2023/Variablen_55plus_2011_2023_OGD.csv'),
            'export_file': '55plus/Befragung_55_plus_alle_jahre.csv',
            'ftp_folder': '55plus',
            'jahr': None,
            'ods_id': '100185'
        }
    ]
    ''' Do not process the single years for now
        { 
            'data_file': os.path.join(data_orig_root, '55plus/2019/DATENSATZ2019_OGD_TEXT.csv'),
            'var_file': os.path.join(data_orig_root, '55plus/2019/VARIABLEN_2019.csv'),
            'export_file': '55plus/Befragung_55_plus_2019.csv',
            'ftp_folder': '55plus',
            'jahr': 2019,
            'ods_id': '100203'
        },
        {
            'data_file': os.path.join(data_orig_root, '55plus/2015/DATENSATZ_2015_TEXT.csv'),
            'var_file': os.path.join(data_orig_root, '55plus/2011/VARIABLEN_2011.csv'),
            'export_file': '55plus/Befragung_55_plus_2015.csv',
            'ftp_folder': '55plus',
            'jahr': 2015,
            'ods_id': '100204'
        },
        {
            'data_file': os.path.join(data_orig_root, '55plus/2011/WORK_DATENSATZ_2011_TEXT.csv'),
            'var_file': os.path.join(data_orig_root, '55plus/2011/VARIABLEN_2011.csv'),
            'export_file': '55plus/Befragung_55_plus_2011.csv',
            'ftp_folder': '55plus',
            'jahr': 2011,
            'ods_id': '100205'
        }
    ]
    '''
    for ds in datasets:
        process_single_file(data_file=ds['data_file'], var_file=ds['var_file'], data_path=data_path,
                            export_file=ds['export_file'], ftp_folder=ds['ftp_folder'], ods_id=ds['ods_id'],
                            jahr=ds['jahr'])

    root_dirs_for_zip = ['55plus']
    for zip_root_dir in root_dirs_for_zip:
        publish_zip_from_folder(data_path, zip_root_dir)


def publish_zip_from_folder(data_path, zip_root_dir):
    logging.info(f'Creating zip archive of data in folder "{zip_root_dir}" for the web...')
    zip_file_name_noext = os.path.join(pathlib.Path(__file__).parent, 'data', zip_root_dir)
    dir_to_zip = os.path.join(data_path, zip_root_dir)
    zip_name = f'{zip_file_name_noext}.zip'
    shutil.make_archive(zip_file_name_noext, 'zip', dir_to_zip)
    if ct.has_changed(zip_name):
        common.upload_ftp(zip_name, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, f'befragungen/{zip_root_dir}')
        ct.update_hash_file(zip_name)


def process_single_file(data_file, var_file, data_path, export_file, ftp_folder, ods_id, jahr=None):
    logging.info(f'Processing survey with data file {data_file}...')
    enc = get_encoding(data_file)
    logging.info(f'Normalizing unicode data to get rid of &nbsp; (\\xa0)...')
    with open(data_file, 'r', encoding=enc) as f:
        text = f.read()
        clean_text = unicodedata.normalize("NFKD", text)
    df_data = pd.read_csv(io.StringIO(clean_text), encoding=enc, sep=';', engine='python')
    df_data.to_csv(os.path.join(data_path, export_file.replace('/', '/Daten_')), index=False)
    if jahr and 'Jahr' not in df_data:
        logging.info(f'Re-setting column Jahr to {jahr}...')
        df_data['Jahr'] = jahr
    df_data_long = df_data.melt(id_vars=['ID', 'Jahr', 'weight', 'Methode', 'S1', 'S2', 'S3', 'S4', 'S5', 'S6', 'S7',
                                         'WV', 'Wohndauer_Adresse', 'Wohndauer_Quartier', 'Wohndauer_Kanton',
                                         'Wohnflaeche', 'Zimmerzahl'])

    enc = get_encoding(var_file)
    df_var = pd.read_csv(var_file, encoding=enc, sep=';', engine='python')
    df_var.to_csv(os.path.join(data_path, export_file.replace('/', '/Variabeln_')), index=False)
    df_merge = df_data_long.merge(df_var, how='left', left_on='variable', right_on='FrageName')
    df_merge = df_merge[df_merge['value'] != '**OTHER**']
    df_export = df_merge[['Jahr', 'ID', 'FrageName', 'FrageLabel', 'FrageLabel_Fortsetzung', 'FrageLabel_Anmerkung', 'value']].rename(columns={'value': 'Antwort'})
    export_filename = os.path.join(data_path, export_file)
    df_export.to_csv(export_filename, index=False)
    if ct.has_changed(export_filename):
        common.upload_ftp(export_filename, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, f'befragungen/{ftp_folder}')
        odsp.publish_ods_dataset_by_id(ods_id)
        ct.update_hash_file(export_filename)


def get_encoding(filename):
    logging.info('Retrieving encoding...')
    result = from_path(filename)
    enc = result.best().encoding
    logging.info(f'Retrieved encoding {enc}...')
    return enc


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info(f'Job successfully completed!')
