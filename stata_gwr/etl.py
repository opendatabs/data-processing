import logging
import os
import zipfile
import common.change_tracking as ct
import ods_publish.etl_id as odsp
import common
from stata_gwr import credentials


def main():
    r = common.requests_get('https://public.madd.bfs.admin.ch/bs.zip')
    r.raise_for_status()
    data_orig_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data_orig')
    zip_folder = 'bs'
    zip_file_path = os.path.join(data_orig_path, f'{zip_folder}.zip')
    with open(zip_file_path, "wb") as f:
        f.write(r.content)
    if ct.has_changed(zip_file_path):
        with zipfile.ZipFile(zip_file_path) as z:
            z.extractall(os.path.join(data_orig_path, zip_folder))
        for filename in os.listdir(os.path.join(data_orig_path, zip_folder)):
            file_path = os.path.join(data_orig_path, zip_folder, filename)
            if os.path.isfile(file_path):
                common.upload_ftp(file_path, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, f'gwr/{zip_folder}')
        # todo: Publish ods datasets
        # filenames = ['eingang_entree_entrata.csv', 'gebaeude_batiment_edificio.csv', 'kodes_codes_codici.csv', 'wohnung_logement_abitazione.csv']
        # odsp.publish_ods_dataset_by_id('')
        ct.update_hash_file(zip_file_path)
    pass


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info('Job successful!')
