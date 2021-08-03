import logging
import common
import os
from stata_daily_upload import credentials
import common.change_tracking as ct
import ods_publish.etl_id as odsp


def main():
    uploads = [{'file': 'Bevoelkerung/sterbefaelle.csv', 'dest_dir': 'bevoelkerung', 'ods_id': '100079'},
               {'file': 'Bevoelkerung/geburten_nach_datum.csv', 'dest_dir': 'bevoelkerung', 'ods_id': '100092'},
               {'file': 'Bevoelkerung/geburten_nach_monat.csv', 'dest_dir': 'bevoelkerung', 'ods_id': '100099'},
               {'file': 'Tourismus/tourismus-daily.csv', 'dest_dir': 'tourismus', 'ods_id': '100106'},
               {'file': 'Tourismus/tourismus-monthly.csv', 'dest_dir': 'tourismus', 'ods_id': '100107'},
               {'file': 'Veranstaltung/veranstaltungen.csv', 'dest_dir': 'veranstaltungen', 'ods_id': '100074'},
               {'file': 'Bevoelkerung/01bevoelkerung_monat_nach_bezirk.csv', 'dest_dir': 'bevoelkerung', 'ods_id': '100125'},
               {'file': 'Bevoelkerung/02bevoelkerung_jahr_nach_CH_A_geschlecht.csv', 'dest_dir': 'bevoelkerung', 'ods_id': '100128'},
               {'file': 'Bevoelkerung/03bevoelkerung_jahr_nach_heimat_geschlecht.csv', 'dest_dir': 'bevoelkerung', 'ods_id': '100126'},
               {'file': 'Bevoelkerung/04bevoelkerung_jahr_nach_vorname.csv', 'dest_dir': 'bevoelkerung', 'ods_id': '100129'},
               {'file': 'Bevoelkerung/05bevoelkerung_jahr_nach_nachname.csv', 'dest_dir': 'bevoelkerung', 'ods_id': '100127'},
               {'file': 'Quartierradar/quartierradar_ogd.csv', 'dest_dir': 'quartierradar', 'ods_id': '100011'},
               {'file': 'Bevoelkerungsszenarien/Bevoelkerungsszenarien_Basel-Stadt.csv', 'dest_dir': 'bevoelkerungsszenarien', 'ods_id': '100007'},
               {'file': 'Leerstand/leerstand.csv', 'dest_dir': 'leerstand', 'ods_id': '100010'},
               ]
    for upload in uploads:
        file_path = os.path.join(credentials.path_work, upload['file'])
        if ct.has_changed(file_path):
            common.upload_ftp(file_path, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, upload['dest_dir'])
            odsp.publish_ods_dataset_by_id(upload['ods_id'])
        else:
            logging.info(f'No changes detected, doing nothing for this dataset: {file_path}')
    print('Job successful!')


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
