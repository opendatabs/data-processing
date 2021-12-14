import logging
import common
import os
from stata_daily_upload import credentials
import common.change_tracking as ct
import ods_publish.etl_id as odsp


def main():
    uploads = [{'file': 'StatA/Bevoelkerung/sterbefaelle.csv', 'dest_dir': 'bevoelkerung', 'ods_id': '100079'},
               {'file': 'StatA/Bevoelkerung/geburten_nach_datum.csv', 'dest_dir': 'bevoelkerung', 'ods_id': '100092'},
               {'file': 'StatA/Bevoelkerung/geburten_nach_monat.csv', 'dest_dir': 'bevoelkerung', 'ods_id': '100099'},
               {'file': 'StatA/Tourismus/tourismus-daily.csv', 'dest_dir': 'tourismus', 'ods_id': '100106', 'embargo': True},
               {'file': 'StatA/Tourismus/tourismus-monthly.csv', 'dest_dir': 'tourismus', 'ods_id': '100107', 'embargo': True},
               {'file': 'StatA/Veranstaltung/veranstaltungen.csv', 'dest_dir': 'veranstaltungen', 'ods_id': '100074'},
               {'file': 'StatA/Bevoelkerung/01bevoelkerung_monat_nach_bezirk.csv', 'dest_dir': 'bevoelkerung', 'ods_id': '100125'},
               {'file': 'StatA/Bevoelkerung/02bevoelkerung_jahr_nach_CH_A_geschlecht.csv', 'dest_dir': 'bevoelkerung', 'ods_id': '100128'},
               {'file': 'StatA/Bevoelkerung/03bevoelkerung_jahr_nach_heimat_geschlecht.csv', 'dest_dir': 'bevoelkerung', 'ods_id': '100126'},
               {'file': 'StatA/Bevoelkerung/04bevoelkerung_jahr_nach_vorname.csv', 'dest_dir': 'bevoelkerung', 'ods_id': '100129'},
               {'file': 'StatA/Bevoelkerung/05bevoelkerung_jahr_nach_nachname.csv', 'dest_dir': 'bevoelkerung', 'ods_id': '100127'},
               {'file': 'StatA/Quartierradar/quartierradar_ogd.csv', 'dest_dir': 'quartierradar', 'ods_id': '100011'},
               {'file': 'StatA/Bevoelkerungsszenarien/Bevoelkerungsszenarien_Basel-Stadt.csv', 'dest_dir': 'bevoelkerungsszenarien', 'ods_id': '100007'},
               {'file': 'StatA/Leerstand/leerstand.csv', 'dest_dir': 'leerstand', 'ods_id': '100010'},
               {'file': 'StatA/Bevoelkerung/OpenDataMigration.csv', 'dest_dir': 'bevoelkerung', 'ods_id': '100002'},
               {'file': 'StatA/Bevoelkerung/OpenDataUmzuege.csv', 'dest_dir': 'bevoelkerung', 'ods_id': '100001'},
               {'file': 'StatA/Bevoelkerung/scheidungen_nach_datum.csv', 'dest_dir': 'bevoelkerung', 'ods_id': '100139'},
               {'file': 'StatA/Bevoelkerung/trauungen_nach_datum.csv', 'dest_dir': 'bevoelkerung', 'ods_id': '100156'},
               {'file': 'GD-GS/coronavirus-massentests/manual-entry/massentests_primarsek1_manual_entry.txt', 'dest_dir': 'gd_gs/coronavirus_massenteststs/manual_entry', 'ods_id': '100145'},
               {'file': 'GD-GS/coronavirus-massentests/manual-entry/massentests_betriebe_manual_entry.txt', 'dest_dir': 'gd_gs/coronavirus_massenteststs/manual_entry', 'ods_id': '100146'},
               ]
    for upload in uploads:
        file_path = os.path.join(credentials.path_work, upload['file'])
        if (not upload.get('embargo')) or (upload.get('embargo') and common.is_embargo_over(file_path)):
            if ct.has_changed(file_path, do_update_hash_file=False):
                common.upload_ftp(file_path, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, upload['dest_dir'])
                odsp.publish_ods_dataset_by_id(upload['ods_id'])
                ct.update_hash_file(file_path)
    print('Job successful!')


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
