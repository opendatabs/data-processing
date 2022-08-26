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
               {'file': 'GD-GS/coronavirus-massentests/manual-entry/massentests_single_sek2_manual_entry.txt', 'dest_dir': 'gd_gs/coronavirus_massenteststs/manual_entry', 'ods_id': '100153'},
               {'file': 'GD-GS/coronavirus-massentests/manual-entry/massentests_schulen_manual_entry.txt', 'dest_dir': 'gd_gs/coronavirus_massenteststs/manual_entry', 'ods_id': '100183'},
               {'file': 'StatA/Bevoelkerung/wanderungen.csv', 'dest_dir': 'bevoelkerung', 'ods_id': '100138'},
               {'file': 'StatA/FST-OGD/interfaces/interfaces.xlsx', 'dest_dir': 'FST-OGD', 'ods_id': '100184'},
               {'file': 'StatA/Bevoelkerung/sterberaten.csv', 'dest_dir': 'bevoelkerung', 'ods_id': '100173'},
               {'file': 'StatA/Bildung/SuS_Prognose_BS.csv', 'dest_dir': 'bildung', 'ods_id': '100121'},
               {'file': 'StatA/Bildung/SuS_Prognose_RiBe.csv', 'dest_dir': 'bildung', 'ods_id': '100122'},
               {'file': 'StatA/Bildung/Perimeter_Schulprognose_korr.zip', 'dest_dir': 'bildung', 'ods_id': '100124'},
               {'file': 'StatA/Bevoelkerung/vornamen_neugeborene.csv', 'dest_dir': 'bevoelkerung', 'ods_id': '100192'},
               {'file': 'StatA/Bildung/Studierende.csv', 'dest_dir': 'bildung', 'ods_id': '100191'},
               {'file': 'StatA/Bevoelkerung/06bevoelkerung_jahr_plz.csv', 'dest_dir': 'bevoelkerung', 'ods_id': '100197'},
               {'file': 'MD/upload/faelle_minderjaehrige_3j_klassen.csv', 'dest_dir': 'covid19bs', 'ods_id': '100152'},
               {'file': 'StatA/Parzellen/Liegenschaften_Parzellen.csv', 'dest_dir': 'parzellen', 'ods_id': '100202'}
               ]
    file_not_found_errors = []
    for upload in uploads:
        file_path = os.path.join(credentials.path_work, upload['file'])
        try:
            if (not upload.get('embargo')) or (upload.get('embargo') and common.is_embargo_over(file_path)):
                if ct.has_changed(file_path, method='modification_date'):
                    common.upload_ftp(file_path, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, upload['dest_dir'])
                    odsp.publish_ods_dataset_by_id(upload['ods_id'])
                    ct.update_mod_timestamp_file(file_path)
        except FileNotFoundError as e:
            file_not_found_errors.append(e)
    error_count = len(file_not_found_errors)
    if error_count > 0:
        for e in file_not_found_errors:
            logging.exception(e)
        raise FileNotFoundError(f'{error_count} FileNotFoundErrors have been raised!')
    print('Job successful!')


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
