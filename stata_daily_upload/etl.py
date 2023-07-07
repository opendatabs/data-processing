import logging
import common
import os
from stata_daily_upload import credentials
import common.change_tracking as ct
import ods_publish.etl_id as odsp


def main():
    uploads = [{'file': 'StatA/Bevoelkerung/100079_sterbefaelle.csv', 'dest_dir': 'bevoelkerung', 'ods_id': '100079'},
               {'file': 'StatA/Bevoelkerung/100092_geburten_nach_datum.csv', 'dest_dir': 'bevoelkerung', 'ods_id': '100092'},
               {'file': 'StatA/Bevoelkerung/100099_geburten_nach_herkunft_monat.csv', 'dest_dir': 'bevoelkerung', 'ods_id': '100099'},
               {'file': 'StatA/Tourismus/100106_tourismus-daily.csv', 'dest_dir': 'tourismus', 'ods_id': '100106', 'embargo': True},
               {'file': 'StatA/Tourismus/100107_tourismus-monthly.csv', 'dest_dir': 'tourismus', 'ods_id': '100107', 'embargo': True},
               {'file': 'StatA/Veranstaltung/100074_veranstaltungen.csv', 'dest_dir': 'veranstaltungen', 'ods_id': '100074'},
               {'file': 'StatA/Bevoelkerung/100125_bevoelkerung_monat_nach_bezirk.csv', 'dest_dir': 'bevoelkerung', 'ods_id': '100125'},
               {'file': 'StatA/Bevoelkerung/100126_bevoelkerung_jahr_nach_heimat_geschlecht.csv', 'dest_dir': 'bevoelkerung', 'ods_id': '100126'},
               {'file': 'StatA/Bevoelkerung/100127_bevoelkerung_jahr_nach_nachname.csv', 'dest_dir': 'bevoelkerung', 'ods_id': '100127'},
               {'file': 'StatA/Bevoelkerung/100128_bevoelkerung_jahr_nach_CH_A_geschlecht.csv', 'dest_dir': 'bevoelkerung', 'ods_id': '100128'},
               {'file': 'StatA/Bevoelkerung/100129_bevoelkerung_jahr_nach_vorname.csv', 'dest_dir': 'bevoelkerung', 'ods_id': '100129'},
               {'file': 'StatA/Quartierradar/100011_quartierradar_ogd.csv', 'dest_dir': 'quartierradar', 'ods_id': '100011'},
               {'file': 'StatA/Quartierradar/100226_quartierradar_ogd_long.csv', 'dest_dir': 'quartierradar', 'ods_id': '100226'},
               {'file': 'StatA/Bevoelkerungsszenarien/100007_Bevoelkerungsszenarien_Basel-Stadt.csv', 'dest_dir': 'bevoelkerungsszenarien', 'ods_id': '100007'},
               {'file': 'StatA/Leerstand/100010_leerstand.csv', 'dest_dir': 'leerstand', 'ods_id': '100010'},
               {'file': 'StatA/Bevoelkerung/100139_scheidungen_nach_datum.csv', 'dest_dir': 'bevoelkerung', 'ods_id': '100139'},
               {'file': 'StatA/Bevoelkerung/100156_trauungen_nach_datum.csv', 'dest_dir': 'bevoelkerung', 'ods_id': '100156'},
               {'file': 'StatA/Bevoelkerung/100238_bevoelkerung_alter_geschl_seit_1945.csv', 'dest_dir': 'bevoelkerung', 'ods_id': '100238'},
               {'file': 'GD-GS/coronavirus-massentests/manual-entry/massentests_primarsek1_manual_entry.txt', 'dest_dir': 'gd_gs/coronavirus_massenteststs/manual_entry', 'ods_id': '100145'},
               {'file': 'GD-GS/coronavirus-massentests/manual-entry/massentests_betriebe_manual_entry.txt', 'dest_dir': 'gd_gs/coronavirus_massenteststs/manual_entry', 'ods_id': '100146'},
               {'file': 'GD-GS/coronavirus-massentests/manual-entry/massentests_single_sek2_manual_entry.txt', 'dest_dir': 'gd_gs/coronavirus_massenteststs/manual_entry', 'ods_id': '100153'},
               {'file': 'GD-GS/coronavirus-massentests/manual-entry/massentests_schulen_manual_entry.txt', 'dest_dir': 'gd_gs/coronavirus_massenteststs/manual_entry', 'ods_id': '100183'},
               {'file': 'StatA/Bevoelkerung/100138_wanderungen.csv', 'dest_dir': 'bevoelkerung', 'ods_id': '100138'},
               {'file': 'StatA/FST-OGD/interfaces/interfaces.xlsx', 'dest_dir': 'FST-OGD', 'ods_id': '100184'},
               {'file': 'StatA/Bevoelkerung/100173_sterberaten.csv', 'dest_dir': 'bevoelkerung', 'ods_id': '100173'},
               {'file': 'StatA/Bildung/100121_SuS_Prognose_BS.csv', 'dest_dir': 'bildung', 'ods_id': '100121'},
               {'file': 'StatA/Bildung/100122_SuS_Prognose_RiBe.csv', 'dest_dir': 'bildung', 'ods_id': '100122'},
               {'file': 'StatA/Bildung/100124_Perimeter_Schulprognose_korr.zip', 'dest_dir': 'bildung', 'ods_id': '100124'},
               {'file': 'StatA/Bevoelkerung/100192_vornamen_neugeborene.csv', 'dest_dir': 'bevoelkerung', 'ods_id': '100192'},
               {'file': 'StatA/Bildung/100191_Studierende.CSV', 'dest_dir': 'bildung', 'ods_id': '100191'},
               {'file': 'StatA/Bevoelkerung/100197_bevoelkerung_jahr_plz.csv', 'dest_dir': 'bevoelkerung', 'ods_id': '100197'},
               {'file': 'MD/upload/faelle_minderjaehrige_3j_klassen.csv', 'dest_dir': 'covid19bs', 'ods_id': '100152'},
               {'file': 'StatA/Bevoelkerung/100225_Schutzsuchende.csv', 'dest_dir': 'bevoelkerung', 'ods_id': '100225'},
               {'file': 'StatA/witterung/100227_Witterungserscheinungen.xlsx', 'dest_dir': 'witterung', 'ods_id': '100227'},
               {'file': 'StatA/Wahlen-Abstimmungen/Zeitreihe_Volksabstimmungen/100229_Abstimmungsvorlagen.xlsx', 'dest_dir': 'wahlen_abstimmungen/zeitreihe_volksabstimmungen', 'ods_id': '100229'},
               {'file': 'StatA/Stromverbrauch/100245_Strom_Wetter.csv', 'dest_dir': 'stromverbrauch', 'ods_id': '100245'},
               {'file': 'BachApp/BachApp-CMS.xlsx', 'dest_dir': 'bachapp', 'ods_id': ['100246', '100247', '100255', '100283', '100284', '100287', '100290']},
               {'file': 'AUE-FA/Fischereiverbotszonen/fischereiverbotszonen_rhein.zip', 'dest_dir': 'aue/fischereiverbotszonen', 'ods_id': '100278'},
               {'file': 'TBA/Rhein-Schwimmzonen/Schwimmzonen.zip', 'dest_dir': 'tba/shapes', 'ods_id': '100270'},
               {'file': 'TBA/Bachapp-Rhein-Ausstiegmoeglichkeiten/Treppen_und_Ausstiegsleitern_area.zip', 'dest_dir': 'tba/shapes', 'ods_id': '100285'},
               {'file': 'TBA/Bachapp-Rhein-Ausstiegmoeglichkeiten/Treppen_und_Ausstiegsleitern_point.zip', 'dest_dir': 'tba/shapes', 'ods_id': '100285'},
               {'file': 'BVD-Stadtgaertnerei/bachapp_grillstellen/grillstellen_stg.gpkg', 'dest_dir': 'stadtgaertnerei/bachapp_grillstellen', 'ods_id': '100276'},
               {'file': ['StatA/Wahlen-Abstimmungen/sr/2023/D0012MAKA.TXT', 'StatA/Wahlen-Abstimmungen/sr/2023/T0012MAKA.TXT'],
                'dest_dir': 'wahlen_abstimmungen/wahlen/sr/2023', 'ods_id': '100282'},
               {'file': ['StatA/Wahlen-Abstimmungen/nr/2023/D0012HERK.TXT', 'StatA/Wahlen-Abstimmungen/nr/2023/T0012HERK.TXT'],
                'dest_dir': 'wahlen_abstimmungen/wahlen/nr/2023',
                'ods_id': '100281'},
               {'file': ['StatA/Wahlen-Abstimmungen/nr/2023/D0012LIST.TXT', 'StatA/Wahlen-Abstimmungen/nr/2023/T0012LIST.TXT'],
                'dest_dir': 'wahlen_abstimmungen/wahlen/nr/2023/aggregiert',
                'ods_id': '100297'},
               {'file': 'StatA/Wahlen-Abstimmungen/bgr/2023/D2701HERK.TXT',
                'dest_dir': 'wahlen_abstimmungen/wahlen/bgr/2023',
                'ods_id': '100300'},
               {'file': 'StatA/Wahlen-Abstimmungen/bgr/2023/D2701LIST.TXT',
                'dest_dir': 'wahlen_abstimmungen/wahlen/bgr/2023',
                'ods_id': '100301'},
               {'file': 'StatA/Steuern/100140_Reineinkommen.csv', 'dest_dir': 'steuern', 'ods_id': '100140'},
               {'file': 'StatA/Steuern/100157_Reinvermoegen.csv', 'dest_dir': 'steuern', 'ods_id': '100157'},
               {'file': 'StatA/Steuern/100165_Kennzahlen.csv', 'dest_dir': 'steuern', 'ods_id': '100165'},
               {'file': 'StatA/Personalbestand/100123_Lohntabelle_Basel_Stadt.csv', 'dest_dir': 'personalbestand', 'ods_id': '100123'},
               {'file': 'StatA/Personalbestand/100263_Personalbestand_Lohnklasse.csv', 'dest_dir': 'personalbestand', 'ods_id': '100263'},
               {'file': 'StatA/Personalbestand/100264_Personalbestand_Dep_Buchung_Geschl.csv', 'dest_dir': 'personalbestand', 'ods_id': '100264'},
               {'file': 'StatA/Personalbestand/100265_Personalbestand_Dep_Dienst_Geschl_Alter.csv', 'dest_dir': 'personalbestand', 'ods_id': '100265'},
               {'file': 'StatA/Personalbestand/100266_Personalbestand_Dep_Buchung_Staat.csv', 'dest_dir': 'personalbestand', 'ods_id': '100266'},
               {'file': 'StatA/Personalbestand/100267_Personalbestand_Dep_Buchung_Kanton.csv', 'dest_dir': 'personalbestand', 'ods_id': '100267'},
               ]
    file_not_found_errors = []
    for upload in uploads:
        file_property = upload['file']
        try:
            changed = 0
            if type(file_property) == list:
                for file in file_property:
                    file_path = os.path.join(credentials.path_work, file)
                    if (not upload.get('embargo')) or (upload.get('embargo') and common.is_embargo_over(file_path)):
                        if ct.has_changed(file_path, method='modification_date'):
                            changed = 1
                            ct.update_mod_timestamp_file(file_path)
                            common.upload_ftp(file_path, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, upload['dest_dir'])

            else:
                file_path = os.path.join(credentials.path_work, upload['file'])
                if (not upload.get('embargo')) or (upload.get('embargo') and common.is_embargo_over(file_path)):
                    if ct.has_changed(file_path, method='modification_date'):
                        changed = 1
                        ct.update_mod_timestamp_file(file_path)
                        common.upload_ftp(file_path, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, upload['dest_dir'])
            if changed == 1:
                ods_id_property = upload['ods_id']
                if type(ods_id_property) == list:
                    for single_ods_id in ods_id_property:
                        odsp.publish_ods_dataset_by_id(single_ods_id)
                else:
                    odsp.publish_ods_dataset_by_id(ods_id_property)

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
