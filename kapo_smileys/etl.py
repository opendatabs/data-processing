import glob
import logging
import os
import shapefile # library pyshp
import common
import pandas as pd
import numpy as np
from common import change_tracking as ct
import ods_publish.etl_id as odsp
from kapo_smileys import credentials


# see https://gist.github.com/aerispaha/f098916ac041c286ae92d037ba5c37ba
def read_shapefile(shp_path):
    sf = shapefile.Reader(shp_path)
    fields = [x[0] for x in sf.fields][1:]
    records = sf.records()
    points = [s.points for s in sf.shapes()]
    df = pd.DataFrame(columns=fields, data=records)
    df = df.assign(coords=points)
    return df


def parse_einzeldaten(curr_dir, df_einsatz_days, df_einsatze):
    any_changes = False
    einzeldaten_folders = glob.glob(os.path.join(curr_dir, 'data_orig', 'Smiley_Testdaten', 'Datenablage', '*'))
    einzeldaten_dfs = []
    for d in einzeldaten_folders:
        logging.info(f'Working through folder {d}...')
        einzeldaten_files = glob.glob(os.path.join(d, '**', '*.TXT'), recursive=True)
        # einzeldaten_files = glob.glob(os.path.join(CURR_DIR, 'data_orig', 'Smiley_Testdaten', 'Datenablage', '**', '*.TXT'), recursive=True)
        einzeldaten_dfs_pro_standort = []
        for f in einzeldaten_files:
            logging.info(f'Parsing Einzeldaten File {f}...')
            l = f.split(os.path.sep)
            id = int(l[l.index('DATA') - 2].split('_')[0])
            # p = re.compile(r'Datenablage\\\\(?P<idstandort>\d+)_')
            df = (pd.read_csv(f, sep=' ', names=['Datum', 'Zeit', 'V_Einfahrt', 'dummy', 'V_Ausfahrt'], parse_dates=[['Datum', 'Zeit']], infer_datetime_format=True, keep_date_col=True)
                  .rename(columns={'Datum_Zeit': 'Messung_Timestamp', 'Datum': 'Messung_Datum', 'Zeit': 'Messung_Zeit'})
                  .drop(columns=['dummy']))
            df.Messung_Timestamp = df.Messung_Timestamp.dt.tz_localize('Europe/Zurich', ambiguous='infer')
            df['id_standort'] = id
            day_str = os.path.basename(f).split('.')[0]
            df['day_str'] = day_str
            df['V_Delta'] = df.V_Einfahrt - df.V_Ausfahrt
            # Determining Zyklus and Smiley_Nr of measurement
            df_m1 = pd.merge(df_einsatz_days, df, how='right', on=['id_standort', 'day_str']).drop(columns=['datum_aktiv', 'day_str'])
            df_m = pd.merge(df_m1, df_einsatze, how='left', left_on=['id_standort', 'Zyklus', 'Smiley_Nr'], right_on=['id_Standorte', 'Zyklus', 'Smiley-Nr.'])
            df_m = df_m.drop(columns=['id_Standorte', 'Smiley-Nr.'])
            df_m['Phase'] = np.where(df_m.Messung_Timestamp < df_m.Start_Vormessung, 'Vor Vormessung',
                                     np.where(df_m.Messung_Timestamp < df_m.Start_Betrieb, 'Vormessung',
                                              np.where(df_m.Messung_Timestamp < df_m.Start_Nachmessung, 'Betrieb',
                                                       np.where(df_m.Messung_Timestamp < df_m.Ende, 'Nachmessung', 'Nach Ende')))
                                     )
            einzeldaten_dfs_pro_standort.append(df_m)
            export_file_single = os.path.join(curr_dir, 'data', f'{day_str}_{id}.csv')
            df_m.to_csv(export_file_single, index=False)
            if True:  # ct.has_changed(export_file_single):
                any_changes = True
                common.upload_ftp(export_file_single, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'kapo/smileys/data')
                ct.update_hash_file(export_file_single)
        df_all_pro_standort = pd.concat(einzeldaten_dfs_pro_standort)
        einzeldaten_dfs.append(df_all_pro_standort)
    all_df = pd.concat(einzeldaten_dfs)
    export_file_all = os.path.join(curr_dir, 'data', 'all_data.csv')
    all_df.to_csv(export_file_all, index=False)
    if True:  # ct.has_changed(export_file_all):
        common.upload_ftp(export_file_all, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'kapo/smileys/all_data')
        ct.update_hash_file(export_file_all)
    return all_df, any_changes


def parse_einsatzplaene(curr_dir):
    einsatzplan_files = glob.glob(os.path.join(curr_dir, 'data_orig', 'Smiley_Testdaten', 'Einsatzplan', 'Zyklus_*.xlsx'))
    einsatzplan_dfs = []
    for f in einsatzplan_files:
        df = pd.read_excel(f, skiprows=1, parse_dates=[['Datum_VM', 'Uhrzeit_VM'], ['Datum_SB', 'Uhrzeit_SB'], ['Datum_NM', 'Uhrzeit_NM'], ['Datum_Ende', 'Uhrzeit_Ende']])
        filename = os.path.basename(f)
        zyklus = int(filename.split('_')[1])
        jahr = int(filename.split('_')[2].split('.')[0])
        df['Zyklus'] = zyklus
        df['Jahr'] = jahr
        df = df.rename(columns={'Datum_VM_Uhrzeit_VM': 'Start_Vormessung', 'Datum_SB_Uhrzeit_SB': 'Start_Betrieb', 'Datum_NM_Uhrzeit_NM': 'Start_Nachmessung', 'Datum_Ende_Uhrzeit_Ende': 'Ende'})
        for col in ['Start_Vormessung', 'Start_Betrieb', 'Start_Nachmessung', 'Ende']:
            logging.info(f'Localizing timestamp in col {col}...')
            df[col] = df[col].dt.tz_localize('Europe/Zurich', ambiguous='infer')
        df = df[['id_Standorte', 'Strassenname', 'Geschwindigkeit', 'Halterung', 'Energie', 'Ort', 'Smiley-Nr.', 'Start_Vormessung', 'Start_Betrieb', 'Start_Nachmessung', 'Ende', 'Zyklus', 'Jahr']]
        einsatzplan_dfs.append(df)
    df_einsaetze = pd.concat(einsatzplan_dfs)
    return df_einsaetze


def main():
    curr_dir = os.path.dirname(os.path.realpath(__file__))
    logging.info(f'Parsing Einsatzplaene...')
    df_einsaetze = parse_einsatzplaene(curr_dir)

    shp_coords_df = read_shapefile(os.path.join(curr_dir, 'data_orig', 'Smiley_Testdaten', 'GIS', 'Layer-Smiley-Standorte', 'Smiley-Standorte.shp'))
    df_einsaetze = pd.merge(df_einsaetze, shp_coords_df[['idstandort', 'coords']], how='left', left_on='id_Standorte', right_on='idstandort').drop(columns=['idstandort'])
    # put coords in separate columns
    df_coords1 = pd.DataFrame(df_einsaetze.coords.to_list(), columns=['coords'])
    df_coords = pd.DataFrame(df_coords1.coords.to_list(), columns=['lon', 'lat'])
    df_einsaetze['lon_lv95'] = df_coords.lon
    df_einsaetze['lat_lv95'] = df_coords.lat


    logging.info(f'Creating df_einsatz_days with one row per day and standort_id...')
    df_einsatz_days = pd.concat([pd.DataFrame({'id_standort': row.id_Standorte, 'Zyklus': row.Zyklus, 'Smiley_Nr': row['Smiley-Nr.'], 'datum_aktiv': pd.date_range(row.Start_Vormessung, row.Ende, freq='D', normalize=True)})  # , 'Start_Vormessung': row.Start_Vormessung, 'Start_Betrieb': row.Start_Betrieb, 'Start_Nachmessung': row.Start_Nachmessung, 'Ende': row.Ende})
                                 for i, row in df_einsaetze.iterrows()], ignore_index=True)
    df_einsatz_days['day_str'] = df_einsatz_days.datum_aktiv.dt.strftime('%y%m%d')

    shp_images_df = read_shapefile(os.path.join(curr_dir, 'data_orig', 'Smiley_Testdaten', 'GIS', 'Layer-Smiley-Standorte', 'Smiley-Standorte_Start_Ende.shp'))

    logging.info(f'Parsing Einzeldaten...')
    df_all, any_changes = parse_einzeldaten(curr_dir, df_einsatz_days, df_einsaetze)
    if any_changes:
        odsp.publish_ods_dataset_by_id('100268')
    pass


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info('Job successful!')
