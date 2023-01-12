import glob
import logging
import os
import shapefile # library pyshp
import common
import pandas as pd
import numpy as np


# see https://gist.github.com/aerispaha/f098916ac041c286ae92d037ba5c37ba
def read_shapefile(shp_path):
    sf = shapefile.Reader(shp_path)
    fields = [x[0] for x in sf.fields][1:]
    records = sf.records()
    shps = [s.points for s in sf.shapes()]
    df = pd.DataFrame(columns=fields, data=records)
    df = df.assign(coords=shps)
    return df


def main():
    curr_dir = os.path.dirname(os.path.realpath(__file__))
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

    shp_coords_df = read_shapefile(os.path.join(curr_dir, 'data_orig', 'Smiley_Testdaten', 'GIS', 'Layer-Smiley-Standorte', 'Smiley-Standorte.shp'))
    df_einsaetze = pd.merge(df_einsaetze, shp_coords_df[['idstandort', 'coords']], how='left', left_on='id_Standorte', right_on='idstandort').drop(columns=['idstandort'])

    shp_images_df = read_shapefile(os.path.join(curr_dir, 'data_orig', 'Smiley_Testdaten', 'GIS', 'Layer-Smiley-Standorte', 'Smiley-Standorte_Start_Ende.shp'))

    einzeldaten_folders = glob.glob(os.path.join(curr_dir, 'data_orig', 'Smiley_Testdaten', 'Datenablage', '*'))
    einzeldaten_dfs = []
    for d in einzeldaten_folders:
        einzeldaten_files = glob.glob(os.path.join(d, '**', '*.txt'), recursive=True)
        # einzeldaten_files = glob.glob(os.path.join(CURR_DIR, 'data_orig', 'Smiley_Testdaten', 'Datenablage', '**', '*.txt'), recursive=True)
        einzeldaten_dfs_pro_standort = []
        for f in einzeldaten_files:
            l = f.split(os.path.sep)
            id = int(l[l.index('DATA') -2].split('_')[0])
            # p = re.compile(r'Datenablage\\\\(?P<idstandort>\d+)_')
            df = (pd.read_csv(f, sep=' ', names=['Datum', 'Zeit', 'V_Einfahrt', 'dummy', 'V_Ausfahrt'], parse_dates=[['Datum', 'Zeit']], infer_datetime_format=True, keep_date_col=True)
                  .rename(columns={'Datum_Zeit': 'Messung_Timestamp', 'Datum': 'Messung_Datum', 'Zeit': 'Messung_Zeit'})
                  .drop(columns=['dummy']))
            df.Messung_Timestamp = df.Messung_Timestamp.dt.tz_localize('Europe/Zurich', ambiguous='infer')
            df['id_standort'] = id
            df['V_Delta'] = df.V_Einfahrt - df.V_Ausfahrt
            # todo: Assign Einzeldaten files to a specific instance of standort_id, depending on the timestamp of the measurement
            df_m = pd.merge(df_einsaetze, df, how='right', left_on='id_Standorte', right_on='id_standort').drop(columns='id_standort')
            df_m['Phase'] = np.where(df_m.Messung_Timestamp < df_m.Start_Vormessung, 'Vor Vormessung',
                                     np.where(df_m.Messung_Timestamp < df_m.Start_Betrieb, 'Vormessung',
                                              np.where(df_m.Messung_Timestamp < df_m.Start_Nachmessung, 'Betrieb',
                                                       np.where(df_m.Messung_Timestamp < df_m.Ende, 'Nachmessung', 'Nach Ende')))
                                     )
            einzeldaten_dfs_pro_standort.append(df_m)
        df_all_pro_standort = pd.concat(einzeldaten_dfs_pro_standort)
        einzeldaten_dfs.append(df_all_pro_standort)
    df_all = pd.concat(einzeldaten_dfs)

    pass


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info('Job successful!')
