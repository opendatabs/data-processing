import glob
import logging
import os
import shapefile  # library pyshp
import sqlite3
import common
import pandas as pd
import pytz
from datetime import timedelta
import numpy as np
from common import change_tracking as ct
import ods_publish.etl_id as odsp
from kapo_smileys import credentials
import zipfile
import io


def csv_to_sqlite(export_file_all):
    df = pd.read_csv(export_file_all)
    # Create a SQLite database
    conn = sqlite3.connect(export_file_all.replace('all_data.csv', 'smileys.db'))
    cursor = conn.cursor()

    # Create a table with the appropriate column types
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS kapo_smileys (
        id_standort INTEGER,
        Zyklus INTEGER,
        Messung_Datum TEXT,
        Messung_Zeit TEXT,
        V_Einfahrt INTEGER,
        V_Ausfahrt INTEGER,
        Messung_Timestamp TEXT,
        V_Delta INTEGER,
        Strassenname TEXT,
        Geschwindigkeit INTEGER,
        Messung_Phase TEXT,
        Ort_Abkuerzung TEXT,
        Start_Vormessung TEXT,
        Start_Betrieb TEXT,
        Start_Nachmessung TEXT,
        Ende TEXT,
        Messung_Jahr INTEGER,
        Ort TEXT,
        geo_point_2d TEXT,
        geo_shape TEXT,
        Phase TEXT
    )
    '''

    cursor.execute(create_table_query)
    conn.commit()

    # Import the CSV data into the SQLite table
    df.to_sql('kapo_smileys', conn, if_exists='replace', index=False)
    # Upload to FTP
    common.upload_ftp(export_file_all.replace('all_data.csv', 'smileys.db'),
                      credentials.ftp_server,
                      credentials.ftp_user,
                      credentials.ftp_pass,
                      'kapo/smileys')

    index_queries = [
        "CREATE INDEX IF NOT EXISTS idx_id_standort ON kapo_smileys (id_standort);",
        "CREATE INDEX IF NOT EXISTS idx_Zyklus ON kapo_smileys (Zyklus);",
        "CREATE INDEX IF NOT EXISTS idx_Phase ON kapo_smileys (Phase);",
        "CREATE INDEX IF NOT EXISTS idx_Messung_Datum ON kapo_smileys (Messung_Datum);"
    ]

    for query in index_queries:
        cursor.execute(query)

    # Commit changes and close the connection
    conn.commit()
    conn.close()


# see https://gist.github.com/aerispaha/f098916ac041c286ae92d037ba5c37ba
def read_shapefile(shp_path):
    sf = shapefile.Reader(shp_path)
    fields = [x[0] for x in sf.fields][1:]
    records = sf.records()
    points = [s.points for s in sf.shapes()]
    df = pd.DataFrame(columns=fields, data=records)
    df = df.assign(coords=points)
    return df


def parse_messdaten(curr_dir, df_einsatz_days, df_einsaetze):
    any_changes = False
    messdaten_path = os.path.join(curr_dir, 'data_orig', 'Datenablage')
    list_path = os.path.join(curr_dir, 'data', 'list_files.txt')
    common.list_files(messdaten_path, list_path, recursive=True)
    if ct.has_changed(list_path):
        messdaten_folders = glob.glob(os.path.join(messdaten_path, '*'))
        messdaten_dfs = []
        stat_dfs = []
        for folder in messdaten_folders:
            if not any(os.listdir(folder)):
                logging.info(f'No data in folder {folder}...')
                continue
            id_standort = int(os.path.basename(folder).split('_')[0])
            if id_standort not in df_einsaetze.id_Standort.values:
                logging.warning(f'Data in the folder {folder}, but either id_standort {id_standort} '
                                'not in Einsatzplan or with non-valid values!')
                continue
            df_all_pro_standort, df_stat_pro_standort = parse_single_messdaten_folder(curr_dir, folder, df_einsatz_days,
                                                                                      df_einsaetze, id_standort)
            messdaten_dfs.append(df_all_pro_standort)
            stat_dfs.append(df_stat_pro_standort)

        all_df = pd.concat(messdaten_dfs)
        export_file_all = os.path.join(curr_dir, 'data', 'all_data.csv')
        all_df.to_csv(export_file_all, index=False)
        if ct.has_changed(export_file_all):
            common.upload_ftp(export_file_all, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass,
                              'kapo/smileys/all_data')
            any_changes = True
            csv_to_sqlite(export_file_all)
            ct.update_hash_file(export_file_all)

        stat_df = pd.concat(stat_dfs)
        export_file_stat = os.path.join(curr_dir, 'data', 'all_stat.csv')
        stat_df.to_csv(export_file_stat, index=False)
        if ct.has_changed(export_file_stat):
            common.upload_ftp(export_file_stat, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass,
                              'kapo/smileys/all_data')
            any_changes = True
            ct.update_hash_file(export_file_stat)
        ct.update_hash_file(list_path)
        return any_changes
    return False


def is_dt(datetime, timezone):
    try:
        return timezone.localize(datetime).dst() == timedelta(0)
    except (pytz.NonExistentTimeError, pytz.AmbiguousTimeError):
        return True
    except ValueError:
        return False


def parse_single_messdaten_folder(curr_dir, folder, df_einsatz_days, df_einsatze, id_standort):
    logging.info(f'Working through folder {folder}...')
    # Go recursively into folders until TXT files are found
    tagesdaten_files = glob.glob(os.path.join(folder, '**', '*.TXT'), recursive=True)
    messdaten_dfs_pro_standort = []
    for f in tagesdaten_files:
        logging.info(f'Parsing Messdaten File {f}...')
        # p = re.compile(r'Datenablage\\\\(?P<idstandort>\d+)_')
        df = (pd.read_csv(f, sep=' ', names=['Datum', 'Zeit', 'V_Einfahrt', 'dummy', 'V_Ausfahrt'], dtype=str,
                          encoding='utf-8', encoding_errors='ignore', on_bad_lines='skip')
              .rename(columns={'Datum': 'Messung_Datum', 'Zeit': 'Messung_Zeit'})
              .drop(columns=['dummy']))
        # Ignore all rows that do not have the correct format
        df = df[df.Messung_Datum.str.match(r'^(0[1-9]|[12][0-9]|3[01])\.(0[1-9]|1[0-2])\.\d{2}$')]
        df = df[df.Messung_Zeit.str.match(r'^(0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])$')]
        df = df[df.V_Einfahrt.str.match(r'^\d{3}$')]
        df = df[df.V_Ausfahrt.str.match(r'^\d{3}$')]
        df['Messung_Timestamp'] = pd.to_datetime(df.Messung_Datum + 'T' + df.Messung_Zeit, format='%d.%m.%yT%H:%M:%S')
        df['is_dt'] = df['Messung_Timestamp'].apply(lambda x: is_dt(x, pytz.timezone('Europe/Zurich')))
        df.loc[df['is_dt'], 'Messung_Timestamp'] = df['Messung_Timestamp'] - pd.Timedelta(hours=1)
        df.Messung_Timestamp = df.Messung_Timestamp.dt.tz_localize('Europe/Zurich', ambiguous=np.array(~df['is_dt']),
                                                                   nonexistent=timedelta(hours=-1))
        df.Messung_Datum = df.Messung_Timestamp.dt.date
        df.Messung_Zeit = df.Messung_Timestamp.dt.time
        df['id_standort'] = id_standort
        day_str = os.path.basename(f).split('.')[0]
        df['day_str'] = day_str
        df['V_Einfahrt'] = df.V_Einfahrt.astype(int)
        df['V_Ausfahrt'] = df.V_Ausfahrt.astype(int)
        df['V_Delta'] = df.V_Ausfahrt - df.V_Einfahrt
        # Determining Zyklus and Smiley_Nr of measurement
        df_m1 = pd.merge(df_einsatz_days, df, how='right', on=['id_standort', 'day_str']).drop(
            columns=['datum_aktiv', 'day_str'])
        df_m = pd.merge(df_m1, df_einsatze, how='left', left_on=['id_standort', 'Zyklus'],
                        right_on=['id_Standort', 'Zyklus'])
        df_m = df_m.drop(columns=['id_Standort', 'is_dt'])
        df_m['Phase'] = np.where((df_m.Messung_Timestamp < df_m.Start_Vormessung) | (df_m.Start_Vormessung.isna()),
                                 'Vor Vormessung',
                                 np.where(df_m.Messung_Timestamp < df_m.Start_Betrieb, 'Vormessung',
                                          np.where(df_m.Messung_Timestamp < df_m.Start_Nachmessung, 'Betrieb',
                                                   np.where(df_m.Messung_Timestamp < df_m.Ende, 'Nachmessung',
                                                            'Nach Ende')))
                                 )
        logging.info(f'Removing measurements with phase "Vor Vormessung"...')
        df_m = df_m[df_m.Phase != 'Vor Vormessung']

        messdaten_dfs_pro_standort.append(df_m)
        export_file_single = os.path.join(curr_dir, 'data', f'{day_str}_{id_standort}.csv')
        df_m.to_csv(export_file_single, index=False)
        if not df_m.empty:
            common.upload_ftp(export_file_single, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass,
                              f'kapo/smileys/data/zyklus{int(df_m.Zyklus.iloc[0])}')
            os.remove(export_file_single)
    df_all_pro_standort = pd.concat(messdaten_dfs_pro_standort)

    if len(df_all_pro_standort.id_standort.unique()) > 1:
        raise RuntimeError(
            f'More than 1 ({df_all_pro_standort.id_standort.unique()}) idstandort found in 1 data folder ({folder}!)')

    # Calculate statistics for this data folder
    dfs_stat_pro_standort = []
    for phase in [['Vormessung'], ['Betrieb'], ['Nachmessung'], ['Vormessung', 'Betrieb', 'Nachmessung']]:
        df_phase = df_all_pro_standort[df_all_pro_standort['Phase'].isin(phase)]
        if df_phase.empty:
            continue
        min_timestamp = df_phase.Messung_Timestamp.min()
        max_timestamp = df_phase.Messung_Timestamp.max()
        messdauer_h = (max_timestamp - min_timestamp) / pd.Timedelta(hours=1)
        anz_messungen = len(df_phase)
        dtv = anz_messungen / messdauer_h * 24
        df_all_v = pd.DataFrame(pd.concat([df_phase.V_Einfahrt, df_phase.V_Ausfahrt], ignore_index=True), columns=['V'])
        id_standort = int(df_phase.id_standort.iloc[0])
        zyklus = int(df_phase.Zyklus.iloc[0])
        strassenname = df_phase.Strassenname.iloc[0]
        geschw = df_phase.Geschwindigkeit.iloc[0]
        coord = df_phase.geo_point_2d.iloc[0]
        stat_pro_standort = {
            'Zyklus': zyklus,
            'Phase': 'Gesamt' if len(phase) > 1 else phase[0],
            'idstandort': id_standort,
            'Strassenname': strassenname,
            'Messbeginn_Phase': df_phase['Start_Vormessung'].iloc[0] if len(phase) > 1 else
            df_phase[f'Start_{phase[0]}'].iloc[0],
            'V_max': max(df_all_v.V),
            'V_min': min(df_all_v.V),
            'V_50': np.median(df_all_v.V),
            'V_85': np.percentile(df_all_v.V, 85),
            'Geschwindigkeit': geschw,
            'V_Einfahrt_pct_ueber_limite': (df_phase.V_Einfahrt > df_phase.Geschwindigkeit).mean() * 100,
            'V_Ausfahrt_pct_ueber_limite': (df_phase.V_Ausfahrt > df_phase.Geschwindigkeit).mean() * 100,
            'Anzahl_Messungen': anz_messungen,
            'Messdauer_h': messdauer_h,
            'dtv': dtv,
            'link_einzelmessungen': f'https://data.bs.ch/explore/dataset/100268/table/?refine.id_standort={id_standort}&refine.zyklus={zyklus}' if len(
                phase) > 1 else f'https://data.bs.ch/explore/dataset/100268/table/?refine.id_standort={id_standort}&refine.zyklus={zyklus}&refine.phase={phase[0]}',
            'geo_point_2d': coord
        }
        df_stat_pro_standort = pd.DataFrame([stat_pro_standort])
        dfs_stat_pro_standort.append(df_stat_pro_standort)
    df_stats_pro_standort = pd.concat(dfs_stat_pro_standort)
    return df_all_pro_standort, df_stats_pro_standort


def parse_einsatzplaene(curr_dir):
    einsatzplan_files = glob.glob(
        os.path.join(curr_dir, 'data_orig', 'Einsatzplan', 'Zyklus_[0-9][0-9]_20[0-9][0-9].xlsx'))
    einsatzplan_dfs = []
    for f in einsatzplan_files:
        # Throws error with openpyxl versions bigger than 3.0.10 since files are filtered
        df = pd.read_excel(f, parse_dates=['Datum_VM', 'Datum_SB', 'Datum_NM', 'Datum_Ende'], engine='openpyxl',
                           skiprows=1)
        df.replace(['', ' ', 'nan nan'], np.nan, inplace=True)
        df = df.dropna(subset=['Smiley-Nr.'])
        filename = os.path.basename(f)
        zyklus = int(filename.split('_')[1])
        jahr = int(filename.split('_')[2].split('.')[0])
        df['Zyklus'] = zyklus
        df['Jahr'] = jahr
        # Iterate over Phase and new column names
        for ph, col in [('VM', 'Start_Vormessung'), ('SB', 'Start_Betrieb'), ('NM', 'Start_Nachmessung'),
                        ('Ende', 'Ende')]:
            logging.info(f'Localizing timestamp in col {col}...')
            # Add time to date
            df[col] = df[f'Datum_{ph}'] + pd.to_timedelta(
                pd.to_datetime(df[f'Uhrzeit_{ph}'], format='%H:%M:%S').dt.time.astype(str))
            df.drop(columns=[f'Datum_{ph}', f'Uhrzeit_{ph}'], inplace=True)
            df['is_dt'] = df[col].apply(lambda x: is_dt(x, pytz.timezone('Europe/Zurich')))
            df.loc[df['is_dt'], col] = df[col] - pd.Timedelta(hours=1)
            df[col] = df[col].dt.tz_localize('Europe/Zurich', ambiguous='infer', nonexistent='NaT').drop(
                columns=['is_dt'])
        df = df[
            ['id_Standort', 'Strassenname', 'Geschwindigkeit', 'Halterung', 'Ort', 'Start_Vormessung', 'Start_Betrieb',
             'Start_Nachmessung', 'Ende', 'Zyklus', 'Jahr']]
        df = df.rename(columns={'Ort': 'Ort_Abkuerzung', 'Jahr': 'Messung_Jahr'})
        df['Ort'] = np.where(df.Ort_Abkuerzung == 'BS', 'Basel',
                             np.where(df.Ort_Abkuerzung == 'Bt', 'Bettingen',
                                      np.where(df.Ort_Abkuerzung == 'Rh', 'Riehen', '')))
        einsatzplan_dfs.append(df)
    df_einsaetze = pd.concat(einsatzplan_dfs)
    return df_einsaetze


def main():
    curr_dir = os.path.dirname(os.path.realpath(__file__))
    export_file_all = os.path.join(curr_dir, 'data', 'all_data.csv')
    common.upload_ftp(export_file_all.replace('all_data.csv', 'smileys.db'),
                      credentials.ftp_server,
                      credentials.ftp_user,
                      credentials.ftp_pass,
                      'kapo/smileys')
    quit()
    logging.info(f'Parsing Einsatzplaene...')
    df_einsaetze = parse_einsatzplaene(curr_dir)
    req = common.requests_get(
        'https://data.bs.ch/api/explore/v2.1/catalog/datasets/100286/exports/shp')
    shp_path = os.path.join(curr_dir, 'data', 'Smiley-Standorte')
    zip_file = io.BytesIO(req.content)
    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        zip_ref.extractall(shp_path)
    shp_coords_df = read_shapefile(os.path.join(shp_path, '100286.shp'))
    # Convert idstandort to int
    shp_coords_df['idstandort'] = shp_coords_df.idstandort.astype(int)
    df_einsaetze = pd.merge(df_einsaetze, shp_coords_df[['idstandort', 'coords']],
                            how='left', left_on='id_Standort', right_on='idstandort').drop(columns=['idstandort'])
    logging.info(f'Creating df_einsatz_days with one row per day and standort_id...')
    df_einsaetze['geo_point_2d'] = df_einsaetze[df_einsaetze['coords'].notna()]['coords'].apply(
        lambda x: f'{x[0][1]}, {x[0][0]}')
    df_einsaetze = df_einsaetze.drop(columns=['coords'])
    df_einsaetze = df_einsaetze.dropna(subset=['Start_Vormessung', 'Start_Betrieb', 'Start_Nachmessung', 'Ende'])
    df_einsatz_days = pd.concat([pd.DataFrame({'id_standort': row.id_Standort, 'Zyklus': row.Zyklus,
                                               'datum_aktiv': pd.date_range(row.Start_Vormessung, row.Ende, freq='D',
                                                                            normalize=True)})
                                 for i, row in df_einsaetze.iterrows()], ignore_index=True)
    df_einsatz_days['day_str'] = df_einsatz_days.datum_aktiv.dt.strftime('%y%m%d')
    logging.info(f'Parsing Messdaten...')
    any_changes = parse_messdaten(curr_dir, df_einsatz_days, df_einsaetze)
    if any_changes:
        odsp.publish_ods_dataset_by_id('100268')
        odsp.publish_ods_dataset_by_id('100277')
    pass


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info('Job successful!')
