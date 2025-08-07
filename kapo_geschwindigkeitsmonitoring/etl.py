import glob
import logging
import os
import sqlite3

import common
import numpy as np
import pandas as pd
import pandas.io.sql as psql
import psycopg2 as pg
from charset_normalizer import from_path
from common import change_tracking as ct
from dotenv import load_dotenv

load_dotenv()
PG_CONNECTION = os.getenv("PG_CONNECTION")
DETAIL_DATA_Q_BASE_PATH = os.getenv("DETAIL_DATA_Q_BASE_PATH")


# Add missing line breaks for lines with more than 5 columns
def fix_data(filename, measure_id, encoding):
    filename_fixed = os.path.join("data", "fixed", measure_id + os.path.basename(filename))
    # logging.info(f'Fixing data if necessary and writing to {filename_fixed}...')
    with (
        open(filename, "r", encoding=encoding) as input_file,
        open(filename_fixed, "w", encoding=encoding) as output_file,
    ):
        for i, line in enumerate(input_file):
            if len(line.split("\t")) > 5:
                wrong_value = line.split("\t")[4]
                newline_position = wrong_value.index(".") + 2
                fixed_value = wrong_value[:newline_position] + "\n" + wrong_value[newline_position:]
                line_fixed = line.replace(wrong_value, fixed_value) + "\n"
                logging.info(f"Fixed line on line {i}:")
                logging.info(f"Bad line: \n{line}")
                logging.info(f"Fixed line: \n{line_fixed}")
                output_file.write(line_fixed)
            else:
                output_file.write(line)
    return filename_fixed


def main():
    logging.info("Connecting to DB...")
    con = pg.connect(PG_CONNECTION)
    logging.info("Reading data into dataframe...")
    df_meta_raw = psql.read_sql(
        """SELECT *, ST_GeomFromText('Point(' || x_coord || ' ' || y_coord || ')', 2056) as the_geom_temp,
        ST_AsGeoJSON(ST_Transform(ST_GeomFromText('Point(' || x_coord || ' ' || y_coord || ')', 2056),4326)) as geometry,
        ST_AsEWKT(ST_GeomFromText('Point(' || x_coord || ' ' || y_coord || ')', 2056)) as the_geom_EWKT,
        ST_AsText('Point(' || x_coord || ' ' || y_coord || ')') as the_geom_WKT
        FROM projekte.geschwindigkeitsmonitoring""",
        con,
    )
    con.close()
    df_meta_raw = df_meta_raw.drop(columns=["the_geom"])
    df_meta_raw = df_meta_raw.rename(columns={"the_geom_temp": "the_geom"})

    logging.info("Calculating in dataset to put single measurements in...")
    # Ignoring the few NaN values the column "Messbeginn" has
    num_ignored = df_meta_raw[df_meta_raw["Messbeginn"].isna()].shape[0]
    logging.info(f"{num_ignored} entries ignored due to missing date!")
    df_meta_raw = df_meta_raw[df_meta_raw["Messbeginn"].notna()]
    df_meta_raw["messbeginn_jahr"] = df_meta_raw.Messbeginn.astype(str).str.slice(0, 4).astype(int)
    df_meta_raw["dataset_id"] = np.where(
        df_meta_raw["messbeginn_jahr"] < 2024,
        np.where(df_meta_raw["messbeginn_jahr"] < 2021, "100200", "100358"),
        "100097",
    )
    df_meta_raw["link_zu_einzelmessungen"] = (
        "https://data.bs.ch/explore/dataset/"
        + df_meta_raw["dataset_id"]
        + "/table/?refine.messung_id="
        + df_meta_raw["ID"].astype(str)
    )
    df_meta_raw["Verzeichnis"] = df_meta_raw["Verzeichnis"].str.replace("\\\\bs.ch\\jdolddfsroot$", "Q:")

    df_metadata = create_metadata_per_location_df(df_meta_raw)
    df_metadata_per_direction = create_metadata_per_direction_df(df_metadata)
    df_measurements = create_measurements_df(df_meta_raw, df_metadata_per_direction)
    create_measures_per_year(df_measurements)


def create_metadata_per_location_df(df):
    raw_metadata_filename = os.path.join("data", "geschwindigkeitsmonitoring_raw_metadata.csv")
    logging.info(f"Saving raw metadata (as received from db) csv and pickle to {raw_metadata_filename}...")
    df.to_csv(raw_metadata_filename, index=False)
    df.to_pickle(raw_metadata_filename.replace(".csv", ".pkl"))

    df_metadata = df[
        [
            "ID",
            "the_geom",
            "geometry",
            "Strasse",
            "Strasse_Nr",
            "Ort",
            "Geschwindigkeit",
            "Richtung_1",
            "Fzg_1",
            "V50_1",
            "V85_1",
            "Ue_Quote_1",
            "Richtung_2",
            "Fzg_2",
            "V50_2",
            "V85_2",
            "Ue_Quote_2",
            "Messbeginn",
            "Messende",
            "messbeginn_jahr",
            "dataset_id",
            "link_zu_einzelmessungen",
        ]
    ]
    df_metadata = df_metadata.rename(columns={"Geschwindigkeit": "Zone"})
    metadata_filename = os.path.join("data", "geschwindigkeitsmonitoring_metadata.csv")
    logging.info(f"Exporting processed metadata csv and pickle to {metadata_filename}...")
    df_metadata.to_csv(metadata_filename, index=False)
    df_metadata.to_pickle(metadata_filename.replace(".csv", ".pkl"))
    common.update_ftp_and_odsp(metadata_filename, "kapo/geschwindigkeitsmonitoring/metadata", "100112")
    return df_metadata


def create_metadata_per_direction_df(df_metadata):
    logging.info("Creating dataframe with one row per Messung-ID and Richtung-ID...")
    # Manual stacking of the columns for Richtung 1 and 2
    df_richtung1 = df_metadata[
        [
            "ID",
            "Richtung_1",
            "Fzg_1",
            "V50_1",
            "V85_1",
            "Ue_Quote_1",
            "the_geom",
            "geometry",
            "Strasse",
            "Strasse_Nr",
            "Ort",
            "Zone",
            "Messbeginn",
            "Messende",
        ]
    ]
    df_richtung1 = df_richtung1.rename(
        columns={
            "ID": "Messung-ID",
            "Richtung_1": "Richtung",
            "Fzg_1": "Fzg",
            "V50_1": "V50",
            "V85_1": "V85",
            "Ue_Quote_1": "Ue_Quote",
        }
    )
    df_richtung1["Richtung ID"] = 1
    df_richtung2 = df_metadata[
        [
            "ID",
            "Richtung_2",
            "Fzg_2",
            "V50_2",
            "V85_2",
            "Ue_Quote_2",
            "the_geom",
            "geometry",
            "Strasse",
            "Strasse_Nr",
            "Ort",
            "Zone",
            "Messbeginn",
            "Messende",
        ]
    ]
    df_richtung2 = df_richtung2.rename(
        columns={
            "ID": "Messung-ID",
            "Richtung_2": "Richtung",
            "Fzg_2": "Fzg",
            "V50_2": "V50",
            "V85_2": "V85",
            "Ue_Quote_2": "Ue_Quote",
        }
    )
    df_richtung2["Richtung ID"] = 2
    df_richtung = pd.concat([df_richtung1, df_richtung2])
    df_richtung = df_richtung.sort_values(by=["Messung-ID", "Richtung ID"])
    # Changing column order
    df_richtung = df_richtung[
        [
            "Messung-ID",
            "Richtung ID",
            "Richtung",
            "Fzg",
            "V50",
            "V85",
            "Ue_Quote",
            "the_geom",
            "geometry",
            "Strasse",
            "Strasse_Nr",
            "Ort",
            "Zone",
            "Messbeginn",
            "Messende",
        ]
    ]
    richtung_filename = os.path.join("data", "geschwindigkeitsmonitoring_richtung.csv")
    logging.info(f"Exporting richtung csv and pickle data to {richtung_filename}...")
    df_richtung.to_csv(richtung_filename, index=False)
    df_richtung.to_pickle(richtung_filename.replace(".csv", ".pkl"))
    common.update_ftp_and_odsp(richtung_filename, "kapo/geschwindigkeitsmonitoring/metadata", "100115")
    return df_richtung


def create_measurements_df(df_meta_raw, df_metadata_per_direction):
    dfs = []
    files_to_upload = []
    logging.info("Removing metadata without data...")
    df_meta_raw = df_meta_raw.dropna(subset=["Verzeichnis"])

    db_filename = os.path.join("data", "datasette", "Geschwindigkeitsmonitoring.db")
    table_name_direction = "Kennzahlen_pro_Richtung"
    table_name = "Einzelmessungen"
    # Columns to index
    columns_to_index_direction = ["Messung-ID", "Richtung ID", "Messbeginn", "Messende", "Zone", "Ort", "Strasse"]
    columns_to_index = ["Timestamp", "Richtung ID"]
    logging.info(f"Creating SQLite connection for {db_filename}...")
    conn = sqlite3.connect(db_filename)
    cursor = conn.cursor()
    logging.info(f"Creating table {table_name_direction} with proper schema...")
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {table_name_direction} (
        "Messung-ID" INTEGER,
        "Richtung ID" INTEGER,
        "Richtung" TEXT,
        "Fzg" INTEGER,
        "V50" INTEGER,
        "V85" INTEGER,
        "Ue_Quote" REAL,
        "geometry" TEXT,
        "Strasse" TEXT,
        "Strasse_Nr" TEXT,
        "Ort" TEXT,
        "Zone" INTEGER,
        "Messbeginn" TEXT,
        "Messende" TEXT
    )
    """)
    cursor.execute(f"DELETE FROM {table_name_direction}")
    conn.commit()

    # Append data, dropping geometry if Spatialite isn't used
    df_metadata_per_direction.drop(columns=["the_geom"], errors="ignore").to_sql(
        name=table_name_direction,
        con=conn,
        if_exists="append",
        index=False
    )
    common.create_indices(conn, table_name_direction, columns_to_index_direction)

    logging.info(f"Creating table {table_name} with proper schema...")
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        Geschwindigkeit INTEGER,
        Zeit TEXT,
        Datum TEXT,
        "Richtung ID" INTEGER,
        Fahrzeuglänge REAL,
        "Messung-ID" INTEGER,
        Datum_Zeit TEXT,
        Timestamp TEXT
    )
    """)
    cursor.execute(f"DELETE FROM {table_name}")
    conn.commit()
    # Drop geometry column since it is not needed anymore
    df_metadata_per_direction = df_metadata_per_direction.drop(columns=["geometry"])
    for index, row in df_meta_raw.iterrows():
        logging.info(f"Processing row {index + 1} of {len(df_meta_raw)}...")
        measure_id = row["ID"]
        metadata_file_path = (
            "data_orig" + os.sep + row.Verzeichnis.replace("\\", os.sep).replace(DETAIL_DATA_Q_BASE_PATH, "")
        )
        data_search_string = os.path.join(metadata_file_path, "**/*.txt")
        raw_files = glob.glob(data_search_string, recursive=True)

        if len(raw_files) == 0:
            logging.info(f"No raw files found for measurement ID {measure_id}!")
            continue
        elif len(raw_files) > 2:
            logging.info(f"More than 2 raw files found for measurement ID {measure_id}!")

        for i, file in enumerate(raw_files):
            file = file.replace("\\", "/")
            filename_current_measure = os.path.join("data", "processed", f"{str(measure_id)}_{i}.csv")
            result = from_path(file)
            enc = result.best().encoding
            logging.info(f"Fixing errors and reading data into dataframe from {file}...")
            raw_df = pd.read_table(
                fix_data(filename=file, measure_id=str(measure_id), encoding=enc),
                skiprows=6,
                header=0,
                encoding=enc,
                names=["Geschwindigkeit", "Zeit", "Datum", "Richtung ID", "Fahrzeuglänge"],
                on_bad_lines="skip",
            )

            if raw_df.empty:
                logging.info("Dataframe is empty, ignoring...")
                continue

            raw_df["Messung-ID"] = measure_id
            logging.info("Calculating timestamp...")
            raw_df["Datum_Zeit"] = raw_df["Datum"] + " " + raw_df["Zeit"]
            raw_df["Timestamp"] = pd.to_datetime(raw_df["Datum_Zeit"], format="%d.%m.%y %H:%M:%S").dt.tz_localize(
                "Europe/Zurich", ambiguous=True, nonexistent="shift_forward"
            )
            logging.info(f"Appending data to SQLite table {table_name} and to list dfs...")
            raw_df.to_sql(name=table_name, con=conn, if_exists="append", index=False)
            raw_df = raw_df.merge(df_metadata_per_direction, "left", ["Messung-ID", "Richtung ID"])

            # Timestamp has to be between Messbeginn and Messende
            num_rows_before = raw_df.shape[0]
            raw_df = raw_df[
                (
                    raw_df["Timestamp"].dt.floor("D")
                    >= pd.to_datetime(raw_df["Messbeginn"])
                    .dt.tz_localize("Europe/Zurich", ambiguous=True, nonexistent="shift_forward")
                    .dt.floor("D")
                )
                & (
                    raw_df["Timestamp"].dt.floor("D")
                    <= pd.to_datetime(raw_df["Messende"])
                    .dt.tz_localize("Europe/Zurich", ambiguous=True, nonexistent="shift_forward")
                    .dt.floor("D")
                )
            ]
            logging.info(
                f"Filtered out {num_rows_before - raw_df.shape[0]} rows "
                f"due to timestamp not being between Messbeginn and Messende..."
            )

            dfs.append(raw_df)

            logging.info(f"Exporting data file for current measurement to {filename_current_measure}")
            raw_df.to_csv(filename_current_measure, index=False)
            files_to_upload.append({"filename": filename_current_measure, "dataset_id": row["dataset_id"]})

    for obj in files_to_upload:
        if ct.has_changed(filename=obj["filename"], method="hash"):
            remote_path = f"{'kapo/geschwindigkeitsmonitoring/data_partitioned'}/{obj['dataset_id']}"
            common.upload_ftp(filename=obj["filename"], remote_path=remote_path)
            ct.update_hash_file(obj["filename"])

    common.create_indices(conn, table_name, columns_to_index)
    conn.close()
    if ct.has_changed(filename=db_filename, method="hash"):
        common.upload_ftp(db_filename, remote_path="kapo/geschwindigkeitsmonitoring")
        ct.update_hash_file(db_filename)

    all_df = pd.concat(dfs)
    pkl_filename = os.path.join("data", "geschwindigkeitsmonitoring_data.pkl")
    all_df.to_pickle(pkl_filename)

    logging.info(f"All data processed and saved to {db_filename} and {pkl_filename}...")
    if ct.has_changed(filename=pkl_filename, method="hash"):
        common.upload_ftp(db_filename, remote_path="kapo/geschwindigkeitsmonitoring")
        common.publish_ods_dataset_by_id("100097")
        common.publish_ods_dataset_by_id("100200")
        common.publish_ods_dataset_by_id("100358")
        ct.update_hash_file(pkl_filename)

    return all_df


def create_measures_per_year(df_all):
    # Create a separate data file per year
    df_all["messbeginn_jahr"] = df_all.Messbeginn.astype(str).str.slice(0, 4).astype(int)
    for year_value, year_df in df_all.groupby("messbeginn_jahr"):
        # CSV
        current_filename = os.path.join("data", f"geschwindigkeitsmonitoring_{str(year_value)}.csv")
        logging.info(f"Saving year {year_value} into {current_filename}...")
        year_df.to_csv(current_filename, index=False)
        common.upload_ftp(filename=current_filename, remote_path="kapo/geschwindigkeitsmonitoring/all_data")
        os.remove(current_filename)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f"Executing {__file__}...")
    main()
    logging.info("Job successful!")
