import gc
import glob
import logging
import os
import sqlite3
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import common
import pandas as pd
import pandas.io.sql as psql
import psycopg2 as pg
from charset_normalizer import from_path
from common import change_tracking as ct
from dotenv import load_dotenv

load_dotenv()
PG_CONNECTION = os.getenv("PG_CONNECTION")
DETAIL_DATA_Q_BASE_PATH = os.getenv("DETAIL_DATA_Q_BASE_PATH")

TZ = ZoneInfo("Europe/Zurich")
TODAY = datetime.now(TZ)
YEARS_TO_PUBLISH = {TODAY.year, TODAY.year - 1}


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
    df_meta_raw["messbeginn_jahr"] = df_meta_raw["Messbeginn"].astype(str).str.slice(0, 4).astype(int)

    df_meta_raw["link_zu_einzelmessungen"] = (
        "https://datatools.bs.ch/Geschwindigkeitsmonitoring/Einzelmessungen"
        + "?Messung-ID__exact="
        + df_meta_raw["ID"].astype(str)
        + "&_sort_desc=Timestamp"
    )
    df_meta_raw["Verzeichnis"] = df_meta_raw["Verzeichnis"].str.replace("\\\\bs.ch\\jdolddfsroot$", "Q:")

    df_metadata = create_metadata_per_location_df(df_meta_raw)
    df_metadata_per_direction = create_metadata_per_direction_df(df_metadata)
    df_measurements = create_measurements_df(df_meta_raw, df_metadata_per_direction, df_metadata)
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


def create_measurements_df(df_meta_raw, df_metadata_per_direction, df_metadata_per_location):
    dfs = []
    files_to_upload_partitioned = []
    files_to_upload = []
    logging.info("Removing metadata without data...")
    df_meta_raw = df_meta_raw.dropna(subset=["Verzeichnis"])

    db_filename = os.path.join("data", "datasette", "Geschwindigkeitsmonitoring.db")
    table_name_location = "Kennzahlen_pro_Standort"
    table_name = "Einzelmessungen"
    # Columns to index
    columns_to_index_location = [
        "Messung-ID",
        "Messbeginn",
        "Messende",
        "Zone",
        "Ort",
        "Strasse",
        "messbeginn_jahr",
        "link_zu_einzelmessungen",
    ]
    columns_to_index = ["Timestamp", "Richtung ID", "Messung-ID", "Datum", "Geschwindigkeit"]
    logging.info(f"Creating SQLite connection for {db_filename}...")
    conn = sqlite3.connect(db_filename)
    cursor = conn.cursor()
    cursor.execute("PRAGMA foreign_keys = ON")
    logging.info(f"Creating table {table_name_location} with proper schema (per Standort)...")
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {table_name_location} (
        "ID" INTEGER PRIMARY KEY,
        "Strasse" TEXT,
        "Strasse_Nr" TEXT,
        "Ort" TEXT,
        "Zone" INTEGER,
        "Richtung_1" TEXT,
        "Fzg_1" INTEGER,
        "V50_1" INTEGER,
        "V85_1" INTEGER,
        "Ue_Quote_1" REAL,
        "Richtung_2" TEXT,
        "Fzg_2" INTEGER,
        "V50_2" INTEGER,
        "V85_2" INTEGER,
        "Ue_Quote_2" REAL,
        "geometry" TEXT,
        "Messbeginn" TEXT,
        "Messende" TEXT,
        "messbeginn_jahr" INTEGER,
        "link_zu_einzelmessungen" TEXT
    )
    """)
    cursor.execute(f"DELETE FROM {table_name_location}")
    conn.commit()
    # Append data, dropping geometry if Spatialite isn't used
    df_metadata_per_location.drop(columns=["the_geom"], errors="ignore").to_sql(
        name=table_name_location, con=conn, if_exists="append", index=False
    )
    common.create_indices(conn, table_name_location, columns_to_index_location)

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
        Timestamp TEXT,
        FOREIGN KEY("Messung-ID") REFERENCES {table_name_location}("ID")
            ON UPDATE CASCADE ON DELETE CASCADE
    )
    """)
    cursor.execute(f"DELETE FROM {table_name}")
    conn.commit()

    # Drop geometry column since it is not needed anymore
    df_metadata_per_direction = df_metadata_per_direction.drop(columns=["geometry"])

    # On Jan 1, clear the folder that holds per-measurement CSVs for 100097
    if TODAY.month == 1 and TODAY.day == 1:
        remote_year_folder = "kapo/geschwindigkeitsmonitoring/data_partitioned"
        common.delete_dir_content_ftp(common.FTP_SERVER, common.FTP_USER, common.FTP_PASSWORD, remote_year_folder)

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

        # collect parts for this measure_id
        measure_parts = []

        for i, file in enumerate(raw_files):
            file = file.replace("\\", "/")
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

            logging.info(f"Appending data to SQLite table {table_name}...")
            raw_df.to_sql(name=table_name, con=conn, if_exists="append", index=False)
            part_df = raw_df.merge(df_metadata_per_direction, "left", ["Messung-ID", "Richtung ID"])

            num_rows_before = part_df.shape[0]
            part_df = part_df[
                (
                    part_df["Timestamp"].dt.floor("D")
                    >= pd.to_datetime(part_df["Messbeginn"])
                    .dt.tz_localize("Europe/Zurich", ambiguous=True, nonexistent="shift_forward")
                    .dt.floor("D")
                )
                & (
                    part_df["Timestamp"].dt.floor("D")
                    <= pd.to_datetime(part_df["Messende"])
                    .dt.tz_localize("Europe/Zurich", ambiguous=True, nonexistent="shift_forward")
                    .dt.floor("D")
                )
            ]
            logging.info(
                f"Filtered out {num_rows_before - part_df.shape[0]} rows "
                f"due to timestamp not being between Messbeginn and Messende..."
            )

            measure_parts.append(part_df)
            dfs.append(part_df)

        # after processing all files for this measure_id, write ONE CSV
        if measure_parts:
            measure_df = pd.concat(measure_parts, ignore_index=True)
            filename_current_measure = os.path.join("data", "processed", f"{str(measure_id)}.csv")
            logging.info(f"Exporting concatenated data for measurement {measure_id} to {filename_current_measure}")
            measure_df.to_csv(filename_current_measure, index=False)
            files_to_upload.append(filename_current_measure)
            # Only upload current & previous year, always to dataset 100097
            year_val = int(str(row["Messbeginn"])[:4])
            if year_val in YEARS_TO_PUBLISH:
                files_to_upload_partitioned.append(filename_current_measure)

    for file in files_to_upload_partitioned:
        if ct.has_changed(filename=file, method="hash"):
            remote_path = "kapo/geschwindigkeitsmonitoring/data_partitioned"
            common.upload_ftp(filename=file, remote_path=remote_path)
    for file in files_to_upload:
        if ct.has_changed(filename=file, method="hash"):
            remote_path = "kapo/geschwindigkeitsmonitoring/data"
            common.upload_ftp(filename=file, remote_path=remote_path)
            ct.update_hash_file(file)

    common.create_indices(conn, table_name, columns_to_index)
    conn.close()

    all_df = pd.concat(dfs)
    pkl_filename = os.path.join("data", "geschwindigkeitsmonitoring_data.pkl")
    all_df.to_pickle(pkl_filename)
    csv_filename = os.path.join("data", "geschwindigkeitsmonitoring_data.csv")
    all_df.to_csv(csv_filename, index=False)

    logging.info(f"All data processed and saved to {db_filename} and {pkl_filename}...")
    if ct.has_changed(filename=pkl_filename, method="hash"):
        common.upload_ftp(filename=csv_filename, remote_path="kapo/geschwindigkeitsmonitoring/all_data")
        common.publish_ods_dataset_by_id("100097")
        ct.update_hash_file(pkl_filename)

    return all_df


def create_measures_per_year(df_all, chunk_size=200_000, years=None, dedupe_subset=None):
    """
    Stream df_all into one CSV per year without copying the whole frame.
    """
    outdir = Path("data")
    outdir.mkdir(parents=True, exist_ok=True)

    cols = list(df_all.columns)  # keep stable column order
    wrote_header = set()
    totals = {}

    n = len(df_all)
    logging.info(f"[per-year] start: {n:,} rows total")
    if n == 0:
        logging.info("[per-year] nothing to do")
        return

    for start in range(0, n, chunk_size):
        end = min(start + chunk_size, n)
        part = df_all.iloc[start:end]

        years_in_chunk = pd.to_datetime(part["Messbeginn"], errors="coerce").dt.year

        if years is not None:
            mask = years_in_chunk.isin(years)
            if not mask.any():
                logging.info(f"[per-year] chunk {start:,}-{end - 1:,}: 0 rows match target years; skip")
                del part
                gc.collect()
                continue
            part = part.loc[mask]
            years_in_chunk = years_in_chunk.loc[mask]

        if dedupe_subset:
            missing = [c for c in dedupe_subset if c not in part.columns]
            if missing:
                logging.warning(f"[per-year] dedupe columns missing: {missing}; skipping de-dup in this chunk")
            else:
                before = len(part)
                part = part.drop_duplicates(subset=dedupe_subset, keep="first")
                after = len(part)
                if after != before:
                    logging.info(f"[per-year] chunk {start:,}-{end - 1:,}: dropped {before - after:,} dups")

        for y, sub in part.groupby(years_in_chunk):
            if pd.isna(y):
                logging.info("[per-year] found NaN year; skipping those rows")
                continue
            y = int(y)
            fname = outdir / f"geschwindigkeitsmonitoring_{y}.csv"
            write_header = y not in wrote_header and not fname.exists()

            sub = sub.loc[:, cols]
            sub.to_csv(
                fname,
                index=False,
                mode="a",
                header=write_header,
                lineterminator="\n",
            )
            wrote_header.add(y)
            totals[y] = totals.get(y, 0) + len(sub)
            logging.info(f"[per-year] wrote {len(sub):,} rows to {fname.name} (year {y}, total {totals[y]:,})")

        del part
        gc.collect()

    for y in sorted(totals):
        fname = outdir / f"geschwindigkeitsmonitoring_{y}.csv"
        size_mb = fname.stat().st_size / 1_048_576
        logging.info(f"[per-year] uploading {fname.name}: {size_mb:.2f} MB, {totals[y]:,} rows")
        common.upload_ftp(filename=str(fname), remote_path="kapo/geschwindigkeitsmonitoring/all_data")
        os.remove(fname)
        logging.info(f"[per-year] removed local {fname.name}")

    logging.info("[per-year] done")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f"Executing {__file__}...")
    main()
    logging.info("Job successful!")
