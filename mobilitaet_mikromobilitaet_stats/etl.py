import glob
import io
import json
import logging
import numbers
import os
import re
import shutil
import sqlite3
import tempfile
import zipfile
from datetime import date, datetime

import common
import geopandas as gpd
import pandas as pd
from common import FTP_PASS, FTP_SERVER, FTP_USER
from dateutil.relativedelta import relativedelta
from shapely import wkt
from shapely.geometry import mapping

CONFIGS = {
    "bezirke": {
        "ods_id_shapes": "100039",
        "ods_id": "100416",
        "fill_empty_polygon_column": "gemeinde_na",
        "group_cols": [
            "xs_provider_name",
            "xs_vehicle_type_name",
            "xs_form_factor",
            "xs_propulsion_type",
            "xs_max_range_meters",
            "bez_id",
        ],
        "output_cols": [
            "date",
            "bez_id",
            "bez_name",
            "wov_id",
            "wov_name",
            "gemeinde_na",
            "geometry",
            "xs_provider_name",
            "xs_vehicle_type_name",
            "xs_form_factor",
            "xs_propulsion_type",
            "xs_max_range_meters",
            "num_measures",
            "mean",
            "min",
            "max",
        ],
    },
    "gemeinden": {
        "ods_id_shapes": "100017",
        "ods_id": "100422",
        "fill_empty_polygon_column": None,
        "group_cols": ["xs_provider_name", "objid"],
        "output_cols": [
            "date",
            "objid",
            "name",
            "geometry",
            "xs_provider_name",
            "num_measures",
            "mean",
            "min",
            "max",
        ],
    },
}

MONTHLY_CONFIGS = {
    "bezirke": {
        "ods_id_shapes": "100039",
        "ods_id": "100428",
        "fill_empty_polygon_column": "gemeinde_na",
        "group_cols": [
            "xs_provider_name",
            "xs_vehicle_type_name",
            "xs_form_factor",
            "xs_propulsion_type",
            "xs_max_range_meters",
            "bez_id",
        ],
        "output_cols": [
            "date",
            "weekday",
            "timerange_start",
            "timerange_end",
            "bez_id",
            "bez_name",
            "wov_id",
            "wov_name",
            "gemeinde_na",
            "geometry",
            "xs_provider_name",
            "xs_vehicle_type_name",
            "xs_form_factor",
            "xs_propulsion_type",
            "xs_max_range_meters",
            "num_measures",
            "mean",
            "min",
            "max",
        ],
    }
}

DATASETTE_DIR = os.path.join("data", "datasette")
DB_100416 = os.path.join(DATASETTE_DIR, "Mikromobilitaet_Bezirke.db")
TABLE_100416 = "bezirke_daily"
DB_100428 = os.path.join(DATASETTE_DIR, "Mikromobilitaet_Wochentage.db")
TABLE_100428 = "bezirke_timerange"
TABLE_BEZIRKE = "bezirke"
VIEW_DAILY = "View_bezirke_daily"
VIEW_TIMERANGE = "View_bezirke_timerange"
BEZIRKE_ATTR_COLS = ["bez_id", "bez_name", "wov_id", "wov_name", "gemeinde_na"]
BEZIRKE_DIM_ONLY_COLS = ["bez_name", "wov_id", "wov_name", "gemeinde_na"]
STRING_ID_COLS = ["bez_id", "wov_id"]
ROLLING_416_PATH = os.path.join("data", "bezirke_stats_rolling.csv")
ROLLING_428_PATH = os.path.join("data", "bezirke_timerange_stats_rolling.csv")
ODS_SIZE_LIMIT_MB = 240


def _ods_export_cols(output_cols):
    """Huwise/ODS export: no geometry (full geometries stay in SQLite for Datasette)."""
    return [c for c in output_cols if c != "geometry"]


DEDUPE_COLS_416 = [
    "date",
    "bez_id",
    "xs_provider_name",
    "xs_vehicle_type_name",
    "xs_form_factor",
    "xs_propulsion_type",
    "xs_max_range_meters",
]
DEDUPE_COLS_428 = DEDUPE_COLS_416 + ["weekday", "timerange_start", "timerange_end"]

INDEX_COLS_416 = DEDUPE_COLS_416
INDEX_COLS_428 = DEDUPE_COLS_428

_DATASETTE_WORK_DIR = None


def _datasette_work_path(final_db_path):
    """Local temp copy path while the job runs (avoids SQLite locks on mounted storage)."""
    if _DATASETTE_WORK_DIR is None:
        return final_db_path
    return os.path.join(_DATASETTE_WORK_DIR, os.path.basename(final_db_path))


def init_datasette_workdir():
    """Copy existing DBs from mounted storage into a local temp dir for this run."""
    global _DATASETTE_WORK_DIR
    _DATASETTE_WORK_DIR = tempfile.mkdtemp(prefix="mikromobilitaet_datasette_")
    os.makedirs(DATASETTE_DIR, exist_ok=True)
    for final_path in (DB_100416, DB_100428):
        if os.path.exists(final_path):
            shutil.copy2(final_path, _datasette_work_path(final_path))
            logging.info(f"Copied {final_path} to local work copy for SQLite updates")


def commit_datasette_workdir():
    """Copy updated DBs from the local work copy back to mounted data/datasette."""
    if _DATASETTE_WORK_DIR is None:
        return
    os.makedirs(DATASETTE_DIR, exist_ok=True)
    for final_path in (DB_100416, DB_100428):
        work_path = _datasette_work_path(final_path)
        if os.path.exists(work_path):
            shutil.copy2(work_path, final_path)
            logging.info(f"Copied SQLite DB to {final_path}")


def _cleanup_datasette_workdir():
    global _DATASETTE_WORK_DIR
    if _DATASETTE_WORK_DIR is not None:
        shutil.rmtree(_DATASETTE_WORK_DIR, ignore_errors=True)
        _DATASETTE_WORK_DIR = None


_DAILY_BEZIRKE_CSV = re.compile(r"^bezirke_stats_\d{4}-\d{2}-\d{2}\.csv$")


def rolling_window_start(today=None) -> date:
    """First day of the month two months before the current month (3 calendar months window)."""
    today = today or datetime.now().date()
    first_of_current = today.replace(day=1)
    return first_of_current - relativedelta(months=2)


def _geometry_to_geojson(value):
    """GeoJSON in WGS84 for Datasette maps (same pattern as kapo_smileys / geschwindigkeitsmonitoring)."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, str):
        value = value.strip()
        if not value or value.lower() == "none":
            return None
        if value.startswith("{"):
            return value
        geom = wkt.loads(value)
    else:
        geom = value
    geom_wgs84 = gpd.GeoSeries([geom], crs="EPSG:2056").to_crs("EPSG:4326").iloc[0]
    if geom_wgs84 is None or geom_wgs84.is_empty:
        return None
    return json.dumps(mapping(geom_wgs84))


def _fact_columns(output_cols):
    exclude = set(BEZIRKE_DIM_ONLY_COLS) | {"geometry"}
    return [c for c in output_cols if c not in exclude]


def _format_id_value(value):
    """District/ward IDs must be strings in Datasette and Huwise (not float/double)."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    if isinstance(value, numbers.Integral) and not isinstance(value, bool):
        return str(value)
    text = str(value).strip()
    if text.endswith(".0"):
        try:
            return str(int(float(text)))
        except ValueError:
            pass
    return text


def _cast_id_columns(df):
    df = df.copy()
    for col in STRING_ID_COLS:
        if col in df.columns:
            df[col] = df[col].map(_format_id_value)
    return df


def _ensure_id_columns_text_in_table(conn, table_name):
    """Migrate existing rows so ID columns are stored as TEXT (not INTEGER/REAL)."""
    if not _table_exists(conn, table_name):
        return
    if not any(_table_has_column(conn, table_name, col) for col in STRING_ID_COLS):
        return
    df = pd.read_sql_query(f'SELECT * FROM "{table_name}"', conn)
    if df.empty:
        return
    df = _cast_id_columns(df)
    df.to_sql(table_name, conn, if_exists="replace", index=False)


def _prepare_sqlite_frames(df_stats, output_cols):
    """Split stats into bezirke dimension (GeoJSON geometry) and fact rows without geometry."""
    df = _cast_id_columns(df_stats.copy())
    dim_cols = [c for c in BEZIRKE_ATTR_COLS + ["geometry"] if c in df.columns]
    df_bezirke = df[dim_cols].drop_duplicates(subset=["bez_id"]).copy()
    df_bezirke["geometry"] = df_bezirke["geometry"].apply(_geometry_to_geojson)

    fact_cols = [c for c in _fact_columns(output_cols) if c in df.columns]
    df_facts = df[fact_cols].copy()
    return df_bezirke, df_facts


def _table_exists(conn, table_name):
    cur = conn.cursor()
    cur.execute(
        "SELECT 1 FROM sqlite_master WHERE type IN ('table', 'view') AND name = ?",
        (table_name,),
    )
    return cur.fetchone() is not None


def _table_has_column(conn, table_name, column):
    cur = conn.cursor()
    cur.execute(f'PRAGMA table_info("{table_name}")')
    return column in [row[1] for row in cur.fetchall()]


def _upsert_bezirke(conn, df_bezirke):
    if df_bezirke.empty:
        return
    conn.execute(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_BEZIRKE} (
        bez_id TEXT PRIMARY KEY,
        bez_name TEXT,
        wov_id TEXT,
        wov_name TEXT,
        gemeinde_na TEXT,
        geometry TEXT
    )
    """)
    df_bezirke = _cast_id_columns(df_bezirke)
    if _table_exists(conn, TABLE_BEZIRKE):
        existing = pd.read_sql_query(f'SELECT * FROM "{TABLE_BEZIRKE}"', conn)
        existing = _cast_id_columns(existing)
        combined = pd.concat([existing, df_bezirke], ignore_index=True)
    else:
        combined = df_bezirke
    combined = combined.drop_duplicates(subset=["bez_id"], keep="last")
    combined.to_sql(TABLE_BEZIRKE, conn, if_exists="replace", index=False)


def _migrate_legacy_flat_table(conn, facts_table, output_cols):
    """One-time migration from denormalized table with WKT geometry to bezirke + facts."""
    if not _table_exists(conn, facts_table) or not _table_has_column(conn, facts_table, "geometry"):
        return
    if _table_exists(conn, TABLE_BEZIRKE):
        return
    logging.info(f"Migrating legacy flat table {facts_table} to normalized schema...")
    df = pd.read_sql_query(f'SELECT * FROM "{facts_table}"', conn)
    df_bezirke, df_facts = _prepare_sqlite_frames(df, output_cols)
    _upsert_bezirke(conn, df_bezirke)
    df_facts.to_sql(facts_table, conn, if_exists="replace", index=False)
    conn.commit()


def _table_row_count(db_path, table_name):
    work_path = _datasette_work_path(db_path)
    if not os.path.exists(work_path):
        return 0
    with sqlite3.connect(work_path, timeout=60) as conn:
        try:
            cur = conn.cursor()
            cur.execute(f'SELECT COUNT(1) FROM "{table_name}"')
            return cur.fetchone()[0]
        except sqlite3.OperationalError:
            return 0


def append_stats_to_sqlite(df_stats, db_path, table_name, dedupe_cols, output_cols):
    if df_stats.empty:
        return

    df_bezirke, df_facts = _prepare_sqlite_frames(df_stats, output_cols)
    work_path = _datasette_work_path(db_path)
    os.makedirs(os.path.dirname(work_path) or ".", exist_ok=True)
    with sqlite3.connect(work_path, timeout=60) as conn:
        _migrate_legacy_flat_table(conn, table_name, output_cols)
        _upsert_bezirke(conn, df_bezirke)
        df_facts.to_sql(table_name, conn, if_exists="append", index=False)
        _ensure_id_columns_text_in_table(conn, TABLE_BEZIRKE)
        df_all = pd.read_sql_query(f'SELECT * FROM "{table_name}"', conn)
        df_all = df_all.drop_duplicates(subset=dedupe_cols, keep="last")
        df_all = _cast_id_columns(df_all)
        df_all.to_sql(table_name, conn, if_exists="replace", index=False)
        conn.commit()
    logging.info(f"SQLite {table_name}: {len(df_all)} fact rows, {len(df_bezirke)} bezirke update(s) in {work_path}")


def _filter_by_rolling_window(df, is_monthly):
    """Keep rows in the rolling window; daily and monthly tables use different date formats."""
    window_start = rolling_window_start()
    dates = df["date"].astype(str)

    if is_monthly:
        monthly = dates.str.match(r"^\d{4}-\d{2}$", na=False)
        skipped = (~monthly).sum()
        if skipped:
            logging.warning(f"Dropping {skipped} row(s) with non-monthly date format from timerange export")
        df = df.loc[monthly]
        if df.empty:
            return df
        window_period = pd.Period(window_start.strftime("%Y-%m"), freq="M")
        periods = pd.PeriodIndex(df["date"], freq="M")
        return df.loc[periods >= window_period]

    daily = dates.str.match(r"^\d{4}-\d{2}-\d{2}$", na=False)
    skipped = (~daily).sum()
    if skipped:
        logging.warning(f"Dropping {skipped} row(s) with non-daily date format from bezirke export")
    df = df.loc[daily]
    if df.empty:
        return df
    parsed = pd.to_datetime(df["date"], format="%Y-%m-%d", errors="coerce")
    return df.loc[parsed.dt.date >= window_start]


def _read_facts_with_bezirke_attrs(conn, facts_table):
    """Join fact rows to bezirke dimension for Huwise exports (names without geometry)."""
    dim_select = ", ".join(f"b.{c}" for c in BEZIRKE_DIM_ONLY_COLS)
    if _table_exists(conn, TABLE_BEZIRKE):
        query = f"""
        SELECT f.*, {dim_select}
        FROM "{facts_table}" f
        LEFT JOIN {TABLE_BEZIRKE} b ON f.bez_id = b.bez_id
        """
    else:
        query = f'SELECT * FROM "{facts_table}"'
    return pd.read_sql_query(query, conn)


def build_rolling_export(db_path, table_name, output_path, output_cols, is_monthly=False):
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    work_path = _datasette_work_path(db_path)
    if not os.path.exists(work_path):
        logging.warning(f"No SQLite database at {work_path}; writing empty rolling export.")
        pd.DataFrame(columns=output_cols).to_csv(output_path, index=False, encoding="utf-8")
        return

    with sqlite3.connect(work_path, timeout=60) as conn:
        try:
            df = _read_facts_with_bezirke_attrs(conn, table_name)
        except sqlite3.OperationalError:
            df = pd.DataFrame()

    if df.empty:
        pd.DataFrame(columns=output_cols).to_csv(output_path, index=False, encoding="utf-8")
        logging.info(f"Rolling export {output_path}: 0 rows (empty table)")
        return

    window_start = rolling_window_start()
    df = _filter_by_rolling_window(df, is_monthly=is_monthly)
    df = _cast_id_columns(df)
    export_cols = [c for c in output_cols if c in df.columns]
    missing = [c for c in output_cols if c not in df.columns and c != "geometry"]
    if missing:
        logging.warning(f"Rolling export missing columns (not in DB): {missing}")
    df = df[export_cols]
    df.to_csv(output_path, index=False, encoding="utf-8")
    logging.info(f"Rolling export {output_path}: {len(df)} rows (from {window_start})")


def _zip_csv_for_ods(csv_path):
    """Zip rolling CSV for ODS upload (same approach as kapo_smileys)."""
    zip_path = csv_path + ".zip"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(csv_path, os.path.basename(csv_path))
    csv_mb = os.path.getsize(csv_path) / (1024 * 1024)
    zip_mb = os.path.getsize(zip_path) / (1024 * 1024)
    logging.info(f"File {csv_path} size is {csv_mb:.2f} MB")
    logging.info(f"Created compressed file: {zip_path}")
    if csv_mb > 0:
        logging.info(f"Compressed file size: {zip_mb:.2f} MB (compression ratio: {zip_mb / csv_mb * 100:.1f}%)")
    else:
        logging.info(f"Compressed file size: {zip_mb:.2f} MB")
    if zip_mb > ODS_SIZE_LIMIT_MB:
        logging.warning(
            f"Compressed file {zip_path} exceeds the OpenDataSoft {ODS_SIZE_LIMIT_MB} MB limit! "
            "Full history remains in data/datasette SQLite."
        )
        logging.warning("See https://userguide.opendatasoft.com/en/articles/2248706 for more information.")
    return zip_path


def _publish_ods_zip(csv_path, ftp_folder, dataset_id):
    if not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0:
        logging.warning(f"Skipping ODS {dataset_id} publish: {csv_path} missing or empty")
        return
    zip_path = _zip_csv_for_ods(csv_path)
    logging.info(f"Publishing {zip_path} for ODS {dataset_id}...")
    common.update_ftp_and_odsp(zip_path, ftp_folder, dataset_id)


def _create_datasette_view(conn, view_name, facts_table, fact_metric_cols):
    """Join facts to bezirke so Datasette can render geometry (kapo_smileys pattern)."""
    f_select = ", ".join(f'f."{c}"' if c == "date" else f"f.{c}" for c in fact_metric_cols)
    b_select = ", ".join(f"b.{c}" for c in BEZIRKE_ATTR_COLS if c != "bez_id") + ", b.geometry"
    conn.execute(f"DROP VIEW IF EXISTS {view_name}")
    conn.execute(f"""
    CREATE VIEW {view_name} AS
    SELECT {f_select}, {b_select}
    FROM "{facts_table}" f
    LEFT JOIN {TABLE_BEZIRKE} b ON f.bez_id = b.bez_id
    """)
    conn.commit()


def finalize_datasette_db(db_path, facts_table, dedupe_cols, index_cols, output_cols, view_name):
    work_path = _datasette_work_path(db_path)
    if not os.path.exists(work_path):
        return
    fact_metric_cols = _fact_columns(output_cols)
    with sqlite3.connect(work_path, timeout=60) as conn:
        _migrate_legacy_flat_table(conn, facts_table, output_cols)
        _ensure_id_columns_text_in_table(conn, TABLE_BEZIRKE)
        _ensure_id_columns_text_in_table(conn, facts_table)
        common.create_indices(conn, facts_table, index_cols)
        common.create_indices(conn, TABLE_BEZIRKE, ["bez_id", "bez_name", "wov_id", "gemeinde_na"])
        if _table_exists(conn, TABLE_BEZIRKE) and _table_exists(conn, facts_table):
            logging.info(f"Creating Datasette view {view_name}...")
            _create_datasette_view(conn, view_name, facts_table, fact_metric_cols)


def _daily_bezirke_archive_files():
    """Daily bezirke CSVs only (YYYY-MM-DD), not timerange slices named bezirke_stats_YYYY-MM_...."""
    pattern = os.path.join("data", "stats", "bezirke", "*", "bezirke_stats_*.csv")
    return sorted(f for f in glob.glob(pattern) if _DAILY_BEZIRKE_CSV.match(os.path.basename(f)))


def seed_sqlite_from_archives():
    """One-time backfill of SQLite from existing stats CSV archives when tables are empty."""
    if _table_row_count(DB_100416, TABLE_100416) == 0:
        archive_files = _daily_bezirke_archive_files()
        if archive_files:
            logging.info(f"Seeding {TABLE_100416} from {len(archive_files)} archive CSV(s)...")
            frames = [pd.read_csv(f) for f in archive_files]
            df = pd.concat(frames, ignore_index=True)
            append_stats_to_sqlite(
                df,
                DB_100416,
                TABLE_100416,
                DEDUPE_COLS_416,
                CONFIGS["bezirke"]["output_cols"],
            )

    if _table_row_count(DB_100428, TABLE_100428) == 0:
        archive_files = sorted(
            glob.glob(os.path.join("data", "stats", "bezirke_timerange", "*", "bezirke_timerange_stats_*.csv"))
        )
        if archive_files:
            logging.info(f"Seeding {TABLE_100428} from {len(archive_files)} archive CSV(s)...")
            frames = [pd.read_csv(f) for f in archive_files]
            df = pd.concat(frames, ignore_index=True)
            append_stats_to_sqlite(
                df,
                DB_100428,
                TABLE_100428,
                DEDUPE_COLS_428,
                MONTHLY_CONFIGS["bezirke"]["output_cols"],
            )


def publish_rolling_datasets():
    config416 = CONFIGS["bezirke"]
    config428 = MONTHLY_CONFIGS["bezirke"]

    build_rolling_export(
        DB_100416,
        TABLE_100416,
        ROLLING_416_PATH,
        _ods_export_cols(config416["output_cols"]),
        is_monthly=False,
    )
    build_rolling_export(
        DB_100428,
        TABLE_100428,
        ROLLING_428_PATH,
        _ods_export_cols(config428["output_cols"]),
        is_monthly=True,
    )

    _publish_ods_zip(
        ROLLING_416_PATH,
        "mobilitaet/mikromobilitaet/stats/bezirke",
        "100416",
    )
    _publish_ods_zip(
        ROLLING_428_PATH,
        "mobilitaet/mikromobilitaet/stats/bezirke_timerange",
        "100428",
    )


def download_spatial_descriptors(ods_id):
    """
    Download and extract a shapefile from data.bs.ch for a given ODS dataset ID.
    Returns a GeoDataFrame in EPSG:2056.
    """
    url_to_shp = f"https://data.bs.ch/explore/dataset/{ods_id}/download/?format=shp"
    r = common.requests_get(url_to_shp)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    extract_folder = os.path.join("data", ods_id)
    if not os.path.exists(extract_folder):
        os.makedirs(extract_folder)

    z.extractall(extract_folder)
    path_to_shp = os.path.join(extract_folder, f"{ods_id}.shp")

    gdf = gpd.read_file(path_to_shp, encoding="utf-8")
    return gdf.to_crs("EPSG:2056")


def combine_files_to_gdf(dates, start_hour=0, end_hour=24):
    """
    Given a list of date strings (e.g. ["2025-02-03", "2025-02-10", ...]) and optional start_hour/end_hour,
    read each local .gpkg file in that date/hour range into a GeoDataFrame, concatenate them,
    and ensure they are in EPSG:2056.

    Returns the combined GeoDataFrame and a list of all missing timestamps (in string form).
    """
    year_month = pd.to_datetime(dates[0]).strftime("%Y-%m")
    local_folder = os.path.join("data", "archiv", year_month)

    if not os.path.exists(local_folder):
        logging.error(f"Local folder {local_folder} does not exist.")
        return gpd.GeoDataFrame(), []

    # We'll accumulate partial GDFs for each date.
    all_gdf_parts = []
    all_missing_timestamps = []
    for date_str in dates:
        date_obj = pd.to_datetime(date_str)
        start_dt = date_obj.replace(hour=start_hour, minute=0)
        if end_hour == 24:
            end_dt = date_obj.replace(hour=0, minute=0) + pd.Timedelta(days=1)
        else:
            end_dt = date_obj.replace(hour=end_hour, minute=0)

        # Generate the list of all 10-min timestamps in [start_hour, end_hour).
        all_timestamps = pd.date_range(start_dt, end_dt, freq="10min", inclusive="left", tz="Europe/Zurich")

        found_timestamps = []
        gdf_list = []
        # Iterate over the gpkg files in our local folder.
        for file in os.listdir(local_folder):
            if file.endswith(".gpkg"):
                # For example: 2025-02-03_09-30_part.gpkg
                file_base = file.replace(".gpkg", "")
                file_ts = pd.to_datetime(file_base, format="%Y-%m-%d_%H-%M%z", errors="coerce")

                if file_ts is not None:
                    # Check if this file_ts is within our date_str day AND within the hour range.
                    if file_ts.date() == date_obj.date() and (start_hour <= file_ts.hour < end_hour):
                        found_timestamps.append(file_ts)

                        path = os.path.join(local_folder, file)
                        gdf_part = gpd.read_file(path)

                        # Reproject if needed
                        if gdf_part.crs is not None and gdf_part.crs.to_epsg() != 2056:
                            gdf_part = gdf_part.to_crs(epsg=2056)

                        gdf_list.append(gdf_part)
                else:
                    logging.warning(f"Skipping non-timestamped file {file}.")
            else:
                logging.warning(f"Skipping non-GPKG file {file}.")

        if gdf_list:
            combined_for_day = pd.concat(gdf_list, ignore_index=True)
            all_gdf_parts.append(combined_for_day)
            logging.info(
                f"Combined {len(gdf_list)} partial files into a single GDF with "
                f"{len(combined_for_day)} records for {date_str} (hours {start_hour}-{end_hour})."
            )
        else:
            logging.info(f"No GPKG files found for {date_str} in hours {start_hour}-{end_hour}.")
            continue

        # Identify missing timestamps for this specific date
        missing_timestamps = all_timestamps.difference(found_timestamps)
        missing_timestamps_str = missing_timestamps.strftime("%Y-%m-%d_%H-%M%z").tolist()

        logging.info(
            f"Found {len(missing_timestamps)} missing timestamps for {date_str} in hour range {start_hour}-{end_hour}."
        )
        all_missing_timestamps.extend(missing_timestamps_str)

    if not all_gdf_parts:
        # Return an empty GeoDataFrame if we never found anything.
        return gpd.GeoDataFrame(), []

    # Concatenate across all requested dates.
    gdf_combined = pd.concat(all_gdf_parts, ignore_index=True)

    logging.info(
        f"Final combined GDF has {len(gdf_combined)} records across {len(dates)} date(s) "
        f"for hours {start_hour}-{end_hour}."
    )

    return gdf_combined, all_missing_timestamps


def compute_stats(
    gdf_points,
    gdf_polygons,
    group_cols,
    fill_empty_polygon_column,
    missing_timestamps_str=None,
    start_hour=None,
    end_hour=None,
):
    """
    Spatially joins point data to a polygon layer and computes statistics.
    When start_time and end_time are provided, statistics are computed only for that period.
    If missing_timestamps_str is provided, those timestamps are removed from the period.
    Returns a pandas DataFrame with one row per group containing the computed statistics.
    """
    if gdf_points.empty:
        logging.warning("No points in the combined GDF; returning empty stats DataFrame.")
        return pd.DataFrame()

    gdf_joined = gpd.sjoin(gdf_points, gdf_polygons, how="left", predicate="intersects")

    # Ensure the timestamp column is in datetime format
    gdf_joined["timestamp"] = pd.to_datetime(gdf_joined["timestamp"], utc=True)
    gdf_joined["timestamp"] = gdf_joined["timestamp"].dt.tz_convert("Europe/Zurich")

    period_start = gdf_joined["timestamp"].min().floor("10min")
    period_end = gdf_joined["timestamp"].max().ceil("10min")
    all_timestamps = pd.date_range(start=period_start, end=period_end, freq="10min", tz="Europe/Zurich")
    if start_hour is not None and end_hour is not None:
        # Build a partial set of timestamps for each day
        all_timestamp_list = []
        days = gdf_joined["timestamp"].dt.normalize().unique()
        for day in days:
            daily_start = day + pd.Timedelta(hours=start_hour)
            daily_end = day + pd.Timedelta(hours=end_hour)

            # Generate just the 10-minute intervals we care about in [start_hour, end_hour)
            if daily_start < daily_end:
                times = pd.date_range(
                    start=daily_start,
                    end=daily_end,
                    freq="10min",
                    tz="Europe/Zurich",
                    inclusive="left",
                )
                all_timestamp_list.extend(times)

        all_timestamps = pd.DatetimeIndex(all_timestamp_list)

    if missing_timestamps_str:
        all_timestamps = all_timestamps[~all_timestamps.strftime("%Y-%m-%d_%H-%M%z").isin(missing_timestamps_str)]

    # Create combinations of groups and timestamps.
    group_combinations = gdf_joined[group_cols].drop_duplicates()
    all_combinations = pd.merge(
        group_combinations.assign(key=1),
        pd.DataFrame({"timestamp": all_timestamps}).assign(key=1),
        on="key",
    ).drop("key", axis=1)

    # Merge with the actual data, filling missing rows with count = 0.
    df_count_grouped = all_combinations.merge(
        gdf_joined.groupby(group_cols + ["timestamp"], dropna=False)
        .agg(count=("xs_provider_name", "size"))
        .reset_index(),
        on=group_cols + ["timestamp"],
        how="left",
    ).fillna({"count": 0})

    # Compute counting stats.
    counting_stats = (
        df_count_grouped.groupby(group_cols, dropna=False)
        .agg(
            sum=("count", "sum"),
            mean=("count", "mean"),
            min=("count", "min"),
            max=("count", "max"),
            median=("count", "median"),
            q1=("count", lambda x: x.quantile(0.25)),
            q3=("count", lambda x: x.quantile(0.75)),
        )
        .reset_index()
    )
    logging.info(f"Computed counting stats for {len(counting_stats)} groups.")

    # Compute range stats (without filling 0s since range metrics shouldn't be artificially set)
    range_stats = (
        gdf_joined.groupby(group_cols, dropna=False)
        .agg(
            current_range_meters_mean=("xs_current_range_meters", "mean"),
            current_range_meters_min=("xs_current_range_meters", "min"),
            current_range_meters_max=("xs_current_range_meters", "max"),
            current_range_meters_median=("xs_current_range_meters", "median"),
            current_range_meters_q1=(
                "xs_current_range_meters",
                lambda x: x.quantile(0.25),
            ),
            current_range_meters_q3=(
                "xs_current_range_meters",
                lambda x: x.quantile(0.75),
            ),
        )
        .reset_index()
    )
    logging.info(f"Computed range stats for {len(range_stats)} groups.")

    # Merge counting and range stats.
    grouped_stats = counting_stats.merge(range_stats, on=group_cols, how="left")
    polygon_id_column = group_cols[-1]
    grouped_stats = grouped_stats.merge(gdf_polygons, on=polygon_id_column, how="left")
    grouped_stats["num_measures"] = len(all_timestamps)

    # Remove rows where polygon_id is NaN if remove_empty_polygon_columns is True
    if fill_empty_polygon_column:
        grouped_stats[fill_empty_polygon_column] = grouped_stats[fill_empty_polygon_column].fillna("ausserkantonal")
    else:
        grouped_stats = grouped_stats.dropna(subset=[polygon_id_column])
        logging.info(f"Removed rows with NaN values in {polygon_id_column}")

    return grouped_stats


def save_stats(df_stats, prefix, date_str, columns_of_interest, timerange_label=None):
    """
    Save the stats to a CSV and upload to the FTP.
    If timerange_label is provided, it is included in the filename (for monthly timerange stats);
    otherwise, a daily stats filename is created.
    """
    if df_stats.empty:
        logging.warning(f"No stats to save for {prefix} on {date_str}.")
        return

    # Create output folder structure.
    output_folder = os.path.join("data", "stats", prefix, date_str[:4])
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    if timerange_label:
        output_file = os.path.join(output_folder, f"{prefix}_stats_{date_str}_{timerange_label}.csv")
    else:
        output_file = os.path.join(output_folder, f"{prefix}_stats_{date_str}.csv")

    df_stats = df_stats[columns_of_interest]
    df_stats.to_csv(output_file, index=False, encoding="utf-8")
    logging.info(f"Saved stats to {output_file}")

    remote_path = f"mobilitaet/mikromobilitaet/stats/{prefix}/{date_str[:4]}"
    common.ensure_ftp_dir(
        FTP_SERVER,
        FTP_USER,
        FTP_PASS,
        remote_path,
    )
    common.upload_ftp(
        output_file,
        remote_path=remote_path,
    )


def process_daily_stats(date_str):
    """
    Process daily stats for a given date.
    """
    gdf_daily_points, missing_timestamps_str = combine_files_to_gdf([date_str])
    for stats_type, config in CONFIGS.items():
        gdf_shapes = download_spatial_descriptors(config["ods_id_shapes"])
        if stats_type == "bezirke":
            gdf_wohnviertel = download_spatial_descriptors("100042")
            gdf_shapes = gdf_shapes.merge(
                gdf_wohnviertel[["wov_id", "wov_name", "gemeinde_na"]],
                on="wov_id",
                how="left",
            )

        df_stats = compute_stats(
            gdf_daily_points,
            gdf_shapes,
            config["group_cols"],
            config["fill_empty_polygon_column"],
            missing_timestamps_str,
        )
        df_stats["date"] = date_str
        save_stats(df_stats, stats_type, date_str, config["output_cols"])
        if stats_type == "bezirke":
            append_stats_to_sqlite(
                df_stats,
                DB_100416,
                TABLE_100416,
                DEDUPE_COLS_416,
                config["output_cols"],
            )


def process_monthly_timerange_stats(year, month):
    """
    Process stats for a given month. For each weekday (Monday to Sunday) and for each timerange,
    aggregate stats across all days (including missing timestamps).
    """

    # Download shapes for the given config. Here we assume processing for one config, e.g. 'bezirke'
    config = MONTHLY_CONFIGS["bezirke"]
    gdf_shapes = download_spatial_descriptors(config["ods_id_shapes"])
    # For 'bezirke' we also merge additional attributes.
    gdf_wohnviertel = download_spatial_descriptors("100042")
    gdf_shapes = gdf_shapes.merge(gdf_wohnviertel[["wov_id", "wov_name", "gemeinde_na"]], on="wov_id", how="left")

    # Define timeranges (adjust as needed)
    timeranges = [
        ("00:00", "03:00"),
        ("03:00", "06:00"),
        ("06:00", "09:00"),
        ("09:00", "12:00"),
        ("12:00", "15:00"),
        ("15:00", "18:00"),
        ("18:00", "21:00"),
        ("21:00", "24:00"),
    ]
    # Define start and end of month.
    start_date = pd.Timestamp(year=year, month=month, day=1)
    end_date = start_date + relativedelta(months=1) - pd.Timedelta(days=1)

    # For each weekday: 0=Monday, …, 6=Sunday.
    for weekday in range(7):
        days_of_week = [day for day in pd.date_range(start_date, end_date, freq="D") if day.weekday() == weekday]
        if not days_of_week:
            continue

        # Process each timerange for the current weekday.
        for start_str, end_str in timeranges:
            date_strs = [day.strftime("%Y-%m-%d") for day in days_of_week]
            gdf_daily_points, missing_timestamps_str = combine_files_to_gdf(
                date_strs, start_hour=int(start_str[:2]), end_hour=int(end_str[:2])
            )

            df_stats = compute_stats(
                gdf_daily_points,
                gdf_shapes,
                config["group_cols"],
                config["fill_empty_polygon_column"],
                missing_timestamps_str,
                start_hour=int(start_str[:2]),
                end_hour=int(end_str[:2]),
            )
            df_stats["date"] = start_date.strftime("%Y-%m")
            df_stats["weekday"] = weekday
            df_stats["timerange_start"] = start_str
            df_stats["timerange_end"] = end_str

            timerange_label = f"{start_str.replace(':', '')}_{end_str.replace(':', '')}_wd{weekday}"
            # Using the date_str from the month start as an identifier.
            save_stats(
                df_stats,
                "bezirke_timerange",
                start_date.strftime("%Y-%m"),
                config["output_cols"],
                timerange_label=timerange_label,
            )
            append_stats_to_sqlite(
                df_stats,
                DB_100428,
                TABLE_100428,
                DEDUPE_COLS_428,
                config["output_cols"],
            )


def main():
    init_datasette_workdir()
    try:
        seed_sqlite_from_archives()

        # Process daily stats for each day between a given start and end date.
        date_str_start = (datetime.now() - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        date_str_end = (datetime.now() - pd.Timedelta(days=1)).strftime("%Y-%m-%d")

        for date_str in pd.date_range(date_str_start, date_str_end, freq="D").strftime("%Y-%m-%d"):
            logging.info(f"Processing daily stats for {date_str}...")
            process_daily_stats(date_str)

        # If today is the first of the month, process the previous month for timerange stats.
        if datetime.now().day == 1:
            logging.info(
                f"Processing monthly timerange stats for the previous month ({datetime.now().strftime('%Y-%m')})..."
            )
            last_month_date = datetime.now() - relativedelta(months=1)
            process_monthly_timerange_stats(last_month_date.year, last_month_date.month)

        finalize_datasette_db(
            DB_100416,
            TABLE_100416,
            DEDUPE_COLS_416,
            INDEX_COLS_416,
            CONFIGS["bezirke"]["output_cols"],
            VIEW_DAILY,
        )
        finalize_datasette_db(
            DB_100428,
            TABLE_100428,
            DEDUPE_COLS_428,
            INDEX_COLS_428,
            MONTHLY_CONFIGS["bezirke"]["output_cols"],
            VIEW_TIMERANGE,
        )
        # Rolling export reads the local work copy; DBs are copied to mount in finally.
        publish_rolling_datasets()

        # Publish gemeinden only; bezirke 100416/100428 use rolling exports above
        for _, config in CONFIGS.items():
            if config["ods_id"] in ("100416",):
                continue
            common.publish_ods_dataset_by_id(config["ods_id"])
    finally:
        commit_datasette_workdir()
        _cleanup_datasette_workdir()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f"Executing {__file__}...")
    main()
    logging.info("Job successful!")
