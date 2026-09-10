import datetime as dt
import json
import logging
from pathlib import Path
from urllib.parse import quote

import common
import geopandas as gpd
import pandas as pd
from dotenv import load_dotenv
from shapely.ops import unary_union

load_dotenv()

CRS = "EPSG:4326"
CRS_SWISS = "EPSG:2056"

ALLMEND_URL = "https://data.bs.ch/api/explore/v2.1/catalog/datasets/100018/exports/geojson"
RIVERS_URL = "https://data.bs.ch/api/explore/v2.1/catalog/datasets/100261/exports/geojson"

ALLMEND_CACHE = Path("data_orig/allmendbewilligungen.geojson")  # gets created the first time the script is run?
RIVERS_CACHE = Path("data_orig/gewaesserachsen.geojson")

BUFFER_M = 150
EVENT_TYPES = ["Veranstaltung", "Aktivität", "Festivität"]
ALLMEND_PARAMS = {"where": "belgartbez IN (" + ", ".join(f'"{t}"' for t in EVENT_TYPES) + ")"} # to filter the event_types already while pulling the data

OUTPUT_COLUMNS = ["Bezeichnung", "Belegstatus", "datum_von", "datum_bis", "Nähe_Flüsse", "Link"]

MAX_URL_LENGTH = 2000  # common safe threshold for URL length

# ------------------- extract --------------------------------

def allmend_has_changed(saved_date: Path) -> bool:

    r = common.requests_get("https://data.bs.ch/api/explore/v2.1/catalog/datasets/100018")

    data = json.loads(r.content)
    new_value = data["metas"]["default"]["modified"]  # e.g. "2026-09-10T00:00:00+00:00"
    new_dt = dt.datetime.fromisoformat(new_value)

    file = Path(saved_date)
    if file.exists():
        old_value = file.read_text().strip()
        if old_value:
            old_dt = dt.datetime.fromisoformat(old_value)
            if new_dt <= old_dt:
                return False

    file.write_text(new_value)
    return True

# downloads the two data sets, allmendbewilligungen and gewässerachsen, as geojson 
def download(source_path: Path, path: Path, *, params: dict | None=None):

    logging.info("trying to downlad the data files")

    r = common.requests_get(source_path, params=params or {"format": "geojson"})  # does this need a catch block?
    r.raise_for_status()

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(r.content)


# -------------------------- transform -------------------------------------


def load_river_lines(rivers_path: Path) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame]:

    gj = json.loads(rivers_path.read_bytes())
    gdf = gpd.GeoDataFrame.from_features(gj["features"], crs=CRS)

    rhein = gdf[gdf["gz_gewaessername"] == "Rhein"]
    wiese = gdf[gdf["gz_gewaessername"] == "Wiese"]
    birs = gdf[gdf["gz_gewaessername"] == "Birs"]
    
    return [rhein.to_crs(CRS_SWISS).geometry.union_all(), wiese.to_crs(CRS_SWISS).geometry.union_all(), birs.to_crs(CRS_SWISS).geometry.union_all()]


def make_query_url(field, values, base_url="https://data.bs.ch/explore/dataset/100018/table/"):
    query = " OR ".join(f'{field}="{v}"' for v in values)
    return f"{base_url}?q={quote(query)}"


def _to_list(value):
    if isinstance(value, str):
        return [v.strip() for v in value.split(",")]
    elif isinstance(value, (list, tuple, set)):
        return list(value)
    else:
        return [value]


def build_link(
    idunique, begehrenid, base_url="https://data.bs.ch/explore/dataset/100018/table/", max_len=MAX_URL_LENGTH
):
    id_list = _to_list(idunique)
    link = make_query_url("idunique", id_list, base_url)

    if len(link) <= max_len:
        return link

    # too long -> fall back to the shorter field
    begehren_list = _to_list(begehrenid)
    return make_query_url("begehrenid", begehren_list, base_url)


def _join_unique(values: list[str]) -> str:
    seen: list[str] = []
    for v in values:
        for part in str(v).split(", "):
            if part and part not in seen:
                seen.append(part)
    return ", ".join(seen)


def merge_consecutive_dates(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Within each (begehrenid, bezeichng) group, merge rows whose date ranges
    are consecutive or overlapping (next datum_von <= current datum_bis + 1 day)
    into a single row spanning min(datum_von) .. max(datum_bis).
    """
    gdf = gdf.copy()
    gdf["datum_von"] = pd.to_datetime(gdf["datum_von"])
    gdf["datum_bis"] = pd.to_datetime(gdf["datum_bis"])

    one_day = pd.Timedelta(days=1)
    merged_rows = []

    for _, group in gdf.groupby(["begehrenid", "bezeichng"], sort=False):
        group = group.sort_values(["datum_von", "datum_bis"]).reset_index(drop=True)

        current = group.iloc[0].to_dict()
        geoms = [group.iloc[0]["geometry"]]
        iduniques = [group.iloc[0]["idunique"]]
        statuses = [group.iloc[0]["belestatbe"]]

        def flush():
            current["geometry"] = unary_union(geoms)
            current["idunique"] = _join_unique(iduniques)
            current["belestatbe"] = _join_unique(statuses)
            merged_rows.append(current.copy())

        for i in range(1, len(group)):
            row = group.iloc[i]
            if row["datum_von"] <= current["datum_bis"] + one_day:
                # consecutive/overlapping -> extend the current run
                current["datum_bis"] = max(current["datum_bis"], row["datum_bis"])
                geoms.append(row["geometry"])
                iduniques.append(row["idunique"])
                statuses.append(row["belestatbe"])
            else:
                flush()
                current = row.to_dict()
                geoms = [row["geometry"]]
                iduniques = [row["idunique"]]
                statuses = [row["belestatbe"]]

        flush()

    result = gpd.GeoDataFrame(merged_rows, crs=gdf.crs)
    result["datum_von"] = result["datum_von"].dt.strftime("%Y-%m-%d")
    result["datum_bis"] = result["datum_bis"].dt.strftime("%Y-%m-%d")
    return result


def load_and_collapse_allmende(allmend_path: Path) -> gpd.GeoDataFrame:

    gj = json.loads(allmend_path.read_bytes())
    gdf = gpd.GeoDataFrame.from_features(gj["features"], crs=CRS)
    logging.info("Loaded %d total Allmend records.", len(gdf))

    gdf["geometry"] = gdf["geometry"].buffer(0)  # fix any self-intersecting rings

    # clustering: events that have the same BegehrenID, Bezeichnung and date can be merged together
    gdf = gdf.dissolve(
        by=["begehrenid", "bezeichng", "datum_von", "datum_bis"],
        aggfunc={
            "idunique": lambda x: ", ".join(x.astype(str)),
            "belestatbe": lambda x: ", ".join(x.astype(str).unique()),
        },
    )

    gdf = gdf.reset_index()

    # cluster the dates further, consecutive dates into one multidate event
    gdf = merge_consecutive_dates(gdf)

    gdf["Link"] = gdf.apply(lambda row: build_link(row["idunique"], row["begehrenid"]), axis=1)

    gdf = gdf.rename(columns={"bezeichng": "Bezeichnung", "belestatbe": "Belegstatus"})

    return gdf


def find_events_near_rivers(allmend_path: Path, rivers_path: Path, buffer_m: float) -> gpd.GeoDataFrame:

    rhein, wiese, birs = load_river_lines(rivers_path)
    allmend_gpd = load_and_collapse_allmende(allmend_path)

    rivers = {"Rhein": rhein, "Wiese": wiese, "Birs": birs}

    allmend_gpd = allmend_gpd.to_crs(CRS_SWISS)

    near_df = pd.DataFrame(
        {name: allmend_gpd.geometry.intersects(river.buffer(buffer_m)) for name, river in rivers.items()},
        index=allmend_gpd.index,
    )

    allmend_gpd["Nähe_Flüsse"] = near_df.apply(
        lambda row: ", ".join(name for name, is_near in row.items() if is_near), axis=1
    )

    allmend_gpd = allmend_gpd[near_df.any(axis=1)].copy()

    return allmend_gpd.to_crs(CRS)


# ---------------------------- load ---------------------------------------------


def write_outputs(gdf: gpd.GeoDataFrame) -> None:
    cols = [c for c in OUTPUT_COLUMNS if c in gdf.columns]
    csv_path = "data/100556_allmend_events_near_rivers.csv"
    gdf[cols].to_csv(csv_path, index=False)
    logging.info("Wrote:\n  %s", csv_path)
    common.update_ftp_and_odsp(str(csv_path), "bachapp", "100556")


# ------------------------------ main -----------------------------------------------------


def main():
    logging.info("ETL job started")

    allmend_changed = allmend_has_changed(Path("data_orig/last_changed.csv"))

    if not allmend_changed:
        logging.info("Allmend dataset not updated since the last run - skipping processing.")
        return

    download(ALLMEND_URL, ALLMEND_CACHE, params=ALLMEND_PARAMS)
    download(RIVERS_URL, RIVERS_CACHE)

    near = find_events_near_rivers(ALLMEND_CACHE, RIVERS_CACHE, BUFFER_M)
    write_outputs(near)

    logging.info("ETL job completed")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f"Executing {__file__}...")
    main()
    logging.info("Job successful.")
