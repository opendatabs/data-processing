import logging
import common
import common.change_tracking as ct #i am not using this, simply compare the bites
import pandas as pd
import geopandas as gpd 
import json
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

CRS = "EPSG:4326"
CRS_SWISS = "EPSG:2056"

ALLMEND_URL = "https://data.bs.ch/explore/dataset/100018/download/"
RIVERS_URL = "https://data.bs.ch/explore/dataset/100261/download/"

ALLMEND_CACHE = Path("data_orig/allmendbewilligungen.geojson") #gets created the first time the script is run?
RIVERS_CACHE = Path("data_orig/gewaesserachsen.geojson")

BUFFER_M = 150
MAX_MONTHS_OUT = 24 # kein filter? später im ui
EVENT_TYPES = ["Veranstaltung", "Aktivität", "Festivität"]
EXCLUDED_STATUSES = ["storniert", "nicht bewilligt"] # auch erst im link
APPROVED_STATUS = "bewilligt"
REQUIRE_APPROVED = False

OUTPUT_COLUMNS = [
    "Bezeichnung",
    "Belegungsstatus-Bezeichung",
    "Datum",
    "MinDistanzRhein",
    "Link" #link to the original data set, filtered so that all entries collapsed into this one are shown
]
# add link zu kollapsed daten, falls mit filter darstellbar (zur not ids angeben)

# ------------------- extract --------------------------------
def _canonicalize_geojson(raw_bytes: bytes) -> bytes:
    """Produce a byte-stable representation of a GeoJSON FeatureCollection,
    independent of the server's (unstable) feature ordering."""
    gj = json.loads(raw_bytes)
    gj["features"] = sorted(gj["features"], key=lambda f: json.dumps(f, sort_keys=True))
    return json.dumps(gj, sort_keys=True, ensure_ascii=False).encode("utf-8")

def _write_if_bytes_changed(path: Path, new_bytes: bytes) -> bool:
    """
    Write bytes to path only if content differs.
    Returns True if file content changed (written), else False.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.read_bytes() == new_bytes: #downlads do not have the same bites even if the data has not changed!!
        return False
    path.write_bytes(new_bytes)
    return True

#downloads the two data sets, allmendbewilligungen and gewässerachsen, as geojson and saves them in the cache if they changed
#returns the paths to the caches and true if any of the two changed, false if both are unchanged
def download_to_cache(source_path: Path, cache_path: Path) -> tuple[Path, bool]:

    logging.info("trying to downlad the data files")

    r = common.requests_get(source_path, params={"format": "geojson"}) #does this need a catch block?
    r.raise_for_status()
    canonical = _canonicalize_geojson(r.content) #so we can compare the raw bites later
    bytes_changed = _write_if_bytes_changed(cache_path, canonical)

    return cache_path, bytes_changed

# -------------------------- transform -------------------------------------

def load_rhine_line(rivers_path: Path) -> gpd.GeoDataFrame:

    gj = json.loads(rivers_path.read_bytes())
    gdf = gpd.GeoDataFrame.from_features(gj["features"], crs=CRS)

    rhine = gdf[gdf["gz_gewaessername"] == "Rhein"]
    if rhine.empty:
        raise RuntimeError("Rhein nicht in Gewässerachsen gefunden")

    return rhine.to_crs(CRS_SWISS).geometry.union_all()

def load_and_collapse_allmende(allmend_path: Path) -> gpd.GeoDataFrame:

    gj = json.loads(allmend_path.read_bytes())
    gdf = gpd.GeoDataFrame.from_features(gj["features"], crs=CRS)
    logging.info("Loaded %d total Allmend records.", len(gdf))

    #TODO: collapsing and adding the link to the original entries!!!
    #make sure, that the coloum names are the same as specified above!!!

    return gdf


def find_events_near_rhine(allmend_path: Path, rivers_path: Path, buffer_m: float) -> gpd.GeoDataFrame:

    rhein_line = load_rhine_line(rivers_path)
    allmend_gpd = load_and_collapse_allmende(allmend_path)
    rhine_buffer = rhein_line.buffer(buffer_m)

    allmend_gpd = allmend_gpd.to_crs(CRS_SWISS)
    allmend_gpd["MinDistanzRhein"] = allmend_gpd.geometry.distance(rhein_line).round(1)
    allmend_gpd = allmend_gpd[allmend_gpd.geometry.intersects(rhine_buffer)].copy()
    
    return allmend_gpd.to_crs(CRS)

# ---------------------------- load ---------------------------------------------

# do we need a geodataframe as output? there are no geo shapes left in the result
# maybe only csv for the moment, can be added later
def write_outputs(gdf: gpd.GeoDataFrame, data_dir: str = "data") -> None:
    data_dir = Path(data_dir)
    cols = [c for c in OUTPUT_COLUMNS if c in gdf.columns]
    csv_path = data_dir / "allmend_events_near_rhine.csv"
    gdf[cols].to_csv(csv_path, index=False)
    logging.info("Wrote:\n  %s", csv_path)
    

# ------------------------------ main -----------------------------------------------------

def main():
    """Main ETL function."""
     # #.                           
    logging.info("ETL job started")
    
    allmend_path, allmend_changed = download_to_cache(ALLMEND_URL, ALLMEND_CACHE)
    rivers_path, rivers_changed = download_to_cache(RIVERS_URL, RIVERS_CACHE)
    
    if not allmend_changed and not rivers_changed:
        logging.info("Neither source dataset changed since the last run - skipping processing.")
        return

    near = find_events_near_rhine(allmend_path, rivers_path, BUFFER_M)
    write_outputs(near)
    
    logging.info("ETL job completed")



if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f"Executing {__file__}...")
    main()
    logging.info("Job successful.")

