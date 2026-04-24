import json
import logging
import urllib.parse
import zipfile
from pathlib import Path

import common
import geopandas as gpd
import httpx
import pandas as pd
from dataspot_auth import DataspotAuth

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

STAC_BASE_URL = "https://api.geo.bs.ch/stac/v1"
DATASET_DETAILS_URL = "https://bs.dataspot.io/rest/prod/datasets/{id}"

COLLECTIONS_FILE = Path("data/bs_stac_collections.xlsx")
GEO_DATASETS_FILE = Path("data/geo_datasets.json")
PUB_DATASETS_FILE = Path("data/pub_datasets.xlsx")

OUTPUT_METADATA_FILE = Path("data/Metadata.csv")
OUTPUT_DATASETS_DIR = Path("data/datasets")
OUTPUT_DATASETS_DIR.mkdir(parents=True, exist_ok=True)

auth = DataspotAuth()


def load_geo_datasets():
    if not GEO_DATASETS_FILE.exists():
        raise FileNotFoundError(f"Datei nicht gefunden: {GEO_DATASETS_FILE}")

    with open(GEO_DATASETS_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError("geo_datasets.json muss eine Liste sein.")

    return data


def load_excel_file(file_path):
    if not file_path.exists():
        raise FileNotFoundError(f"Datei nicht gefunden: {file_path}")

    df = pd.read_excel(file_path)
    df = df.fillna("")
    for col in df.columns:
        if pd.api.types.is_object_dtype(df[col]):
            df[col] = df[col].str.strip()

    return df


def convert_timestamp(value):

    if value is None or value == "":
        return None
    if pd.isna(value):
        return None

    try:
        if isinstance(value, (int, float)):
            return pd.to_datetime(value, unit="ms").strftime("%Y-%m-%d")

        if isinstance(value, str) and value.isdigit():
            return pd.to_datetime(int(value), unit="ms").strftime("%Y-%m-%d")

    except Exception:
        return value
    return value


def fetch_dataset_details(dataset_id):
    url = DATASET_DETAILS_URL.format(id=dataset_id)
    headers = auth.get_headers()
    response = common.requests_get(url=url, headers=headers)
    response.raise_for_status()
    return response.json()


def build_geo_lookup(geo_data):
    geo_lookup = {}

    for record in geo_data:
        paket_name = str(record.get("title", "")).strip()
        if not paket_name:
            continue

        children_lookup = {}
        for child in record.get("children", []):
            name = str(child.get("productLayername", "")).strip()
            if name:
                children_lookup[name] = child

        geo_lookup[paket_name] = {
            "record": record,
            "children": children_lookup,
        }

    return geo_lookup


def build_metadata_rows(collections_df, geo_data, pub_df):
    rows = []

    for _, pub_row in pub_df.iterrows():
        paket_name = pub_row.Paket
        geo_dataset_name = pub_row.geo_dataset
        pub_dataset_id = pub_row.get("id")

        collection_match = collections_df[collections_df["title"] == paket_name]
        if collection_match.empty:
            logging.warning("Kein Collection-Match für Paket gefunden: %s", paket_name)
            continue
        collection_row = collection_match.iloc[0]
        geo_lookup = build_geo_lookup(geo_data)
        geo_entry = geo_lookup.get(paket_name)
        if geo_entry is None:
            logging.warning("Kein JSON-Paket gefunden: %s", paket_name)
            continue

        child_record = geo_entry["children"].get(geo_dataset_name)
        if child_record is None:
            logging.warning(
                "Kein JSON-Child gefunden für Paket='%s', geo_dataset='%s'",
                paket_name,
                geo_dataset_name,
            )
            continue
        dataset_details = {}
        custom_properties = {}

        if pub_dataset_id:
            try:
                dataset_details = fetch_dataset_details(pub_dataset_id)
                custom_properties = dataset_details.get("customProperties", {})
            except Exception as e:
                logging.warning(
                    "Konnte Dataset-Details nicht laden für id='%s': %s",
                    pub_dataset_id,
                    e,
                )

        detail_description = dataset_details.get("description")

        relation_values = [
            collection_row.get("MapBS_link"),
            collection_row.get("Metadata"),
        ]
        relation_values = [value for value in relation_values if value]

        row = {
            "ods_id": pub_row.get("ods_id"),
            "title": pub_row.get("titel_nice"),
            "description": pub_row.get("description") if pub_row.get("description") else detail_description,
            "theme": pub_row.get("theme"),
            "keyword": pub_row.get("keyword"),
            "dcat_ap_ch.rights": "NonCommercialAllowed-CommercialAllowed-ReferenceRequired",
            "dcat_ap_ch.license": "terms_by",
            "dcat.contact_name": "Open Data Basel-Stadt",
            "dcat.contact_email": "opendata@bs.ch",
            "dcat.created": convert_timestamp(custom_properties.get("creationDate")),
            "dcat.creator": pub_row.get("herausgeber"),
            "dcat.accrualperiodicity": dataset_details.get("accrualPeriodicity"),
            "attributions": "Geodaten Kanton Basel-Stadt",
            "publisher": pub_row.get("herausgeber"),
            "dcat.issued": convert_timestamp(custom_properties.get("publicationDate")),
            "dcat.relation": "; ".join(relation_values),
            "modified": convert_timestamp(custom_properties.get("lastUpdate")),
            "language": "de",
            "publizierende-organisation": pub_row.get("publizierende Organisation"),
            "tags": pub_row.get("tags"),
            "schema_file": pub_row.get("schema_file"),
            # Technische Referenzen
            "collection_id": collection_row.get("id"),
            "paket": paket_name,
            "geo_dataset": geo_dataset_name,
            "pub_dataset_id": pub_dataset_id,
            "license": collection_row.get("license"),
            "MapBS_link": collection_row.get("MapBS_link"),
            "Metadata": collection_row.get("Metadata"),
        }

        for key, value in child_record.items():
            row[key] = value

        rows.append(row)

    return rows


def save_metadata_csv(rows):
    df = pd.DataFrame(rows)

    columns_to_drop = [
        "collection_id",
        "paket",
        "geo_dataset",
        "pub_dataset_id",
        "MapBS_link",
        "Metadata",
        "label",
        "id",
        "productLayername",
        "inCollection",
    ]

    df = df.drop(columns=columns_to_drop, errors="ignore")

    df.to_csv(OUTPUT_METADATA_FILE, sep=";", index=False, encoding="utf-8-sig")
    logging.info("Metadata.csv gespeichert: %s", OUTPUT_METADATA_FILE)
    return df


def build_download_rows(collections_df, geo_data, pub_df):
    rows = []

    for _, pub_row in pub_df.iterrows():
        paket_name = pub_row.Paket
        geo_dataset_name = pub_row.geo_dataset

        collection_match = collections_df[collections_df["title"] == paket_name]
        if collection_match.empty:
            logging.warning("Kein Collection-Match für Paket gefunden: %s", paket_name)
            continue
        collection_row = collection_match.iloc[0]
        geo_lookup = build_geo_lookup(geo_data)
        geo_entry = geo_lookup.get(paket_name)
        if geo_entry is None:
            logging.warning("Kein JSON-Paket gefunden: %s", paket_name)
            continue

        child_record = geo_entry["children"].get(geo_dataset_name)
        if child_record is None:
            logging.warning(
                "Kein JSON-Child gefunden für Paket='%s', geo_dataset='%s'",
                paket_name,
                geo_dataset_name,
            )
            continue

        rows.append(
            {
                "collection_id": collection_row.get("id"),
                "productLayername": child_record.get("productLayername"),
                "MapBS_link": collection_row.get("MapBS_link"),
                "create_map_links": pub_row.get("create_map_links"),
            }
        )

    return pd.DataFrame(rows)


def find_matching_geojson_name(zip_names, product_layername):
    if not product_layername:
        return None

    wanted = str(product_layername).strip().lower()

    for name in zip_names:
        file_name = Path(name).name
        stem = Path(file_name).stem.lower()
        if file_name.lower() == product_layername or stem == wanted:
            return name

    return None


def create_map_links(geometry, p1, p2):
    p1 = urllib.parse.quote(p1)
    p2 = urllib.parse.quote(p2)

    if geometry is None:
        return None

    if geometry.geom_type == "Polygon":
        centroid = geometry.centroid
    else:
        centroid = geometry

    lat, lon = centroid.y, centroid.x
    return f"https://opendatabs.github.io/map-links/?lat={lat}&lon={lon}&p1={p1}&p2={p2}"


def extract_map_params(link):
    if not link:
        return None, None

    try:
        with httpx.Client(follow_redirects=True, timeout=60.0) as client:
            response = client.get(link)

        redirect_link = str(response.url)
        parsed = urllib.parse.urlparse(redirect_link)
        query_params = urllib.parse.parse_qs(parsed.query)

        p1 = query_params.get("tree_groups", [None])[0]

        p2 = None
        for key, values in query_params.items():
            if key.startswith("tree_group_layers_"):
                p2 = values[0] if values else None
                break

        return p1, p2

    except Exception as e:
        logging.warning("Konnte Redirect-Parameter aus MapBS_link nicht extrahieren: %s", e)
        return None, None


def add_map_links_to_dataset(dataset_file, mapbs_link):
    p1, p2 = extract_map_params(mapbs_link)

    if not p1 or not p2:
        logging.warning("Map-Link-Parameter fehlen für %s", dataset_file)
        return False

    try:
        gdf = gpd.read_file(dataset_file)
        gdf_transformed = gdf.copy()
        gdf_transformed = gdf_transformed.to_crs("EPSG:4326")
        if "geometry" not in gdf_transformed.columns:
            logging.warning("Keine geometry-Spalte in %s gefunden", dataset_file)
            return False

        gdf_transformed["map_links"] = gdf_transformed["geometry"].apply(
            lambda geom: create_map_links(geom, p1, p2) if geom is not None else None
        )
        gdf["map_links"] = gdf_transformed["map_links"]
        gdf.to_file(dataset_file, driver="GeoJSON")
        logging.info("map_links ergänzt: %s", dataset_file)
        return True

    except Exception as e:
        logging.error("Fehler beim Ergänzen von map_links für %s: %s", dataset_file, e)
        return False


def download_and_extract_dataset(collection_id, product_layername):
    url = f"{STAC_BASE_URL}/download/{collection_id}/latest/geojson"

    zip_path = OUTPUT_DATASETS_DIR / f"{collection_id}.zip"
    output_file = OUTPUT_DATASETS_DIR / f"{collection_id}_{product_layername}.geojson"

    try:
        response = common.requests_get(url=url)
        response.raise_for_status()

        with open(zip_path, "wb") as f:
            f.write(response.content)

        with zipfile.ZipFile(zip_path, "r") as zf:
            zip_names = [name for name in zf.namelist() if not name.endswith("/")]

            matched_name = find_matching_geojson_name(zip_names, product_layername)
            if matched_name is None:
                raise ValueError(
                    f"Keine passende Datei für productLayername='{product_layername}' gefunden. Inhalt: {zip_names}"
                )

            with zf.open(matched_name) as source, open(output_file, "wb") as target:
                target.write(source.read())

        zip_path.unlink(missing_ok=True)
        logging.info("Datensatz gespeichert: %s", output_file)
        return output_file

    except Exception as e:
        logging.error(
            "Fehler beim Download für collection_id=%s, productLayername=%s: %s",
            collection_id,
            product_layername,
            e,
        )
        return None


def download_datasets(download_df):
    for _, row in download_df.iterrows():
        collection_id = row.collection_id
        product_layername = row.productLayername

        if not collection_id or not product_layername:
            continue

        dataset_file = download_and_extract_dataset(collection_id, product_layername)
        if dataset_file is None:
            continue

        if row.get("create_map_links"):
            logging.info("Create map links for %s", dataset_file.name)
            add_map_links_to_dataset(dataset_file, row.get("MapBS_link"))


def main():
    logging.info("BS_stac_Collection.xlsx laden ...")
    collections_df = load_excel_file(COLLECTIONS_FILE)

    logging.info("geo_datasets.json laden ...")
    geo_data = load_geo_datasets()

    logging.info("pub_datasets.xlsx laden ...")
    pub_df = load_excel_file(PUB_DATASETS_FILE)

    logging.info("Metadaten-Zeilen bauen ...")
    metadata_rows = build_metadata_rows(collections_df, geo_data, pub_df)

    logging.info("Metadata.csv schreiben ...")
    save_metadata_csv(metadata_rows)

    logging.info("Download-Zeilen bauen ...")
    download_df = build_download_rows(collections_df, geo_data, pub_df)

    logging.info("Geo-Datasets herunterladen ...")
    download_datasets(download_df)

    logging.info("Fertig.")


if __name__ == "__main__":
    main()
