import io
import json
import logging
import os
import time
import zipfile

import common
import geopandas as gpd
import numpy as np
import pandas as pd
from common import change_tracking as ct
from common import email_message
from geopy.distance import geodesic
from geopy.extra.rate_limiter import RateLimiter
from geopy.geocoders import Nominatim
from rapidfuzz import process
from shapely.geometry import Point
from tqdm import tqdm


def main():
    list_path = os.path.join("data_orig", "list_directories.txt")
    directories = common.list_directories("data_orig", list_path, ["Old", "export", "2020_07_27"])
    if True or ct.has_changed(list_path):
        df_2017 = process_data_2017()
        df_all = process_data_from_2018(directories, df_2017)
        df_export, df_all = transform_for_export(df_all)
        big_bussen = os.path.join("data", "big_bussen.csv")
        new_plz = os.path.join("data", "new_plz.csv")
        plz = os.path.join("data", "plz.csv")
        if ct.has_changed(big_bussen):
            text = f"The exported file {big_bussen} has changed, please check.\n"
            text += "It contains new values with Bussen > 300 CHF."
            msg = email_message(subject="Warning Ordnungsbussen", text=text, img=None, attachment=None)
            common.send_email(msg)
            ct.update_hash_file(big_bussen)
        if ct.has_changed(new_plz):
            text = f"The exported file {plz} has changed, please check.\n"
            df_plz = pd.read_csv(plz)
            text += f"PLZ before: {df_plz['Ü-Ort PLZ'].to_list()}\n"
            df_new_plz = pd.read_csv(new_plz)
            text += f"PLZ after: {df_new_plz['Ü-Ort PLZ'].to_list()}"
            msg = email_message(subject="Warning Ordnungsbussen", text=text, img=None, attachment=None)
            common.send_email(msg)
            df_plz.to_csv(os.path.join("data", "plz.csv"))
            ct.update_hash_file(new_plz)
        export_path = os.path.join("data", "Ordnungsbussen_OGD.csv")
        logging.info(f"Exporting data to {export_path}...")
        df_export.to_csv(export_path, index=False)
        common.upload_ftp(export_path, remote_path="kapo/ordnungsbussen")
        common.publish_ods_dataset_by_id("100058")

        df_all = append_coordinates(df_all)
        df_all["coordinates"] = df_all["coordinates"].astype(str).str.strip("()").str.strip("[]")
        df_all = calculate_distances(df_all)
        df_all = add_wohnviertel_columns(df_all, ods_id="100042")
        export_path_all = os.path.join("data", "Ordnungsbussen_OGD_all.csv")
        logging.info(f"Exporting all data to {export_path_all}...")
        df_all.to_csv(export_path_all, index=False)
        ct.update_hash_file(list_path)


def _points_from_lonlat(lon, lat):
    """Vectorized Point creation; returns list of shapely Points or None."""
    # shapely Point expects (x=lon, y=lat)
    pts = []
    for x, y in zip(lon, lat):
        if pd.isna(x) or pd.isna(y):
            pts.append(None)
        else:
            pts.append(Point(float(x), float(y)))
    return pts


def _parse_latlon_str(series):
    """
    Parse df['coordinates'] where values look like '47.55,7.59' (lat,lon).
    Returns two float Series: lat, lon (NaN on failure).
    """
    s = series.astype(str).str.strip()
    # allow both comma and comma+space variants
    parts = s.str.split(",", n=1, expand=True)
    if parts.shape[1] < 2:
        return pd.Series(np.nan, index=series.index), pd.Series(np.nan, index=series.index)

    lat = pd.to_numeric(parts[0].str.strip(), errors="coerce")
    lon = pd.to_numeric(parts[1].str.strip(), errors="coerce")
    return lat, lon


def add_wohnviertel_columns(df, ods_id="100473"):
    """
    Adds two columns mapping coordinates -> Wohnviertel name (wov_name),
    but only for rows where Ü-Ort PLZ != -1.
    """
    logging.info(f"Downloading Wohnviertel spatial descriptors (ODS {ods_id})...")
    gdf_wov = download_spatial_descriptors(ods_id)

    # Ensure we have the expected name column
    if "wov_name" not in gdf_wov.columns:
        raise ValueError(f"Expected column 'wov_name' not found in ODS {ods_id} shapefile columns: {gdf_wov.columns}")

    # Work only on rows with a real PLZ
    mask = df["Ü-Ort PLZ"].fillna(-1).astype(int) != -1

    # Initialize columns with NaN
    df["wohnviertel_from_gps"] = np.nan
    df["wohnviertel_from_georef"] = np.nan

    # --- 1) From GPS Breite/Länge (assuming WGS84 lat/lon) ---
    gps_lon = pd.to_numeric(df.loc[mask, "GPS Länge"], errors="coerce")
    gps_lat = pd.to_numeric(df.loc[mask, "GPS Breite"], errors="coerce")

    gdf_points_gps = gpd.GeoDataFrame(
        df.loc[mask].copy(),
        geometry=_points_from_lonlat(gps_lon, gps_lat),
        crs="EPSG:4326",
    ).dropna(subset=["geometry"])

    if not gdf_points_gps.empty:
        gdf_points_gps = gdf_points_gps.to_crs("EPSG:2056")
        joined_gps = gpd.sjoin(
            gdf_points_gps,
            gdf_wov[["wov_name", "geometry"]],
            how="left",
            predicate="within",
        )
        df.loc[joined_gps.index, "wohnviertel_from_gps"] = joined_gps["wov_name"].values

    # --- 2) From your georeferenced df['coordinates'] (stored as 'lat,lon') ---
    lat2, lon2 = _parse_latlon_str(df.loc[mask, "coordinates"])
    gdf_points_geo = gpd.GeoDataFrame(
        df.loc[mask].copy(),
        geometry=_points_from_lonlat(lon2, lat2),
        crs="EPSG:4326",
    ).dropna(subset=["geometry"])

    if not gdf_points_geo.empty:
        gdf_points_geo = gdf_points_geo.to_crs("EPSG:2056")
        joined_geo = gpd.sjoin(
            gdf_points_geo,
            gdf_wov[["wov_name", "geometry"]],
            how="left",
            predicate="within",
        )
        df.loc[joined_geo.index, "wohnviertel_from_georef"] = joined_geo["wov_name"].values

    return df


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


def calculate_distances(df):
    # Function to calculate distance between two points
    def calculate_distance(row):
        if pd.isna(row["GPS Breite"]) or pd.isna(row["GPS Länge"]) or pd.isna(row["coordinates"]):
            return float("nan")
        point1 = (row["GPS Breite"], row["GPS Länge"])
        point2 = tuple(map(float, row["coordinates"].split(",")))
        return geodesic(point1, point2).meters

    # Apply the function to each row with a progress bar
    tqdm.pandas()
    df["distance_to_coordinates"] = df.progress_apply(calculate_distance, axis=1)
    return df


def process_data_2017():
    logging.info("Reading 2017 data from csv...")
    df_2020_07_27 = pd.read_csv(
        os.path.join("data_orig", "2020_07_27/OGD_BussenDaten.csv"),
        sep=";",
        encoding="cp1252",
    )
    df_2020_07_27["Übertretungsdatum"] = pd.to_datetime(df_2020_07_27["Übertretungsdatum"], format="%d.%m.%Y")
    df_2017 = df_2020_07_27.query("Übertretungsjahr == 2017")
    return df_2017


def process_data_from_2018(list_directories, df_2017):
    logging.info("Reading 2018+ data from xslx...")
    df_all = df_2017
    for directory in list_directories:
        file = os.path.join("data_orig", directory, "OGD.xlsx")
        logging.info(f"process data from file {file}")
        df = pd.read_excel(file)
        # want to take the data from the latest file, so remove in the df I have up till now all data of datum_min and after
        datum_min = df["Übertretungsdatum"].min()
        logging.info(
            f"Earliest date is {datum_min}, add new data from this date on (and remove data after this date coming from older files)"
        )
        df_all = df_all[df_all["Übertretungsdatum"] < datum_min]
        df_all = pd.concat([df_all, df], ignore_index=True)
    return df_all


def transform_for_export(df_all):
    logging.info("Calculating weekday, weekday number, and its combination...")
    df_all["Übertretungswochentag"] = df_all["Übertretungsdatum"].dt.weekday.apply(lambda x: common.weekdays_german[x])
    # Translate from Mo=0 to So=1, Mo=2 etc. to be backward.compatible with previously used SAS code
    df_all["ÜbertretungswochentagNummer"] = df_all["Übertretungsdatum"].dt.weekday.replace(
        {0: 2, 1: 3, 2: 4, 3: 5, 4: 6, 5: 7, 6: 1}
    )
    df_all["Wochentag"] = (
        df_all["ÜbertretungswochentagNummer"].astype(str) + " " + df_all["Übertretungswochentag"].astype(str)
    )

    logging.info("Replacing wrong PLZ...")
    plz_replacements = {
        4002: 4053,
        4009: 4055,
        4019: 4057,
        4031: 4056,
        4103: 4059,
        4123: 4055,
        4127: 4052,
        4000: -1,
        np.nan: -1,
        4030: 4056,
        405: 4052,
        0000: 4051,
        4102: 4053,
        4132: -1
    }
    df_all["Ü-Ort PLZ"] = df_all["Ü-Ort PLZ"].replace(plz_replacements).astype(int)

    logging.info("Replacing old BuZi with new ones using lookup table...")
    df_lookup = pd.read_excel(os.path.join("data_orig", "2022_06_30", "Lookup-Tabelle BuZi.xlsx"))
    df_all["BuZi"] = df_all["BuZi"].replace(df_lookup.ALT.to_list(), df_lookup.NEU.to_list())

    logging.info("Cleaning up data for export...")
    df_all["Laufnummer"] = range(1, 1 + len(df_all))
    df_all["BuZi Text"] = df_all["BuZi Text"].str.replace('"', "'")
    # Remove newline, carriage return, and tab, see https://stackoverflow.com/a/67541987
    df_all["BuZi Text"] = df_all["BuZi Text"].str.replace(r"\r+|\n+|\t+", "", regex=True)

    df_bussen_big = df_all.query("`Bussen-Betrag` > 300")

    df_all = df_all.query("`Bussen-Betrag` > 0")
    df_all = df_all.query("`Bussen-Betrag` <= 300")
    logging.info("Exporting data for high Bussen, and for all found PLZ...")
    df_bussen_big.to_csv(os.path.join("data", "big_bussen.csv"))
    df_plz = pd.DataFrame(sorted(df_all["Ü-Ort PLZ"].unique()), columns=["Ü-Ort PLZ"])
    df_plz.to_csv(os.path.join("data", "new_plz.csv"))
    df_export = df_all[
        [
            "Laufnummer",
            "KAT BEZEICHNUNG",
            "Wochentag",
            "ÜbertretungswochentagNummer",
            "Übertretungswochentag",
            "Übertretungsmonat",
            "Übertretungsjahr",
            "GK-Limite",
            "Ü-Ort PLZ",
            "Ü-Ort ORT",
            "Bussen-Betrag",
            "BuZi",
            "BuZi Zus.",
            "BuZi Text",
        ]
    ]
    df_export = df_export.copy()
    return df_export, df_all


def append_coordinates(df):
    df["address"] = (
        df["Ü-Ort STR"].astype(str)
        + " "
        + df["Ü-Ort STR-NR"].astype(str)
        + ", "
        + df["Ü-Ort PLZ"].astype(str)
        + " "
        + df["Ü-Ort ORT"].astype(str)
    )
    df_geb_eing = get_gebaeudeeingaenge()
    gdf_streets = get_street_shapes()
    # First try to get coordinates from Gebäudeeingänge directly
    df = get_coordinates_from_gwr(df, df_geb_eing)
    # Then try to get coordinates from Nominatim
    df = get_coordinates_from_nomatim_and_gwr(df, df_geb_eing)
    # Finally, append shapes of streets
    df = get_shapes_for_streets(df, gdf_streets)
    return df


def get_gebaeudeeingaenge():
    raw_data_file = os.path.join("data", "gebaeudeeingaenge.csv")
    logging.info(f"Downloading Gebäudeeingänge from ods to file {raw_data_file}...")
    r = common.requests_get("https://data.bs.ch/api/records/1.0/download?dataset=100231")
    with open(raw_data_file, "wb") as f:
        f.write(r.content)
    return pd.read_csv(raw_data_file, sep=";")


def get_street_shapes():
    path_to_folder = os.path.join("data", "streets")
    logging.info(f"Downloading street shapes from ods to file {path_to_folder}...")
    r = common.requests_get("https://data.bs.ch/explore/dataset/100189/download/?format=shp")
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall(path_to_folder)
    path_to_shp = os.path.join(path_to_folder, "100189.shp")
    gdf = gpd.read_file(path_to_shp, encoding="utf-8")
    return gdf


def get_coordinates_from_gwr(df, df_geb_eing):
    df_geb_eing["address"] = (
        df_geb_eing["strname"]
        + " "
        + df_geb_eing["deinr"].astype(str)
        + ", "
        + df_geb_eing["dplz4"].astype(str)
        + " "
        + df_geb_eing["dplzname"]
    )
    df = df.merge(df_geb_eing[["address", "eingang_koordinaten"]], on="address", how="left")
    df.rename(columns={"eingang_koordinaten": "coordinates"}, inplace=True)
    return df


def get_coordinates_from_nominatim(df, cached_coordinates, use_rapidfuzz=False, street_series=None):
    geolocator = Nominatim(user_agent="zefix_handelsregister")
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)
    shp_file_path = os.path.join("data", "shp_bs", "bs.shp")
    gdf_bs = gpd.read_file(shp_file_path)
    # If there are missing coordinates, try to get them from Nominatim
    # and there is no nan in address
    missing_coords = df[df["coordinates"].isna() & ~df["address"].str.contains(" nan, ")]
    for index, row in missing_coords.iterrows():
        if use_rapidfuzz:
            closest_streetname = find_closest_streetname(str(row["Ü-Ort STR"]), street_series)
            row["closest_adress"] = (
                closest_streetname
                + " "
                + str(row["Ü-Ort STR-NR"])
                + ", "
                + str(row["Ü-Ort PLZ"])
                + " "
                + str(row["Ü-Ort ORT"]).split(" ")[0]
            )
            address = row["closest_adress"]
        else:
            address = row["address"]
        if address not in cached_coordinates:
            try:
                location = geocode(address)
                if location:
                    point = Point(location.longitude, location.latitude)
                    is_in_bs = (
                        "Basel" in row["Ü-Ort ORT"] or "Riehen" in row["Ü-Ort ORT"] or "Bettingen" in row["Ü-Ort ORT"]
                    )
                    if is_in_bs != gdf_bs.contains(point).any():
                        logging.info(f"Location {location} is not in Basel-Stadt")
                        continue
                    cached_coordinates[address] = (
                        location.latitude,
                        location.longitude,
                    )
                    df.at[index, "coordinates"] = cached_coordinates[address]
                else:
                    logging.info(f"Location not found for address: {address}")
                    cached_coordinates[address] = None
            except Exception as e:
                logging.info(f"Error occurred for address {address}: {e}")
                time.sleep(5)
        else:
            logging.info("Using cached coordinates for address")
            df.at[index, "coordinates"] = cached_coordinates[address]
    return df, cached_coordinates


def get_coordinates_from_nomatim_and_gwr(df, df_geb_eing):
    # Get lookup table for addresses that could not be found
    path_lookup_table = os.path.join("data", "addr_to_coords_lookup_table.json")
    if os.path.exists(path_lookup_table):
        with open(path_lookup_table, "r") as f:
            cached_coordinates = json.load(f)
    else:  # Create empty lookup table since it does not exist yet
        cached_coordinates = {}

    df, cached_coordinates = get_coordinates_from_nominatim(df, cached_coordinates)

    # Last resort: Find closest street in Gebäudeeingänge (https://data.bs.ch/explore/dataset/100231)
    # with help of rapidfuzz and then get coordinates from Nominatim
    street_series = df_geb_eing["strname"]
    df, cached_coordinates = get_coordinates_from_nominatim(
        df, cached_coordinates, use_rapidfuzz=True, street_series=street_series
    )

    # Save lookup table
    with open(path_lookup_table, "w") as f:
        json.dump(cached_coordinates, f)
    return df


def find_closest_streetname(street, street_series):
    if street:
        closest_address, _, _ = process.extractOne(str(street), street_series)
        logging.info(f"Closest address for {street} according to fuzzy matching is: {closest_address}")
        return closest_address
    return street


def get_shapes_for_streets(df, gdf_streets):
    street_names = df["Ü-Ort STR"].unique()
    for street_name in street_names:
        # Find closest street name
        closest_street = find_closest_streetname(street_name, gdf_streets["strname"])
        # Get shape of closest street
        street_shape = gdf_streets[gdf_streets["strname"] == closest_street].geometry
        # Append shape and closest street name to df
        df.loc[df["Ü-Ort STR"] == street_name, "street_shape"] = street_shape
        df.loc[df["Ü-Ort STR"] == street_name, "closest_streetname"] = closest_street
    return df


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f"Executing {__file__}...")
    main()
    logging.info("Job successful!")
