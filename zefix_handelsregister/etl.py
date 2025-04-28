import datetime
import json
import logging
import os
import pathlib
import time

import common
import common.change_tracking as ct
import geopandas as gpd
import pandas as pd
from geopy.extra.rate_limiter import RateLimiter
from geopy.geocoders import Nominatim
from rapidfuzz import process
from shapely.geometry import Point
from SPARQLWrapper import JSON, SPARQLWrapper


def main():
    # Get NOGA data (Temporarily deactivated)
    # df_burweb = get_noga_data()
    # Get Zefix and BurWeb data for all cantons
    get_data_of_all_cantons()

    # Extract data for Basel-Stadt and make ready for data.bs.ch
    file_name = "100330_zefix_firmen_BS.csv"
    path_export = os.path.join(
        pathlib.Path(__file__).parents[0], "data", "export", file_name
    )
    df_BS = work_with_BS_data()
    df_BS.to_csv(path_export, index=False)
    common.update_ftp_and_odsp(path_export, "zefix_handelsregister", "100330")
    create_diff_files(path_export)


def create_diff_files(path_to_new):
    logging.info("Creating diff files...")
    # Load last version of the file
    df_new = pd.read_csv(path_to_new)
    path_to_last = os.path.join(
        pathlib.Path(__file__).parents[0], "data", "handelsregister_last_version.csv"
    )
    if os.path.exists(path_to_last):
        df_last = pd.read_csv(path_to_last)
        # Find new rows if any
        new_rows = ct.find_new_rows(df_last, df_new, "company_uid")
        path_export = os.path.join(
            pathlib.Path(__file__).parents[0],
            "data",
            "diff_files",
            f"handelsregister_new_{datetime.date.today()}.csv",
        )
        upload_rows_to_ftp(new_rows, path_export)
        # Find modified rows if any
        deprecated_rows, updated_rows = ct.find_modified_rows(
            df_last, df_new, "company_uid"
        )
        path_export = os.path.join(
            pathlib.Path(__file__).parents[0],
            "data",
            "diff_files",
            f"handelsregister_deprecated_{datetime.date.today()}.csv",
        )
        upload_rows_to_ftp(deprecated_rows, path_export)
        path_export = os.path.join(
            pathlib.Path(__file__).parents[0],
            "data",
            "diff_files",
            f"handelsregister_updated_{datetime.date.today()}.csv",
        )
        upload_rows_to_ftp(updated_rows, path_export)
        # Find deleted rows if any
        deleted_rows = ct.find_deleted_rows(df_last, df_new, "company_uid")
        path_export = os.path.join(
            pathlib.Path(__file__).parents[0],
            "data",
            "diff_files",
            f"handelsregister_deleted_{datetime.date.today()}.csv",
        )
        upload_rows_to_ftp(deleted_rows, path_export)
    # Save new version of the file as the last version
    df_new.to_csv(path_to_last, index=False)


def upload_rows_to_ftp(df, path_export):
    if len(df) > 0:
        df.to_csv(path_export, index=False)
        common.upload_ftp(path_export, remote_path="zefix_handelsregister/diff_files")


def get_data_of_all_cantons():
    sparql = SPARQLWrapper("https://lindas.admin.ch/query")
    sparql.setReturnFormat(JSON)
    # Iterate over all cantons
    for i in range(1, 27):
        logging.info(f"Getting data for canton {i}...")
        # Query can be tested and adjusted here: https://ld.admin.ch/sparql/#
        sparql.setQuery(
            """
                PREFIX schema: <http://schema.org/>
                PREFIX admin: <https://schema.ld.admin.ch/>
                SELECT ?canton_id ?canton ?short_name_canton ?district_id ?district_de ?district_fr ?district_it ?district_en ?muni_id ?municipality ?company_uri ?company_uid ?company_legal_name ?type_id ?company_type_de ?company_type_fr ?adresse ?plz ?locality 
                WHERE {
                    # Get information of the company
                    ?company_uri a admin:ZefixOrganisation ;
                        schema:legalName ?company_legal_name ;
                        admin:municipality ?muni_id ;
                        schema:identifier ?company_identifiers ;
                        schema:address ?adr ;
                        schema:additionalType ?type_id .
                    # Get Identifier UID, but filter by CompanyUID, since there are three types of ID's
                    ?company_identifiers schema:value ?company_uid .
                    ?company_identifiers schema:name "CompanyUID" .
                    ?muni_id schema:name ?municipality .
                    ?type_id schema:name ?company_type_de .
                    # Get address-information (do not take c/o-information in, since we get fewer results)
                    ?adr schema:streetAddress ?adresse ;
                        schema:addressLocality ?locality ;
                        schema:postalCode ?plz .
                    # Finally filter by Companies that are in a certain canton
                    <https://ld.admin.ch/canton/"""
            + str(i)
            + """> schema:containsPlace ?muni_id ;
                        schema:legalName ?canton ;
                        schema:alternateName ?short_name_canton ;
                        schema:identifier ?canton_id .
                    ?district_id schema:containsPlace ?muni_id ;
                        schema:name ?district_de .

                    # Optional to get district names in French
                    OPTIONAL {
                        ?district_id schema:containsPlace ?muni_id ;
                            schema:name ?district_fr .
                        FILTER langMatches(lang(?district_fr), "fr")
                    }

                    # Optional to get district names in Italian
                    OPTIONAL {
                        ?district_id schema:containsPlace ?muni_id ;
                            schema:name ?district_it .
                        FILTER langMatches(lang(?district_it), "it")
                    }

                    # Optional to get district names in English
                    OPTIONAL {
                        ?district_id schema:containsPlace ?muni_id ;
                            schema:name ?district_en .
                        FILTER langMatches(lang(?district_en), "en")
                    }

                    # Optional to get company types in French
                    OPTIONAL {
                        ?type_id schema:name ?company_type_fr .
                        FILTER langMatches(lang(?company_type_fr), "fr")
                    }

                    # Filter by company-types that are german (otherwise result is much bigger)
                    FILTER langMatches(lang(?district_de), "de") .
                    FILTER langMatches(lang(?company_type_de), "de") .
                }
                ORDER BY ?company_legal_name
            """
        )

        results = sparql.query().convert()
        results_df = pd.json_normalize(results["results"]["bindings"])
        results_df = results_df.filter(regex="value$", axis=1)
        new_column_names = {
            col: col.replace(".value", "") for col in results_df.columns
        }
        results_df = results_df.rename(columns=new_column_names)
        # Split the column 'address' into zusatz and street,
        # but if there is no zusatz, then street is in the first column
        temp_df = results_df["adresse"].str.split("\n", expand=True)
        results_df.loc[results_df["adresse"].str.contains("\n"), "zusatz"] = temp_df[0]
        results_df.loc[results_df["adresse"].str.contains("\n"), "street"] = temp_df[1]
        results_df.loc[~results_df["adresse"].str.contains("\n"), "street"] = temp_df[0]
        results_df = results_df.drop(columns=["adresse"])

        short_name_canton = results_df["short_name_canton"][0]
        # Add url to cantonal company register
        # Transform UID in format CHE123456789 to format CHE-123.456.789
        company_uid_str = results_df["company_uid"].str.replace(
            "CHE([0-9]{3})([0-9]{3})([0-9]{3})", "CHE-\\1.\\2.\\3", regex=True
        )
        results_df["url_cantonal_register"] = (
            "https://"
            + short_name_canton.lower()
            + ".chregister.ch/cr-portal/auszug/auszug.xhtml?uid="
            + company_uid_str
        )

        """
        # Get noga data
        results_df = pd.merge(results_df, df_burweb, on='company_uid', how='left')
        """

        file_name = f"companies_{short_name_canton}.csv"
        path_export = os.path.join(
            pathlib.Path(__file__).parents[0], "data", "all_cantons", file_name
        )
        results_df.to_csv(path_export, index=False)
        if ct.has_changed(path_export):
            logging.info(f"Exporting {file_name} to FTP server")
            common.upload_ftp(
                path_export, remote_path="zefix_handelsregister/all_cantons"
            )
            ct.update_hash_file(path_export)


def get_gebaeudeeingaenge():
    raw_data_file = os.path.join(
        pathlib.Path(__file__).parent, "data", "gebaeudeeingaenge.csv"
    )
    logging.info(f"Downloading Gebäudeeingänge from ods to file {raw_data_file}...")
    r = common.requests_get(
        "https://data.bs.ch/api/records/1.0/download?dataset=100231"
    )
    with open(raw_data_file, "wb") as f:
        f.write(r.content)
    return pd.read_csv(raw_data_file, sep=";")


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
    df = df.merge(
        df_geb_eing[["address", "eingang_koordinaten"]], on="address", how="left"
    )
    df.rename(columns={"eingang_koordinaten": "coordinates"}, inplace=True)
    return df


def get_coordinates_from_nominatim(
    df, cached_coordinates, use_rapidfuzz=False, street_series=None
):
    geolocator = Nominatim(user_agent="zefix_handelsregister")
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)
    shp_file_path = os.path.join(
        pathlib.Path(__file__).parents[0], "data", "shp_bs", "bs.shp"
    )
    gdf_bs = gpd.read_file(shp_file_path)
    missing_coords = df[df["coordinates"].isna()]
    for index, row in missing_coords.iterrows():
        if use_rapidfuzz:
            closest_streetname = find_closest_streetname(row["street"], street_series)
            address = (
                closest_streetname
                + ", "
                + row["plz"]
                + " "
                + row["locality"].split(" ")[0]
            )
        else:
            address = row["address"]
        if address not in cached_coordinates:
            try:
                location = geocode(address)
                if location:
                    point = Point(location.longitude, location.latitude)
                    is_in_bs = (
                        "Basel" in row["locality"]
                        or "Riehen" in row["locality"]
                        or "Bettingen" in row["locality"]
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
            except Exception as e:
                logging.info(f"Error occurred for address {address}: {e}")
                time.sleep(5)
        else:
            logging.info(f"Using cached coordinates for address: {address}")
            df.at[index, "coordinates"] = cached_coordinates[address]
    return df, cached_coordinates


def get_coordinates_from_nomatim_and_gwr(df, df_geb_eing):
    # Get lookup table for addresses that could not be found
    path_lookup_table = os.path.join(
        pathlib.Path(__file__).parents[0], "data", "addr_to_coords_lookup_table.json"
    )
    if os.path.exists(path_lookup_table):
        with open(path_lookup_table, "r") as f:
            cached_coordinates = json.load(f)
    else:  # Create empty lookup table since it does not exist yet
        cached_coordinates = {}

    df, cached_coordinates = get_coordinates_from_nominatim(df, cached_coordinates)

    # Last resort: Find closest street in Gebäudeeingänge (https://data.bs.ch/explore/dataset/100231)
    # with help of rapidfuzz and then get coordinates from Nominatim
    street_series = df_geb_eing["strname"] + " " + df_geb_eing["deinr"].astype(str)
    df, cached_coordinates = get_coordinates_from_nominatim(
        df, cached_coordinates, use_rapidfuzz=True, street_series=street_series
    )

    # Save lookup table
    with open(path_lookup_table, "w") as f:
        json.dump(cached_coordinates, f)
    return df


def find_closest_streetname(street, street_series):
    if street:
        closest_address, _, _ = process.extractOne(street, street_series)
        logging.info(
            f"Closest address for {street} according to fuzzy matching is: {closest_address}"
        )
        return closest_address
    return street


def work_with_BS_data():
    path_BS = os.path.join(
        pathlib.Path(__file__).parents[0], "data", "all_cantons", "companies_BS.csv"
    )
    df_BS = pd.read_csv(path_BS)
    # Pre-processing
    df_BS["plz"] = df_BS["plz"].fillna(0).astype(int).astype(str).replace("0", "")
    df_BS["street"] = df_BS["street"].str.replace("Str.", "Strasse", regex=False)
    df_BS["street"] = df_BS["street"].str.replace("str.", "strasse", regex=False)
    # Replace *St. followed by a letter with *St. * and then the letter
    df_BS["street"] = df_BS["street"].str.replace(
        "St\.([a-zA-Z])", "St. \\1", regex=True
    )
    df_BS["address"] = (
        df_BS["street"]
        + ", "
        + df_BS["plz"]
        + " "
        + df_BS["locality"].str.split(" ").str[0]
    )
    # Get data of Gebäudeeingänge https://data.bs.ch/explore/dataset/100231
    df_geb_eing = get_gebaeudeeingaenge()

    # First try to get coordinates from Gebäudeeingänge directly
    df_BS = get_coordinates_from_gwr(df_BS, df_geb_eing)
    # Then try to get coordinates from Nominatim
    df_BS = get_coordinates_from_nomatim_and_gwr(df_BS, df_geb_eing)

    return df_BS[
        [
            "company_type_de",
            "company_legal_name",
            "company_uid",
            "municipality",
            "street",
            "zusatz",
            "plz",
            "locality",
            "address",
            "coordinates",
            "url_cantonal_register",
            "type_id",
            "company_uri",
            "muni_id",
        ]
    ]


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f"Executing {__file__}...")
    main()
    logging.info("Job successful")
