import io
import logging
import os
import shutil
import sys
import urllib.parse
import xml.etree.ElementTree as ET
from datetime import datetime

import common
import geopandas as gpd
import pandas as pd
import requests
from common import change_tracking as ct
from dotenv import load_dotenv
from owslib.wfs import WebFeatureService

load_dotenv()

FTP_SERVER = os.getenv("FTP_SERVER")
FTP_USER = os.getenv("FTP_USER_01")
FTP_PASS = os.getenv("FTP_PASS_01")


# Create new ‘Title’ column in df_wfs (Kanton Basel-Stadt WMS/**Hundesignalisation**/Hundeverbot)
def extract_second_hier_name(row, df2):
    # extract from wms the second name in the hierarchy
    # Search in df2 for the matching name
    # df2 is the df of wms
    matching_row = df2[df2["Name"] == row["Name"]]
    if not matching_row.empty:
        hier_name = matching_row["Hier_Name"].values[0]
        hier_parts = hier_name.split("/")
        return hier_parts[1] if len(hier_parts) > 1 else None


def to_iso_date(wert: str) -> str:
    for fmt in ("%d.%m.%Y", "%Y/%m/%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(wert, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    raise ValueError(f"Unknown date format: {wert}")


# Function for retrieving and parsing WMS GetCapabilities
def get_wms_capabilities(url_wms):
    response = requests.get(url=url_wms, verify=False)
    xml_data = response.content
    root = ET.fromstring(xml_data)
    namespaces = {"wms": "http://www.opengis.net/wms"}
    return root, namespaces


# Recursive function to traverse the layer hierarchy and save the paths
def extract_layers(layer_element, namespaces, data, name_hierarchy=None, title_hierarchy=None):
    # Find the name and title of the current layer
    name_element = layer_element.find("wms:Name", namespaces)
    title_element = layer_element.find("wms:Title", namespaces)

    layer_name = name_element.text if name_element is not None else None
    layer_title = title_element.text if title_element is not None else None

    # If the layer has a name and a title, set up the hierarchy path
    if layer_name is not None and layer_title is not None:
        # Update the hierarchy path
        current_name_hierarchy = f"{name_hierarchy}/{layer_name}" if name_hierarchy else layer_name
        current_title_hierarchy = f"{title_hierarchy}/{layer_title}" if title_hierarchy else layer_title

        # Check whether there are sub-layers
        sublayers = layer_element.findall("wms:Layer", namespaces)

        if sublayers:
            # If there are sublayers, go through them recursively
            for sublayer in sublayers:
                extract_layers(
                    sublayer,
                    namespaces,
                    data,
                    current_name_hierarchy,
                    current_title_hierarchy,
                )
        else:
            # If there are no sub-layers, add the deepest layer to the data
            data.append(
                [
                    layer_name,
                    layer_title,
                    current_name_hierarchy,
                    current_title_hierarchy,
                ]
            )


# Main function to process WMS data and create a DataFrame
def process_wms_data(url_wms):
    root, namespaces = get_wms_capabilities(url_wms)
    capability_layer = root.find(".//wms:Capability/wms:Layer", namespaces)

    # Initialize the data list outside the recursive function
    data = []
    if capability_layer is not None:
        extract_layers(capability_layer, namespaces, data)
        df_wms = pd.DataFrame(data, columns=["Name", "Layer", "Hier_Name", "Hier_Titel"])
    return df_wms


# Function for retrieving and parsing WFS GetCapabilities
def process_wfs_data(wfs):
    # Retrieve the list of available layers (feature types) and their metadata
    feature_list = []
    for feature in wfs.contents:
        feature_list.append({"Name": feature})
    # Convert to DataFrame and display
    df_wfs = pd.DataFrame(feature_list)
    # Clearing the column 'Layer Name' ( remove 'ms:')
    df_wfs["Name"] = df_wfs["Name"].str.replace("ms:", "", regex=False)
    return df_wfs


# Function to create Map_links
def create_map_links(geometry, p1, p2):
    # encode p1, p2
    p1 = urllib.parse.quote(p1)
    p2 = urllib.parse.quote(p2)
    # check whether the data is a geo point or geo shape
    logging.info(f"the type of the geometry is {geometry.geom_type}")
    # geometry_types = gdf.iloc[0][geometry].geom_type
    if geometry.geom_type == "Polygon":
        centroid = geometry.centroid
    else:
        centroid = geometry

    #  create a Map_links
    lat, lon = centroid.y, centroid.x
    Map_links = f"https://opendatabs.github.io/map-links/?lat={lat}&lon={lon}&p1={p1}&p2={p2}"
    return Map_links


no_file_copy = False
if "no_file_copy" in sys.argv:
    no_file_copy = True
    logging.info("Proceeding without copying files...")
else:
    logging.info("Proceeding with copying files...")


def get_metadata_cat(df, thema):
    filtered_df = df[df["Thema"] == thema]
    if filtered_df.empty:
        return None, None
    row = filtered_df.iloc[0]
    return row["Aktualisierung"], row["Metadaten"]


def remove_empty_string_from_list(string_list):
    return list(filter(None, string_list))


def extract_meta_geocat(geocat_uid):
    # extract the metadata form geocat
    geocat_url = f"https://www.geocat.ch/geonetwork/srv/api/records/{geocat_uid}/formatters/xml"
    response = common.requests_get(geocat_url)
    if response.status_code == 200:
        logging.info(f"Data successfully fetched from {geocat_url}")
    else:
        logging.warning(f"Failed to fetch data from {geocat_url}: Status {response.status_code}")
    xml_data = response.content

    # parsing the XML data
    namespace = {
        "che": "http://www.geocat.ch/2008/che",
        "gmd": "http://www.isotc211.org/2005/gmd",
        "gco": "http://www.isotc211.org/2005/gco",
    }
    root = ET.fromstring(xml_data)

    # Extract "Publizierende Organisation"
    position_name = root.find(
        ".//gmd:pointOfContact/che:CHE_CI_ResponsibleParty/gmd:positionName/gco:CharacterString",
        namespace,
    )
    # Extract "Herausgeber"
    first_name = root.find(
        ".//gmd:pointOfContact/che:CHE_CI_ResponsibleParty/che:individualFirstName/gco:CharacterString",
        namespace,
    )
    # Extract the description
    Beschreibung = root.find(
        ".//gmd:abstract/gmd:PT_FreeText/gmd:textGroup/gmd:LocalisedCharacterString",
        namespace,
    )
    # Extract dcat.created
    xpath_date_time = (
        ".//che:CHE_MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:date/gmd:CI_Date/gmd:date/gco:DateTime"
    )
    xpath_date = ".//che:CHE_MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:date/gmd:CI_Date/gmd:date/gco:Date"
    # Try to finde the path
    date_time_node = root.find(xpath_date_time, namespace)
    date_node = root.find(xpath_date, namespace)
    if date_node is not None:
        date_value = date_node.text
    else:
        date_value = date_time_node.text
    return position_name.text, first_name.text, Beschreibung.text, date_value


# Function for saving FGI geodata for each layer name
def save_geodata_for_layers(wfs, df_fgi, file_path):
    meta_data = pd.read_excel(os.path.join("data", "Metadata.xlsx"), na_filter=False)
    path_cat = os.path.join("data", "100410_geodatenkatalog.csv")
    df_cat = pd.read_csv(path_cat, sep=";")
    metadata_for_ods = []
    logging.info("Iterating over datasets...")
    failed = []
    for index, row in meta_data.iterrows():
        if row["import"]:
            # Which shapes need to be imported to ods?
            shapes_to_load = remove_empty_string_from_list(row["Layers"].split(";"))
            num_files = len(shapes_to_load)
            if num_files == 0:  # load all shapes.
                # find the list of shapes in fgi_list
                ind_list = df_fgi[df_fgi["Gruppe"] == row["Gruppe"]].index
                shapes_to_load = df_fgi.iloc[ind_list]["Name"].values[0]
            gdf_result = gpd.GeoDataFrame()
            for shapefile in shapes_to_load:
                try:
                    # Retrieve and save the geodata for each layer name in shapes_to_load
                    response = wfs.getfeature(typename=shapefile)
                    gdf = gpd.read_file(io.BytesIO(response.read()))
                    gdf_result = pd.concat([gdf_result, gdf])
                except Exception as e:
                    logging.error(f"Error loading layer '{shapefile}': {e}")
                    failed.append({"Layer": shapefile, "Error": str(e)})
                    continue

            # creat a maps_urls
            if row["create_map_urls"]:
                logging.info(f"Create Map urls for {row['titel_nice']}")
                # Extract params from redirect
                link = row["mapbs_link"].replace("www.geo.bs.ch", "https://geo.bs.ch")
                response = requests.get(link, allow_redirects=True)
                # Find the redircet
                redirect_link = response.url
                # Extract the part of the URL after the '?'
                query_string = redirect_link.split("?")[1]
                # Splitting the parameters at'&'
                params = query_string.split("&")
                tree_groups = [param.replace("tree_groups=", "") for param in params if "tree_groups=" in param][0]
                tree_group_layers_ = [
                    param.replace("tree_group_layers_", "") for param in params if "tree_group_layers_" in param
                ][0]
                gdf_result = gdf_result.to_crs(epsg=4326)
                gdf_result["Map Links"] = gdf_result.apply(
                    lambda row2: create_map_links(row2["geometry"], tree_groups, tree_group_layers_),
                    axis=1,
                    result_type="expand",
                )
            # save the geofile locally
            titel = row["Gruppe"]
            titel_dir = os.path.join(file_path, titel)
            os.makedirs(titel_dir, exist_ok=True)
            file_name = f"{row['Dateiname']}.gpkg"
            geopackage_file = os.path.join(titel_dir, file_name)
            save_gpkg(gdf_result, file_name, geopackage_file)
            # save in ftp server
            ftp_remote_dir = "harvesters/GVA/data"
            common.upload_ftp(geopackage_file, FTP_SERVER, FTP_USER, FTP_PASS, ftp_remote_dir)
            # In some geocat URLs there's a tab character, remove it.
            aktualisierung, geocat = get_metadata_cat(df_cat, titel)
            if pd.isna(aktualisierung) or str(aktualisierung).strip() == "":
                aktualisierung = ""
            else:
                aktualisierung = to_iso_date(str(aktualisierung).strip())
            geocat_url = row["geocat"] if len(row["geocat"]) > 0 else geocat
            geocat_uid = geocat_url.rsplit("/", 1)[-1].replace("\t", "")
            (
                publizierende_organisation,
                herausgeber,
                geocat_description,
                dcat_created,
            ) = extract_meta_geocat(geocat_uid)
            ods_id = row["ods_id"]
            schema_file = ""
            if row["schema_file"]:
                schema_file = f"{ods_id}.csv"
            # Check if a description to the current shape is given in Metadata.csv
            description = row["beschreibung"]
            dcat_ap_ch_domain = ""
            if str(row["dcat_ap_ch.domain"]) != "":
                dcat_ap_ch_domain = str(row["dcat_ap_ch.domain"])

            # Add entry to harvester file
            metadata_for_ods.append(
                {
                    "ods_id": ods_id,
                    "name": geocat_uid + ":" + row["Dateiname"],
                    "title": row["titel_nice"],
                    "description": description if len(description) > 0 else geocat_description,
                    # Only add nonempty strings as references
                    "references": "; ".join(filter(None, [row["mapbs_link"], row["geocat"], row["referenz"]])),
                    "theme": str(row["theme"]),
                    "keyword": str(row["keyword"]),
                    "dcat_ap_ch.domain": dcat_ap_ch_domain,
                    "dcat_ap_ch.rights": "NonCommercialAllowed-CommercialAllowed-ReferenceRequired",
                    "dcat.contact_name": "Open Data Basel-Stadt",
                    "dcat.contact_email": "opendata@bs.ch",
                    "dcat.created": dcat_created,
                    "dcat.creator": herausgeber,
                    "dcat.accrualperiodicity": row["dcat.accrualperiodicity"],
                    "attributions": "Geodaten Kanton Basel-Stadt",
                    "publisher": herausgeber,
                    "dcat.issued": row["dcat.issued"],
                    "dcat.relation": "; ".join(filter(None, [row["mapbs_link"], row["geocat"], row["referenz"]])),
                    "modified": aktualisierung,
                    "language": "de",
                    "publizierende-organisation": publizierende_organisation,
                    # Concat tags from csv with list of fixed tags, remove duplicates by converting to set, remove empty string list comprehension
                    "tags": ";".join([i for i in list(set(row["tags"].split(";") + ["opendata.swiss"])) if i != ""]),
                    "geodaten-modellbeschreibung": row["modellbeschreibung"],
                    "source_dataset": "https://data-bs.ch/opendatasoft/harvesters/GVA/data/" + file_name,
                    "schema_file": schema_file,
                }
            )

    pd.DataFrame(failed).to_excel("data/wfs_failed_layers.xlsx", index=False)
    # Save harvester file
    if len(metadata_for_ods) > 0:
        ods_metadata = pd.concat(
            [pd.DataFrame(), pd.DataFrame(metadata_for_ods)],
            ignore_index=True,
            sort=False,
        )
        ods_metadata_filename = os.path.join("data", "Opendatasoft_Export_GVA_GPKG.csv")
        ods_metadata.to_csv(ods_metadata_filename, index=False, sep=";", encoding="utf-8")
    if ct.has_changed(ods_metadata_filename) and (not no_file_copy):
        logging.info(f"Uploading ODS harvester file {ods_metadata_filename} to FTP Server...")
        common.upload_ftp(ods_metadata_filename, FTP_SERVER, FTP_USER, FTP_PASS, "harvesters/GVA")
        ct.update_hash_file(ods_metadata_filename)

    # Upload each schema_file
    logging.info("Uploading ODS schema files to FTP Server...")

    for schemafile in ods_metadata["schema_file"].unique():
        if schemafile != "":
            schemafile_with_path = os.path.join("data", "schema_files", schemafile)
            if ct.has_changed(schemafile_with_path) and (not no_file_copy):
                logging.info(f"Uploading ODS schema file to FTP Server: {schemafile_with_path}...")
                common.upload_ftp(
                    schemafile_with_path,
                    FTP_SERVER,
                    FTP_USER,
                    FTP_PASS,
                    "harvesters/GVA",
                )
                ct.update_hash_file(schemafile_with_path)
    else:
        logging.info("Harvester File contains no entries, no upload necessary.")


# Get the names of columns for each layers and save it in a csv-schema
def get_name_col(wfs, df_wfs):
    # Iterate over all rows in DataFrame `df_wfs`
    for index, row in df_wfs.iterrows():
        layer = row["Name"]
        titel_folder = row["Gruppe"]
        folder_path = os.path.join("data", "schema_files/templates", titel_folder)
        logging.info(f"Load layer: {layer} into folder: {titel_folder}")
        try:
            # Create folder if it does not exist
            if not os.path.exists(folder_path):
                os.makedirs(folder_path)

            # GetFeature query with GeoJSON output
            response = wfs.getfeature(typename=layer)

            # Load GeoJSON into GeoDataFrame
            gdf = gpd.read_file(io.BytesIO(response.read()))

            # Extract column names
            spalten_namen = gdf.columns.tolist()

            schema_data = {
                "name": spalten_namen,
                "label": ["" for _ in spalten_namen],
                "description": ["" for _ in spalten_namen],
            }
            schema_df = pd.DataFrame(schema_data)
            # Creat a path for the CSV_file
            file_path = os.path.join(folder_path, f"{layer}.csv")
            # Save CSV
            schema_df.to_csv(file_path, index=False, sep=";")

            logging.info(f"Column names of layer '{layer}' successfully saved to '{file_path}'.")
        except Exception as e:
            logging.error(f"Error loading layer '{layer}': {e}")

    logging.info("All column names were saved successfully.")


# Get the number of columns for each layer
def get_num_col(wfs, df_fgi):
    # Iterate over all rows in DataFrame `df_fgi
    for index, row in df_fgi.iterrows():
        layers = row["Name"]
        # List for storing the results
        results = []
        # Retrieve layers from WFS and save them directly to GeoPackage
        for layer in layers:
            try:
                logging.info(f"load layer: {layer}")
                response = wfs.getfeature(typename=layer)
                gdf = gpd.read_file(io.BytesIO(response.read()))
                anzahl_spalten = gdf.shape[1]
                # Ergebnis speichern
                results.append({"Layer": layer, "Anzahl_Spalten": anzahl_spalten})
                logging.info(f"layer '{layer}' successfully loaded and number of columns saved.")
            except Exception as e:
                # Fehler protokollieren und Layer überspringen
                logging.error(f"Error loading layer '{layer}': {e}")
                results.append({"Layer": layer, "Anzahl_Spalten": "Fehler"})
        # DataFrame mit den Ergebnissen erstellen
        file_name = row["Gruppe"]
        file_path = os.path.join("data", f"schema_files/templates/{file_name}", "Anzahl der Spalten")
        df_results = pd.DataFrame(results)
        # Ergebnisse in eine Excel-Datei speichern
        df_results.to_csv(f"{file_path}.csv", index=False, sep=";")


def save_gpkg(gdf, file_name, final_gpkg_path):
    # save gpkg_file temporarily
    temp_gpkg_path = os.path.join("temp", file_name)
    gdf.to_file(temp_gpkg_path, driver="GPKG")
    logging.info(f"{file_name}.gpkg saved temporarily in :{temp_gpkg_path}")
    shutil.copy(temp_gpkg_path, final_gpkg_path)
    logging.info(f"{file_name}.gpkg copied in :{final_gpkg_path}")
    if os.path.exists(temp_gpkg_path):
        os.remove(temp_gpkg_path)


def ods_id_col(df_wfs, df_fgi):
    # make a new column for ods_id in FGI Data set
    meta_data = pd.read_excel(os.path.join("data", "Metadata.xlsx"), na_filter=False)
    layer_data = []  # Sammeln von Daten als Liste
    for _, row in meta_data.iterrows():
        shapes_to_load = remove_empty_string_from_list(row["Layers"].split(";"))
        num_files = len(shapes_to_load)
        if num_files == 0:
            ind_list = df_fgi[df_fgi["Gruppe"] == row["Gruppe"]].index
            shapes_to_load = df_fgi.iloc[ind_list]["Name"].values[0]
        for layer in shapes_to_load:
            if layer in df_wfs["Name"].values:
                layer_data.append({"Name": layer, "ods_ids": row["ods_id"]})
    layer_mapping = pd.DataFrame(layer_data)  # Daten in DataFrame umwandeln
    grouped_mapping = layer_mapping.groupby("Name")["ods_ids"].apply(list).reset_index()

    # add the new column
    df_wfs = df_wfs.merge(grouped_mapping, on="Name", how="left")
    return df_wfs


def main():
    url_wms = "https://wms.geo.bs.ch/?SERVICE=wms&REQUEST=GetCapabilities"
    url_wfs = "https://wfs.geo.bs.ch/"
    wfs = WebFeatureService(url=url_wfs, version="2.0.0", timeout=120)
    df_wms = process_wms_data(url_wms)
    df_wfs = process_wfs_data(wfs)

    df_wfs["Gruppe"] = df_wfs.apply(lambda row: extract_second_hier_name(row, df_wms), axis=1)
    new_column_order = ["Gruppe", "Name"]
    df_wfs = df_wfs[new_column_order]
    df_wms_not_in_wfs = df_wms[~df_wms["Name"].isin(df_wfs["Name"])]
    # assign the layer names under main names to collect the geodata
    df_fgi = df_wfs.groupby("Gruppe")["Name"].apply(list).reset_index()
    # Add the ods_ids column
    df_wfs = ods_id_col(df_wfs, df_fgi)
    # save DataFrames in CSV files
    df_wms.to_csv(os.path.join("data", "Hier_wms.csv"), sep=";", index=False)
    df_fgi.to_csv(os.path.join("data", "mapBS_shapes.csv"), sep=";", index=False)
    df_wms_not_in_wfs.to_csv(os.path.join("data", "wms_not_in_wfs.csv"), sep=";", index=False)
    path_export = os.path.join("data", "100395_OGD_datensaetze.csv")
    df_wfs.to_csv(path_export, sep=";", index=False)
    common.update_ftp_and_odsp(path_export, "opendatabs", "100395")

    get_name_col(wfs, df_wfs)
    get_num_col(wfs, df_fgi)
    file_path = os.path.join("data", "export")
    save_geodata_for_layers(wfs, df_fgi, file_path)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f"Executing {__file__}...")
    main()
    logging.info("Job successful!")
