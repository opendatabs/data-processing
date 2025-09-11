import io
import logging
import os
import xml.etree.ElementTree as ET
from urllib.parse import urlencode

import common
import pandas as pd
import pyproj
import requests
from dotenv import load_dotenv
from shapely.geometry import shape
from shapely.ops import transform, unary_union

load_dotenv()

MapBS_API = os.getenv("API_KEY_MAPBS")

# References:
# https://www.amtsblattportal.ch/docs/api/


def main():
    df = get_urls()
    all_data = []

    for index, row in df.iterrows():
        df_content = add_content_to_row(row)
        all_data.append(df_content)

    df = pd.concat(all_data, ignore_index=True)  # Concatenate all dataframes
    df = get_columns_of_interest(df)
    df = legal_form_code_to_name(df)
    df = get_parzellen(df)
    path_export = os.path.join("data", "export", "100366_kantonsblatt_bauplikationen.csv")
    df.to_csv(path_export, index=False)
    common.update_ftp_and_odsp(path_export, "staka/kantonsblatt", "100366")


def get_urls():
    # Get urls from metadata already published as a dataset
    url_kantonsblatt_ods = "https://data.bs.ch/explore/dataset/100352/download/"
    params = {"format": "csv", "refine.subrubric": "BP-BS10"}
    r = common.requests_get(url_kantonsblatt_ods, params=params)
    r.raise_for_status()
    # Save into a list
    return pd.read_csv(io.StringIO(r.content.decode("utf-8")), sep=";")[["id", "url_xml"]]


def add_content_to_row(row):
    content, _ = get_content_from_xml(row["url_xml"])
    df_content = xml_to_dataframe(content)
    row["content"] = ET.tostring(content, encoding="utf-8")
    row["url_kantonsblatt_ods"] = "https://data.bs.ch/explore/dataset/100352/table/?q=%23exact(id," + row["id"] + ")&"
    for col in row.index:
        if col in df_content.columns:
            # Combine existing DataFrame column with value from row, if it exists
            df_content[col] = df_content[col].combine_first(pd.Series([row[col]] * len(df_content)))
        else:
            # Create the column in df_content if it does not exist and fill with the value from the row
            df_content[col] = pd.Series([row[col]] * len(df_content))
    return df_content


def get_content_from_xml(url):
    try:
        r = common.requests_get(url)
        r.raise_for_status()
        xml_content = r.text
        root = ET.fromstring(xml_content)
        content = root.find("content")
        attachments = root.find("attachments")
    except requests.exceptions.HTTPError as err:
        logging.error(f"HTTP error occurred: {err}")
        return None, None
    return content, attachments


def xml_to_dataframe(root):
    def traverse(node, path="", path_dictionary=None):
        if path_dictionary is None:
            path_dictionary = {}

        if list(node):  # If the node has children
            for child in node:
                child_path = f"{path}_{child.tag}" if path else child.tag
                traverse(child, child_path, path_dictionary)
        else:  # If the node is a leaf
            value = node.text.strip() if node.text and node.text.strip() else ""
            if path in path_dictionary:
                path_dictionary[path].append(value)
            else:
                path_dictionary[path] = [value]

        return path_dictionary

    path_dict = traverse(root)

    # Find the maximum length of any list in the dictionary to standardize the DataFrame size
    max_len = max(len(v) for v in path_dict.values())  # Find the longest list

    # Expand all lists to this maximum length
    expanded_data = {k: v * max_len if len(v) == 1 else v + [""] * (max_len - len(v)) for k, v in path_dict.items()}

    df = pd.DataFrame(expanded_data)
    return df


def get_columns_of_interest(df):
    # Replace columns names so it's easier to understand
    df.columns = df.columns.str.replace("_legalEntity_multi_companies_", "_")
    df.columns = df.columns.str.replace("_multi_companies_", "_")
    columns_of_interest = [
        "publicationArea_selectType",
        "buildingContractor_legalEntity_selectType",
        "buildingContractor_noUID",
        "buildingContractor_company_name",
        "buildingContractor_company_uid",
        "buildingContractor_company_legalForm",
        "buildingContractor_company_customAddress",
        "buildingContractor_company_country_name_de",
        "delegation_selectType",
        "delegation_buildingContractor_legalEntity_selectType",
        "delegation_buildingContractor_noUID",
        "delegation_buildingContractor_company_name",
        "delegation_buildingContractor_company_uid",
        "delegation_buildingContractor_company_legalForm",
        "delegation_buildingContractor_company_customAddress",
        "delegation_buildingContractor_company_country_name_de",
        "projectFramer_selectType",
        "projectFramer_legalEntity_selectType",
        "projectFramer_noUID",
        "projectFramer_company_name",
        "projectFramer_company_uid",
        "projectFramer_company_legalForm",
        "projectFramer_company_customAddress",
        "projectFramer_company_country_name_de",
        "projectDescription",
        "projectLocation_address_street",
        "projectLocation_address_houseNumber",
        "projectLocation_address_swissZipCode",
        "projectLocation_address_town",
        "projectLocation_address_locationAdditional",
        "districtCadastre_relation_section",
        "districtCadastre_relation_plot",
        "locationCirculationOffice",
        "entryDeadline",
        "id",
        "url_kantonsblatt_ods",
    ]

    logging.info("The following columns are in both lists:")
    logging.info(set.intersection(set(df.columns), set(columns_of_interest)))

    logging.info("The following columns are in the df but not in columns_of_interest:")
    logging.info(set(df.columns) - set(columns_of_interest))

    logging.info("IMPORTANT: The following columns are in columns_of_interest but missing in the df:")
    logging.info(set(columns_of_interest) - set(df.columns))

    missing_columns = [col for col in columns_of_interest if col not in df.columns]
    for col in missing_columns:
        logging.info(f"Filling empty value for missing column: {col}")
        df[col] = ""

    return df[columns_of_interest]


def legal_form_code_to_name(df):
    url_i14y = "https://api.i14y.admin.ch/api/public/v1/concepts/08dad8ff-f18a-560b-bfa6-20767f2afb17/codelist-entries/exports/json"
    response = requests.get(url_i14y)
    response.raise_for_status()
    legal_forms = response.json()["data"]
    code_to_german_name = {entry["code"]: entry["name"]["de"] for entry in legal_forms}
    df["projectFramer_company_legalForm"] = df["projectFramer_company_legalForm"].map(code_to_german_name)
    df["buildingContractor_company_legalForm"] = df["buildingContractor_company_legalForm"].map(code_to_german_name)
    df["delegation_buildingContractor_company_legalForm"] = df["delegation_buildingContractor_company_legalForm"].map(
        code_to_german_name
    )
    return df


# Function to correct the parzellennummer i.e. 48 to 0048
def correct_parzellennummer(parzellennummer):
    parts = parzellennummer.split(",")
    parts = [part.strip() for part in parts]
    corrected = [num.zfill(4) for num in parts]
    return ",".join(corrected)


def get_geometry(row):
    # Transformer: CH1903+ (2056) → WGS84 (4326)
    project_to_wgs84 = pyproj.Transformer.from_crs("EPSG:2056", "EPSG:4326", always_xy=True).transform

    parzellennummer = row["districtCadastre_relation_plot"]
    section = row["districtCadastre_relation_section"]

    if pd.isna(parzellennummer) or pd.isna(section) or not str(parzellennummer).strip():
        logging.warning("⚠️  Empty parcel or section - skipped.")
        return None

    numbers = parzellennummer.split(", ")
    geometries = []

    for number in numbers:
        s_par = f"{section}-{number}"
        logging.info(f"→ Get Geometry for: {s_par}")
        url = "https://api.geo.bs.ch/grundstueckinfo/v1/realestatesinformation"
        params = {
            "ids": s_par,
            "withgeometry": "true",
            "apikey": MapBS_API,
        }
        try:
            response = common.requests_get(url, params=params)
            if response.status_code != 200 or not response.content:
                logging.warning(f"⏭️  No valid answer for {s_par} - is skipped.")
                continue
            data = response.json()
            realestates = data.get("RealEstates", [])
            if not realestates:
                logging.warning(f"⏭️  No entry for {s_par} - is skipped.")
                continue
            geom = realestates[0].get("Geometry")
            if geom:
                shapely_geom = shape(geom)
                geometries.append(transform(project_to_wgs84, shapely_geom))
        except Exception as e:
            logging.error(f"❌ Error in {s_par}: {e}")
            continue

    if len(geometries) == 1:
        return geometries[0]
    elif len(geometries) > 1:
        return unary_union(geometries)
    else:
        return None


def get_parzellen(df):
    df.loc[df["districtCadastre_relation_plot"].isna(), "districtCadastre_relation_plot"] = ""
    df["districtCadastre_relation_plot"] = df["districtCadastre_relation_plot"].astype(str)
    df["geo_shape"] = df.apply(lambda x: get_geometry(x), axis=1)

    df["districtCadastre_relation_plot"] = df["districtCadastre_relation_plot"].apply(correct_parzellennummer)

    df["url_parzellen"] = df.apply(
        lambda row: "https://data.bs.ch/explore/dataset/100201/table/?"
        + urlencode(
            {
                "refine.r1_sektion": row["districtCadastre_relation_section"],
                "q": "parzellennummer: " + " OR ".join(row["districtCadastre_relation_plot"].split(",")),
            }
        ),
        axis=1,
    )
    return df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
    logging.info("Job successful!")
