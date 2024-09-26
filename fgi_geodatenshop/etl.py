import os
import xml.etree.ElementTree as ET
import pandas as pd
from owslib.wfs import WebFeatureService
from fgi_geodatenshop import credentials
import geopandas as gpd 
import logging
import io
import common
import requests


# Create new ‘Title’ column in df_wfs (Kanton Basel-Stadt WMS/**Hundesignalisation**/Hundeverbot)
def extract_second_hier_name(row, df2):
    # Search in df2 for the matching name
    matching_row = df2[df2['Name'] == row['Name']]
    if not matching_row.empty:
        hier_name = matching_row['Hier_Name'].values[0]
        hier_parts = hier_name.split('/')
        return hier_parts[1] if len(hier_parts) > 1 else None


# Function for retrieving and parsing WMS GetCapabilities
def get_wms_capabilities(url_wms):
    response = requests.get(url=url_wms, verify=False)
    xml_data = response.content
    root = ET.fromstring(xml_data)
    namespaces = {'wms': 'http://www.opengis.net/wms'}
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
                extract_layers(sublayer, namespaces, data, current_name_hierarchy, current_title_hierarchy)
        else:
            # If there are no sub-layers, add the deepest layer to the data
            data.append([layer_name, layer_title, current_name_hierarchy, current_title_hierarchy])


# Main function to process WMS data and create a DataFrame
def process_wms_data(url_wms):
    root, namespaces = get_wms_capabilities(url_wms)
    capability_layer = root.find(".//wms:Capability/wms:Layer", namespaces)

    # Initialize the data list outside the recursive function
    data = []
    if capability_layer is not None:
        extract_layers(capability_layer, namespaces, data)
        df_wms = pd.DataFrame(data, columns=['Name', 'Layer', 'Hier_Name', 'Hier_Titel'])
    return df_wms


# Function for retrieving and parsing WFS GetCapabilities
def process_wfs_data(url_wfs):
    wfs = WebFeatureService(url=url_wfs, version='2.0.0')
    # Retrieve the list of available layers (feature types) and their metadata
    # feature_types = [(ft, wfs[ft].title) for ft in wfs.contents]
    feature_list = []
    for feature in wfs.contents:
        feature_info = wfs[feature]
        feature_list.append({
         'Name': feature,
         'Metadata URL': feature_info.metadataUrls})
    # Convert to DataFrame and display
    df_wfs = pd.DataFrame(feature_list)
    # Clearing the column 'Layer Name' ( remove 'ms:')
    df_wfs['Name'] = df_wfs['Name'].str.replace('ms:', '', regex=False)
    # Clean-up of the ‘Metadata URL’ column (remove prefix and suffix)
    df_wfs['Metadata URL'] = df_wfs['Metadata URL'].astype(str)
    df_wfs['Metadata URL'] = df_wfs['Metadata URL'].str.replace(r"\[{'url': '", '', regex=True)
    df_wfs['Metadata URL'] = df_wfs['Metadata URL'].str.replace(r"'}\]", '', regex=True)
    return df_wfs


# Function for saving FGI geodata for each layer name
def save_geodata_for_layers(wfs, df_grouped, file_path):
    # Create a folder for the title to save geopackage data
    for titel, layer_name_list in df_grouped[['Titel', 'Name']].values:
        titel_dir = os.path.join(file_path, titel)
        os.makedirs(titel_dir, exist_ok=True)
        # Retrieve and save the geodata for each layer name in the list
        for layer_name in layer_name_list:
            try:
                response = wfs.getfeature(typename=layer_name)
                gdf = gpd.read_file(io.BytesIO(response.read()))
                gdf = gdf.to_crs(epsg=4326) 
                geopackage_file = os.path.join(titel_dir, f'{layer_name}.gpkg')
                gdf.to_file(geopackage_file, driver='GPKG') 
                ftp_remote_dir = 'harvesters/GVA/data/geopackage'
                common.upload_ftp(geopackage_file, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass,
                                      ftp_remote_dir)
                logging.info(f'Successfully saved {layer_name} for {titel}')
            except Exception as e:
                logging.error(f'Error processing {layer_name} for {titel}: {e}')


def main():
    url_wms = 'https://wms.geo.bs.ch/?SERVICE=wms&REQUEST=GetCapabilities'
    url_wfs = 'https://wfs.geo.bs.ch/'
    
    df_wms = process_wms_data(url_wms)
    df_wfs = process_wfs_data(url_wfs)
    
    df_wfs['Titel'] = df_wfs.apply(lambda row: extract_second_hier_name(row, df_wms), axis=1)
    new_column_order = ['Titel', 'Name', 'Metadata URL']
    df_wfs = df_wfs[new_column_order]
    df_wms_not_in_wfs = df_wms[~df_wms['Name'].isin(df_wfs['Name'])]
    # assign the layer names under main names to collect the geodata
    df_fgi = df_wfs.groupby('Titel')['Name'].apply(list).reset_index()

    # save DataFrames in CSV files
    data_path = credentials.data_path
    df_wms.to_csv(os.path.join(data_path, 'Hier_wms.csv'), sep=';', index=False)
    df_fgi.to_csv(os.path.join(data_path, 'FGI_List.csv'), sep=';', index=False)
    df_wms_not_in_wfs.to_csv(os.path.join(data_path, 'wms_not_in_wfs.csv'), sep=';', index=False)
    path_export = os.path.join(data_path, '100395.csv')
    df_wfs.to_csv(path_export, sep=';', index=False)
    common.update_ftp_and_odsp(path_export, 'FST-OGD', '100395')


    wfs = WebFeatureService(url=url_wfs, version='2.0.0')
    file_path = os.path.join(credentials.data_path,'export')
    save_geodata_for_layers(wfs, df_fgi, file_path)

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info('Job successful!')
