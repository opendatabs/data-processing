import logging
import numpy as np
import pandas as pd
import geopandas as gpd
from xml.dom.minidom import *
from iwb_elektroauto_ladestationen import credentials


def main():
    # Source: https://www.riannek.de/2022/kml-to-geopandas/
    dom = parse(credentials.kml_file)

    # Get the path of a placemark
    def subfolders(node):
        if node.parentNode == dom.documentElement:
            return ""
        else:
            foldername = node.getElementsByTagName("name")[0].firstChild.data
            path = subfolders(node.parentNode) + "/" + foldername
        return path

    # Parse the DOM of the KML
    entries = []
    placemarks = dom.getElementsByTagName("Placemark")
    for idx, i in enumerate(placemarks):
        logging.info(f'Parsing Placemark {idx}...')
        try:
            name = i.getElementsByTagName("name")[0].firstChild.data.replace('\n', '').strip()
        except IndexError:
            logging.info(f'Could not find name...')
            name = ""
        try:
            coordinates = i.getElementsByTagName("coordinates")[0].firstChild.data.replace('\n', '').strip()
            latitude = coordinates.split(',')[0]
            longitude = coordinates.split(',')[1]
            address = name
        except IndexError:
            logging.info(f'Could not find coordinates for {name}...')
            coordinates = ''
            latitude = np.NAN
            longitude = np.NAN
            address = i.getElementsByTagName("address")[0].firstChild.data.replace('\n', '').strip()
        try:
            description = i.getElementsByTagName("description")[0].firstChild.data.replace('\n', '').strip()
        except IndexError:
            logging.info(f'Could not find description for {name}...')
            description = ''
        parent = i.parentNode
        foldername = parent.getElementsByTagName("name")[0].firstChild.data
        path = subfolders(parent)
        # entries.append((name, latitude, longitude, foldername, path)) # List of tuples
        entries.append((name, description, address, latitude, longitude, coordinates, foldername, path))  # List of tuples

    # df = pd.DataFrame(entries, columns=('name', 'latitude', 'longitude', 'folder', 'path'))
    df = pd.DataFrame(entries, columns=('name', 'description', 'address', 'latitude', 'longitude', 'coordinates', 'folder', 'path'))
    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.longitude, df.latitude, crs="EPSG:4326"))
    pass


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()