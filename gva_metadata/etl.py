import pandas as pd
import common
import os
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
from gva_metadata import credentials
import time
import logging
import re

# ChromeDriver Setup
driver_path = os.path.join(credentials.data_path, 'chromedriver.exe')
service = Service(driver_path)  # Replace with your ChromeDriver path
options = Options()
options.add_argument("--headless")  # Runs without a visible browser
driver = webdriver.Chrome(service=service, options=options)

# URL of the Website
url = "https://shop.geo.bs.ch/geodaten-katalog/"
driver.get(url)

try:
    # Waiting time for the page to load
    time.sleep(5)

    # Saving the page as HTML
    page_source = driver.page_source
    with open("Geodaten_Katalog.html", "w", encoding="utf-8") as file:
        file.write(page_source)
    logging.info("Page was saved as Geodaten_Katalog.html")

    # Extracting the topics, subtopics and additional information with BeautifulSoup
    soup = BeautifulSoup(page_source, "html.parser")
    data = []

    # Search for main topics
    headers = soup.find_all("div", class_="headerText")
    for header in headers:
        main_theme = header.get_text(strip=True)
        logging.info(f"main topic: {main_theme}")

        # Search for subtopics
        parent_div = header.find_parent("div", class_="header").find_next_sibling("div", class_="SubGliedContext")
        if parent_div:
            sub_themes = parent_div.find_all("div", id="thema")
            for sub_theme in sub_themes:
                # Extract subtopic
                sub_theme_title = sub_theme.find("div", class_="themaTitel").get_text(strip=True)
                last_abbreviation = None
                if "(" in sub_theme_title and ")" in sub_theme_title:
                    match = re.search(r"\(([^()]*)\)$", sub_theme_title)
                    if match:
                        last_abbreviation = match.group(1)
                        sub_theme_title = re.sub(r"\s*\([^()]*\)$", "", sub_theme_title)

                # Extract Description
                description_div = sub_theme.find("div", class_="themaBesch")
                description = description_div.get_text(strip=True) if description_div else None

                # Extract Update Date
                date_div = sub_theme.find("div", class_="aktualisierung")
                update_date = date_div.get_text(strip=True).replace("Stand der Geodaten: ", "") if date_div else None

                # Extract Links
                links_container = sub_theme.find("div", class_="themaLinksContainer")
                links_dict = {}
                if links_container:
                    link_elements = links_container.find_all("a")
                    for link in link_elements:
                        link_text = link.get_text(strip=True)
                        links_dict[link_text] = link["href"]
                # Extract layers
                ebene_container = sub_theme.find_all("div", class_="ebenen")
                ebene_names = [ebene.find("b").get_text(strip=True) for ebene in ebene_container if
                               ebene.find("b")]  # only <b>-Texts extract

                # Extract the link of the image
                image_div = sub_theme.find("div", class_="themaBild")
                image_tag = image_div.find("img") if image_div else None
                image_url = f"https://shop.geo.bs.ch/geodaten-katalog/{image_tag['src']}" if image_tag else None

                entry = {
                    "Kategorie": main_theme,
                    "Thema": sub_theme_title,
                    "Abkuerzung": last_abbreviation,
                    "Beschreibung": description,
                    "Aktualisierung": update_date,
                    "Ebenen": "; ".join(ebene_names),  # Save layers as a semicolon separated list
                    "Bild-URL": image_url
                }

                # Add the links as separate columns
                for link_text, link_url in links_dict.items():
                    entry[link_text] = link_url

                data.append(entry)
        else:
            data.append({"Kategorie": main_theme, "Thema": None,  "Abkuerzung": None, "Beschreibung": None, "Aktualisierung": None, "Ebenen": None})

    # Save to an Excel file
    df = pd.DataFrame(data)

    # Convert entries in the columns to  "öffentlich" and "beschränkt öffentlich"
    if "öffentlich" in df.columns:
        df["öffentlich"] = df["öffentlich"].apply(lambda x: "Kategorie A" if pd.notna(x) else None)
    if "beschränkt öffentlich" in df.columns:
        df["beschränkt öffentlich"] = df["beschränkt öffentlich"].apply(
            lambda x: "Kategorie B" if pd.notna(x) else None)

    # Combine the columns "öffentlich" and "beschränkt öffentlich" in a new columns "Zugriff"
    if "öffentlich" in df.columns or "beschränkt öffentlich" in df.columns:
        df["Zugriff"] = df["öffentlich"].fillna(df["beschränkt öffentlich"])
        df.drop(columns=["öffentlich", "beschränkt öffentlich"], inplace=True)
        df['Zugriff'] = df['Zugriff'] + f": \"https://www.geo.bs.ch/erweiterte-berechtigung\""

        # Sort the columns in the desired order
        desired_columns = ["Kategorie", "Thema", "Abkuerzung", "Beschreibung", "Aktualisierung", "Geodaten-Shop",
                           "Metadaten", "MapBS", "Geobasisdaten", "Ebenen", "WMS", "WFS", "WMTS", "Bild-URL",
                           "Zugriff"]
        existing_columns = [col for col in desired_columns if col in df.columns]
        df = df[existing_columns]
        file_name = "100410_geodatenkatalog.csv"
        file_path = os.path.join(credentials.data_path, file_name)
        df.to_csv(file_path, index=False, sep=';')
        common.update_ftp_and_odsp(file_path, '/gva/geodatenkatalog', '100410.csv')
        print(f"CSV-Datei wurde erfolgreich gespeichert: {file_name}")

except Exception as e:
    print(f"Fehler: {e}")

finally:
    # Close browser
    driver.quit()
