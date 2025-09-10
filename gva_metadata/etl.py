import logging
import os
import re
import time

import common
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
# from selenium.webdriver.chrome.options import Options
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.chrome.service import Service
# from webdriver_manager.chrome import ChromeDriverManager

# ChromeDriver Setup locally
# driver_path = os.path.join('data', 'chromedriver.exe')
# service = Service(driver_path)  # Replace with your ChromeDriver path
# options = Options()
# options.add_argument("--headless")  # Runs without a visible browser
# driver = webdriver.Chrome(service=service, options=options)

options = Options()
options.add_argument("--headless") 
options.add_argument("--no-sandbox")
# service = Service(ChromeDriverManager().install())
# driver = webdriver.Chrome(service=service, options=options)
driver = webdriver.Firefox(options=options)
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
    ebene_data = []  # dataframe for descreption of "Ebenen"
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
                # Extract layers and descriptions
                ebene_container = sub_theme.find_all("div", class_="ebenen")
                ebene_details = []
                for ebene in ebene_container:
                    title = ebene.find("b").get_text(strip=True) if ebene.find("b") else None
                    beschreibung = (
                        ebene.find("b").next_sibling.strip()
                        if ebene.find("b") and ebene.find("b").next_sibling
                        else None
                    )
                    if title:
                        ebene_details.append(title)
                        ebene_data.append({"Ebene": title, "Beschreibung": beschreibung})

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
                    "Ebenen": " ; ".join(ebene_details),
                    "Bild-URL": image_url,
                }

                # Add the links as separate columns
                for link_text, link_url in links_dict.items():
                    entry[link_text] = link_url

                data.append(entry)
        else:
            data.append(
                {
                    "Kategorie": main_theme,
                    "Thema": None,
                    "Abkuerzung": None,
                    "Beschreibung": None,
                    "Aktualisierung": None,
                    "Ebenen": None,
                }
            )

    # Save to an Excel file
    df = pd.DataFrame(data)

    # Save Ebenen-Beschreibungen separately
    df_ebenen = pd.DataFrame(ebene_data).drop_duplicates()
    # Convert entries in the columns to  "öffentlich" and "beschränkt öffentlich"
    if "öffentlich" in df.columns:
        df["öffentlich"] = df["öffentlich"].apply(lambda x: "Kategorie A" if pd.notna(x) else None)
    if "beschränkt öffentlich" in df.columns:
        df["beschränkt öffentlich"] = df["beschränkt öffentlich"].apply(
            lambda x: "Kategorie B" if pd.notna(x) else None
        )

    # Combine the columns "öffentlich" and "beschränkt öffentlich" in a new columns "Zugriff"
    if "öffentlich" in df.columns or "beschränkt öffentlich" in df.columns:
        df["Zugriff"] = df["öffentlich"].fillna(df["beschränkt öffentlich"])
        df.drop(columns=["öffentlich", "beschränkt öffentlich"], inplace=True)
        df["Zugriff"] = df["Zugriff"] + ': "https://www.geo.bs.ch/erweiterte-berechtigung"'
        df["Page"] = "https://opendatabs.github.io/geoportal-poc/?param=" + df["Abkuerzung"]
        # Sort the columns in the desired order
        desired_columns = [
            "Kategorie",
            "Thema",
            "Abkuerzung",
            "Page",
            "Beschreibung",
            "Aktualisierung",
            "Geodaten-Shop",
            "Metadaten",
            "MapBS",
            "Geobasisdaten",
            "Ebenen",
            "WMS",
            "WFS",
            "WMTS",
            "Bild-URL",
            "Zugriff",
        ]
        existing_columns = [col for col in desired_columns if col in df.columns]
        df = df[existing_columns]
        file_name = "100410_geodatenkatalog.csv"
        file_path = os.path.join("data", file_name)
        df.to_csv(file_path, index=False, sep=";")
        common.update_ftp_and_odsp(file_path, "/gva/geodatenkatalog", "100410")
        logging.info(f"CSV-Datei wurde erfolgreich gespeichert: {file_name}")

except Exception as e:
    logging.error(f"Fehler: {e}")

finally:
    # Close browser
    driver.quit()

try:
    # Group by "topic" and keep the first entry of each group
    grouped_df = (
        df.groupby("Thema")
        .agg(
            {
                "Kategorie": lambda x: ";".join(set(x)),  # Merge categories separated by ';'
                **{col: "first" for col in df.columns if col not in ["Thema", "Kategorie"]},
            }
        )
        .reset_index()
    )

    # Create new columns
    grouped_df["title"] = grouped_df["Thema"]
    grouped_df["modified"] = grouped_df["Aktualisierung"]
    grouped_df["attributions"] = "Geodaten Kanton Basel-Stadt"
    grouped_df["language"] = "de"
    grouped_df["tags"] = grouped_df["Kategorie"]

    # Format description as HTML
    def create_description(row):
        description = """<div style='display: flex; align-items: flex-start; gap: 20px;'>
        <div style='flex: 1;'>
            <h3>Kategorie</h3>
            <ul>"""

        # linking category entries
        if pd.notna(row["Kategorie"]):
            for category in row["Kategorie"].split(";"):
                category = category.strip()
                url = f"https://data.bs.ch/explore/?q=tags%3D{category.replace(' ', '%20')}"
                description += f"<li><a href='{url}' target='_blank'>{category}</a></li>"
        description += "</ul>"

        description += """

            <h3>Beschreibung</h3>
            <p>{Beschreibung}</p>

            <h3>Ebenen</h3>
            <ul>""".format(Beschreibung=row["Beschreibung"])

        # Add layer descriptions dynamically from df_ebenen
        if pd.notna(row["Ebenen"]):
            for ebene in row["Ebenen"].split(";"):
                ebene = ebene.strip()
                beschreibung = df_ebenen.loc[df_ebenen["Ebene"] == ebene, "Beschreibung"].values
                if len(beschreibung) > 0 and beschreibung[0].strip():
                    beschreibung_text = f"<strong>{ebene}:</strong> {beschreibung[0]}"
                else:
                    beschreibung_text = ebene
                description += f"<li>{beschreibung_text}</li>"
        description += "</ul>"

        # Add links
        description += "<h3>Links</h3><ul>"
        for link_column in ["Geodaten-Shop", "Metadaten", "MapBS", "Geobasisdaten", "WMS", "WFS", "WMTS"]:
            if pd.notna(row.get(link_column)):
                description += f"<li><a href='{row[link_column]}' target='_blank'>{link_column}</a></li>"
        description += "</ul></div>"

        # Add Bild-URL
        if pd.notna(row["Bild-URL"]):
            description += f"<div><img src='{row['Bild-URL']}' alt='Bildbeschreibung' style='max-width:300px; height:auto; border-radius:8px;'></div>"

        description += "</div>"

        return description

    grouped_df["description"] = grouped_df.apply(create_description, axis=1)

    final_df = grouped_df[["title", "description", "attributions", "modified", "tags", "language"]]
    metadata_file = "gva_metadata.csv"
    metadata_file_path = os.path.join("data", metadata_file)
    final_df.to_csv(metadata_file_path, index=False, sep=";")
    logging.info(f"Neue Tabelle wurde erfolgreich gespeichert: {metadata_file}")

except Exception as e:
    logging.error(f"Fehler bei der Verarbeitung: {e}")
