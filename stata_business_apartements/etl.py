import logging
import re

import pandas as pd
import requests
from bs4 import BeautifulSoup

# === WOW Living setup ===
BASE_URL_WOW_LIVING = "https://www.wowliving.ch/de/serviced-apartments/basel"
PAGES_WOW_LIVING = [1, 2]


def fetch_soup(url, params=None):
    resp = requests.get(url, params=params)
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "html.parser")


def parse_wow_living_listing(card):
    meta = card.select_one(".apartment--meta")
    if not meta:
        raise ValueError("Missing .apartment--meta")

    paragraphs = meta.find_all("p")
    if len(paragraphs) < 2:
        raise ValueError("Expected 2 paragraphs in meta block")

    details = " ".join(paragraphs[0].stripped_strings)
    address = paragraphs[1].get_text(strip=True)

    match = re.search(r"CHF\s*([\d']+)\s*-\s*([\d']+)\s*\|\s*([\d.]+)\s*Zimmer\s*\|\s*([\d.]+)\s*m", details)
    if not match:
        raise ValueError(f"Could not parse details: {details}")

    return {
        "address": address,
        "price_min": int(match.group(1).replace("'", "")),
        "price_max": int(match.group(2).replace("'", "")),
        "rooms": float(match.group(3)),
        "sqm": float(match.group(4)),
        "source": "wow_living",
    }


def scrape_wow_living():
    records = []
    for page in PAGES_WOW_LIVING:
        soup = fetch_soup(BASE_URL_WOW_LIVING, params={"page": page})
        cards = soup.select("a.apartment")
        print(f"[WowLiving] Page {page}: {len(cards)} listings")

        for card in cards:
            try:
                parent = card.find_parent("div", class_="box")
                record = parse_wow_living_listing(parent)
                record["url"] = card["href"]
                records.append(record)
            except Exception as e:
                print(f"  Skipping a card on page {page}: {e}")
    return records


# === Glandon setup ===
BASE_URL_GLANDON = "https://www.glandon-apartments.ch/basel"


def parse_glandon_listing(card):
    name = card.select_one("strong").get_text(strip=True)

    tds = card.select("table td")
    if len(tds) < 2:
        raise ValueError("Missing table cells")

    street = tds[0].get_text(strip=True)
    price_text = tds[1].get_text(strip=True)

    zip_city = tds[2].get_text(strip=True)
    address = f"{street}, {zip_city}"

    prices = re.findall(r"\d+'?\d*", price_text)
    price_min = int(prices[0].replace("'", "")) if prices else None
    price_max = int(prices[1].replace("'", "")) if len(prices) > 1 else price_min

    link_tag = card.select_one(".bottomLine a")
    url = "https://www.glandon-apartments.ch" + link_tag["href"] if link_tag else None

    return {
        "address": address,
        "price_min": price_min,
        "price_max": price_max,
        "rooms": None,
        "sqm": None,
        "url": url,
        "source": "glandon",
        "name": name,
    }


def scrape_glandon():
    soup = fetch_soup(BASE_URL_GLANDON)
    cards = soup.select(".house-item")
    print(f"[Glandon] Found {len(cards)} listings")
    records = []
    for card in cards:
        try:
            record = parse_glandon_listing(card)
            records.append(record)
        except Exception as e:
            print(f"  Skipping Glandon card: {e}")
    # Replace \t\t\t\t\t\t\ with ", " in address
    for record in records:
        record["address"] = re.sub(r"\s{2,}", ", ", record["address"]).strip()
    return records


# === Domicile setup ===
def scrape_domicile():
    base_url = "https://www.immoscout24.ch/anbieter/s040/domicile-co-ag/alle-kaufinserate/kaufen/trefferliste-ag"
    records = []
    ep = 1

    while True:
        print(f"[Domicile] Fetching page {ep}")
        soup = fetch_soup(base_url, params={"ep": ep, "nrs": 8, "aa": "purchorrentall", "o": "dateCreated-desc"})

        cards = soup.select(".HgListingPreviewGallery_card_NrDZZ")
        if not cards:
            break

        for card in cards:
            try:
                price_raw = card.select_one(".HgListingPreviewGalleryInfoBox_price_RmxSL")
                price = re.search(r"\d+[’']?\d*", price_raw.get_text()) if price_raw else None
                price_min = int(price.group().replace("’", "").replace("'", "")) if price else None

                meta = card.select_one(".HgListingRoomsLivingSpace_roomsLivingSpace_GyVgq")
                rooms = sqm = None
                if meta:
                    text = meta.get_text(" ", strip=True)
                    rooms_match = re.search(r"(\d+(\.\d+)?)\s*Zimmer", text)
                    sqm_match = re.search(r"(\d+)\s*m²", text)
                    rooms = float(rooms_match.group(1)) if rooms_match else None
                    sqm = float(sqm_match.group(1)) if sqm_match else None

                address_tag = card.select_one(".HgListingPreviewGalleryInfoBox_address_nVKUZ")
                address = address_tag.get_text(strip=True) if address_tag else None

                records.append(
                    {
                        "address": address,
                        "price_min": price_min,
                        "price_max": None,
                        "rooms": rooms,
                        "sqm": sqm,
                        "url": None,  # Could be added later if detail URLs are available
                        "source": "domicile",
                        "page": ep,
                    }
                )

            except Exception as e:
                print(f"  Skipping Domicile card on page {ep}: {e}")

        ep += 1

    return records


# === Main ===
def main():
    wow = scrape_wow_living()
    glandon = scrape_glandon()
    domicile = scrape_domicile()

    df = pd.DataFrame(wow + glandon + domicile)
    df = df[df["address"].str.endswith(("Basel", "Riehen", "Bettingen"), na=False)]
    df.to_csv("data/apartments_combined.csv", index=False)
    print(df[["address", "price_min", "price_max", "rooms", "sqm", "source"]])


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
