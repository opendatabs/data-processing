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
        logging.info(f"[WowLiving] Page {page}: {len(cards)} listings")

        for card in cards:
            try:
                parent = card.find_parent("div", class_="box")
                record = parse_wow_living_listing(parent)
                record["url"] = card["href"]
                records.append(record)
            except Exception as e:
                logging.info(f"  Skipping a card on page {page}: {e}")
    return records


# === Glandon setup ===
BASE_URL_GLANDON = "https://www.glandon-apartments.ch/basel/"

_WS = r"\u00A0\u202F\u2007\u2009"  # nbsp, thin space, figure space, hair space
DASH = r"\u2012\u2013\u2014\u2212"  # figure/en/em/minus
NUM = r"\d+[’']?\d*(?:[.,]\d+)?"


def _clean(s: str) -> str:
    if not s:
        return ""
    # normalize apostrophes and spaces
    s = s.replace("\u2019", "'").replace("’", "'").replace("\xa0", " ").replace("\u202f", " ")
    return " ".join(s.split())


def _to_int(txt):
    return int(txt.replace("'", "").replace(",", "").replace(".", ""))


def _to_float(txt):
    return float(txt.replace("'", "").replace("’", "").replace(" ", "").replace(",", "."))


def parse_glandon_listing(card):
    # URL
    link = card.select_one("a.link")
    url = link["href"] if link and link.has_attr("href") else None

    # Name
    name_tag = card.select_one("h2.h3")
    name = _clean(name_tag.get_text(strip=True)) if name_tag else None

    # Address
    addr_tag = card.select_one(".location-address, .location.location-detail.location-address")
    address = _clean(addr_tag.get_text(strip=True)) if addr_tag else None

    # Price range e.g. "CHF 1'550 – 2'350"
    price_tag = card.select_one(".location-price, .location.location-detail.location-price")
    price_text = _clean(price_tag.get_text(strip=True)) if price_tag else ""
    prices = re.findall(NUM, price_text)
    price_min = _to_int(prices[0]) if prices else None
    price_max = _to_int(prices[1]) if len(prices) > 1 else price_min

    # Rooms e.g. "1,5 – 2,5 Zimmer" or "1 Zimmer"
    rooms_tag = card.select_one(".location-rooms, .location.location-detail.location-rooms")
    rooms_text = _clean(rooms_tag.get_text(strip=True)) if rooms_tag else ""
    room_nums = re.findall(NUM, rooms_text)
    room_min = _to_float(room_nums[0]) if room_nums else None
    room_max = _to_float(room_nums[1]) if len(room_nums) > 1 else room_min

    return {
        "name": name,
        "address": address,
        "price_min": price_min,
        "price_max": price_max,
        "rooms": room_min,  # keep your existing schema
        "rooms_min": room_min,  # optional: new fields if you want ranges
        "rooms_max": room_max,
        "sqm": None,
        "url": url,
        "source": "glandon",
    }


def scrape_glandon():
    soup = fetch_soup(BASE_URL_GLANDON)
    # Narrow to the correct component in case the page has multiple ULs
    scope = soup.select_one('flynt-component[name="GridPostsLocationsDetail"]') or soup
    cards = scope.select("ul.grid > li.post")
    if not cards:
        # help debug what you actually fetched
        snippet = soup.get_text(" ", strip=True)[:400]
        print("[Glandon] No cards found. First 400 chars of page:\n", snippet)
    else:
        print(f"[Glandon] Found {len(cards)} listings")
    records = []
    for card in cards:
        try:
            records.append(parse_glandon_listing(card))
        except Exception as e:
            print(f"  Skipping Glandon card: {e}")
    return records


# === Domicile setup ===
def scrape_domicile():
    base_url = "https://www.immoscout24.ch/anbieter/s040/domicile-co-ag/alle-kaufinserate/kaufen/trefferliste-ag"
    records = []
    ep = 1

    while True:
        logging.info(f"[Domicile] Fetching page {ep}")
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
                logging.info(f"  Skipping Domicile card on page {ep}: {e}")

        ep += 1

    return records


# === Apartmenthaus setup ===
BASE_URL_APARTMENTHAUS = "https://www.apartmenthaus.ch/"


# re-usable cleaners (you can reuse ones you added for Glandon)
def _ah_clean(s: str) -> str:
    if not s:
        return ""
    return " ".join(s.replace("\u00a0", " ").replace("\u202f", " ").replace("’", "'").split())


def _ah_to_int(txt: str) -> int:
    # 3'000.- /mtl.  -> 3000
    return int(re.sub(r"[^0-9]", "", txt))


def _ah_to_float(txt: str) -> float:
    # 72.5 / 72,5 -> float
    return float(txt.replace("'", "").replace(" ", "").replace(",", "."))


def parse_apartmenthaus_list_locations():
    """Return list of dicts with name (acts as address label) and detail url."""
    soup = fetch_soup(BASE_URL_APARTMENTHAUS)
    grid = soup.select_one(".th-portfolio-row")
    if not grid:
        print("[Apartmenthaus] No grid found on homepage.")
        return []

    items = grid.select(".th-portfolio-item .th-port-card a.th-port-card-link")
    out = []
    for a in items:
        try:
            name_tag = a.select_one(".th-port-title")
            name = _ah_clean(name_tag.get_text(strip=True)) if name_tag else None
            url = a["href"]
            out.append({"name": name, "url": url})
        except Exception as e:
            print(f"  Skipping grid item: {e}")
    print(f"[Apartmenthaus] Found {len(out)} locations")
    return out


ROOM_RE = re.compile(r"(\d+(?:[.,]\d+)?)\s*(?:Zimmer)(?:\s*Wohnung)?", re.I)
SQM_RE = re.compile(r"ca\.?\s*([0-9]+(?:[.,][0-9]+)?)\s*(?:m2|m²)", re.I)
PRICE_RE = re.compile(r"CHF\s*([0-9’'.,\s]+)")


def parse_apartmenthaus_detail(detail_url: str, base_name: str):
    """Return list of offers (one per h4 block) for a location detail page."""
    soup = fetch_soup(detail_url)

    # All the room headings live in h4.elementor-heading-title
    h4s = soup.select("h4.elementor-heading-title")
    if not h4s:
        print(f"[Apartmenthaus] No room headers on {detail_url}")
        return []

    offers = []
    for h in h4s:
        text = _ah_clean(h.get_text(" ", strip=True))
        if not text:
            continue

        # Extract numbers
        rooms = None
        sqm = None
        price_min = None

        m_room = ROOM_RE.search(text)
        if m_room:
            rooms = _ah_to_float(m_room.group(1))

        m_sqm = SQM_RE.search(text)
        if m_sqm:
            sqm = _ah_to_float(m_sqm.group(1))

        m_price = PRICE_RE.search(text)
        if m_price:
            price_min = _ah_to_int(m_price.group(1))

        # Keep also a human label (e.g., "Hofseite", "Stadtseite", "Dachgeschoss"…)
        label = text

        # Require at least rooms or price; skip decorative headings
        if rooms is None and price_min is None:
            continue

        offers.append(
            {
                "name": base_name,  # e.g., "Schweizergasse, Basel"
                "address": base_name,  # homepage doesn't show street/number -> use name as address label
                "price_min": price_min,
                "price_max": None,
                "rooms": rooms,
                "rooms_min": rooms,  # consistent with your range pattern
                "rooms_max": rooms,
                "sqm": sqm,
                "url": detail_url,
                "variant": label,  # full heading text for traceability
                "source": "apartmenthaus",
            }
        )
    return offers


def scrape_apartmenthaus():
    locations = parse_apartmenthaus_list_locations()
    records = []
    for loc in locations:
        try:
            records.extend(parse_apartmenthaus_detail(loc["url"], loc["name"]))
        except Exception as e:
            print(f"  Skipping location {loc.get('name')}: {e}")
    return records


# === Main ===
def main():
    wow = scrape_wow_living()
    glandon = scrape_glandon()
    domicile = scrape_domicile()
    apartmenthaus = scrape_apartmenthaus()

    df = pd.DataFrame(wow + glandon + domicile + apartmenthaus)
    df = df[df["address"].str.contains("Basel|Riehen|Bettingen", na=False)]
    df.to_csv("data/apartments_combined.csv", index=False)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
