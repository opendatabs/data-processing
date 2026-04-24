from pathlib import Path
from typing import Any

import pandas as pd
import requests
from bs4 import BeautifulSoup

STAC_BASE_URL = "https://api.geo.bs.ch/stac/v1"
OUTPUT_DIR = Path("data")


def fetch_json(url: str, timeout: int = 60) -> dict[str, Any]:
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()
    return response.json()


def safe_get(data: dict[str, Any], *keys: str, default=None):
    current = data
    for key in keys:
        if not isinstance(current, dict):
            return default
        current = current.get(key)
        if current is None:
            return default
    return current


def extract_links(links: list[dict[str, Any]] | None) -> dict[str, str]:
    result: dict[str, str] = {}
    if not links:
        return result

    for link in links:
        rel = link.get("rel")
        href = link.get("href")
        if rel and href:
            result[rel] = href

    return result


def extract_orgs(providers):
    producer = []
    publisher = []

    for p in providers:
        roles = p.get("roles", [])
        name = p.get("name")
        if not name:
            continue

        if "producer" in roles:
            producer.append(name)
        if "host" in roles or "licensor" in roles:
            publisher.append(name)

    return "; ".join(producer), "; ".join(publisher)


def extract_datasets_from_metadata(metadata_url):
    if not metadata_url:
        return None

    try:
        response = requests.get(metadata_url)
        response.raise_for_status()
    except requests.exceptions.RequestException:
        return None

    soup = BeautifulSoup(response.text, "html.parser")

    datasets_heading = soup.find("h2", id="datasets")
    if not datasets_heading:
        return None

    dataset_names = []
    current = datasets_heading.find_next_sibling()

    while current:
        if current.name == "h2":
            break

        if current.name == "h3":
            name = current.get_text(strip=True)
            if name:
                dataset_names.append(name)

        current = current.find_next_sibling()

    if not dataset_names:
        return None

    return "; ".join(dataset_names)


def extract_temporal_interval(intervals):
    if not intervals or not isinstance(intervals, list):
        return None

    first_interval = intervals[0]
    if not isinstance(first_interval, list) or len(first_interval) == 0:
        return None

    start = first_interval[0]
    if not start:
        return None

    try:
        dt = pd.to_datetime(start)
        return dt.strftime("%Y-%m-%d")  # oder "%d.%m.%Y"
    except Exception:
        return start


def collection_to_row(collection: dict[str, Any]) -> dict[str, Any]:
    links = extract_links(collection.get("links", []))
    extent = collection.get("extent", {})
    temporal_interval = safe_get(extent, "temporal", "interval", default=[])
    keywords = collection.get("keywords", [])
    producer, publisher = extract_orgs(collection.get("providers", []))
    metadata_link = links.get("describedby")
    datasets = extract_datasets_from_metadata(metadata_link)

    return {
        "id": collection.get("id"),
        "title": collection.get("title"),
        "description": collection.get("description"),
        "license": collection.get("license"),
        "stac_version": collection.get("stac_version"),
        "keywords": ", ".join(keywords) if isinstance(keywords, list) else None,
        "temporal_interval_json": extract_temporal_interval(temporal_interval),
        "self_link": links.get("self"),
        "items_link": links.get("items"),
        "MapBS_link": links.get("related"),
        "Metadata": metadata_link,
        "producer_organization": producer,
        "publishing_organization": publisher,
        "datasets": datasets,
    }


def fetch_all_collections(stac_base_url: str) -> list[dict[str, Any]]:
    url = f"{stac_base_url.rstrip('/')}/collections"
    payload = fetch_json(url)

    collections = payload.get("collections", [])
    if not isinstance(collections, list):
        raise ValueError("Antwort enthält kein gültiges 'collections'-Array.")

    return collections


def build_collections_dataframe(stac_base_url: str) -> pd.DataFrame:
    collections = fetch_all_collections(stac_base_url)
    rows = [collection_to_row(collection) for collection in collections]
    df = pd.DataFrame(rows)

    if not df.empty:
        df = df.sort_values(by=["id"], na_position="last").reset_index(drop=True)

    return df


def main() -> None:
    df = build_collections_dataframe(STAC_BASE_URL)
    excel_path = OUTPUT_DIR / "bs_stac_collections.xlsx"
    print(f"{len(df)} Collections gefunden.")
    df.to_excel(excel_path, index=False)
    print(f"Excel gespeichert: {excel_path.resolve()}")


if __name__ == "__main__":
    main()
