import logging
import pandas as pd
import requests

def main():
    basel_grid = pd.read_csv("data_orig/basel_grid.csv")
    df = pd.concat([get_bs_accidents_by_grid(row) for row in basel_grid.iterrows()])
    df.to_excel("data/Unfaelle.xlsx", sheet_name="Unfaelle", index=False)
    # See how many unique feauterIds are in the data
    unique_feature_ids = df["featureId"].nunique()
    print(unique_feature_ids)

def get_bs_accidents_by_grid(row):
    geodata = f"{row[1].left},{row[1].bottom},{row[1].right},{row[1].top}"
    url = "https://api3.geo.admin.ch/rest/services/api/MapServer/identify"

    params = {
        "geometryType": "esriGeometryEnvelope",
        "geometry": geodata,
        "imageDisplay": "500,600,96",
        "mapExtent": "548945.5,147956,549402,148103.5",
        "tolerance": 1,
        "layers": "all:ch.astra.unfaelle-personenschaeden_alle",
    }

    response = requests.get(url, params=params)
    response.raise_for_status()  # Raises HTTPError for bad responses

    data = response.json()
    return pd.json_normalize(data.get("results", []))  # Use 'results' field if present

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.info(f"Executing {__file__}...")
    main()
    logging.info("Job successful!")
