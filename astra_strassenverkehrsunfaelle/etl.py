import logging
import pandas as pd
import requests
import json

def main():
    df = get_all_accidents_by_canton("BS")
    df.to_excel("data/Unfaelle.xlsx", sheet_name="Unfaelle", index=False)

def get_all_accidents_by_canton(canton_code):
    url = "https://api3.geo.admin.ch/rest/services/api/MapServer/identify"
    layer_name = "ch.astra.unfaelle-personenschaeden_alle"
    filter_expr = f"canton = '{canton_code}'"
    layer_defs = json.dumps({layer_name: filter_expr})

    params = {
        "geometryType": "esriGeometryEnvelope",
        "geometry": "0,0,3000000,3000000",  # entire Swiss extent
        "imageDisplay": "500,600,96",
        "mapExtent": "0,0,3000000,3000000",
        "tolerance": 0,
        "layers": f"all:{layer_name}",
        "layerDefs": layer_defs,
    }

    all_results = []
    offset = 0
    batch_size = 50  # API returns a maximum of 50 features per request

    while True:
        params["offset"] = offset
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        results = data.get("results", [])
        if not results:
            break
        all_results.extend(results)
        offset += batch_size

    return pd.json_normalize(all_results)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.info(f"Executing {__file__}...")
    main()
    logging.info("Job successful!")
