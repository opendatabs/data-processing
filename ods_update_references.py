import logging

import ods_utils_py as ods_utils
import pandas as pd
from tqdm import tqdm

# 1) Alle IDs in einem Request holen
ids_url = "https://data.bs.ch/api/explore/v2.1/catalog/datasets/100057/exports/json?select=dataset_identifier&limit=-1"
response_data = ods_utils.requests_get(ids_url).json()
r = ods_utils.requests_get(ids_url)
response_data = r.json()
ids = [id["dataset_identifier"] for id in response_data]

# 2) Metadaten holen und references auslesen
rows = []
# tqdm packt einfach dein Iterable ein
for id in tqdm(ids, desc="Lade Metadaten", unit="Datensatz"):
    metadata = ods_utils.get_dataset_metadata(dataset_id=id)
    if not metadata:
        logging.info(f"No metadata found for ID {id}")
        continue

    # Feld "reference" direkt holen
    logging.info(f"Processing ID {id}")
    ref = (
        metadata.get("default", {}).get("references", {}).get("value")
        or metadata.get("reference")  # falls es in manchen Metadaten flach vorliegt
    )

    rows.append({"ods_ids": id, "reference": ref})
# 3) Speichern
pd.DataFrame(rows).to_excel("ods_refs.xlsx", index=False)
print(f"{len(rows)} Zeilen gespeichert in ods_refs.csv")
