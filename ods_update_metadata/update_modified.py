import logging
from pathlib import Path

import ods_utils_py as ods_utils
import pandas as pd
from tqdm import tqdm


def load_ids_from_excel(path, col="ods_id"):
    df = pd.read_excel(path)
    ids = []
    for entry in df[col].dropna():
        parts = [p.strip() for p in str(entry).split(";") if p.strip()]
        ids.extend(parts)
    return ids


def main():
    excel_path = "Metadata.xlsx"
    ids = load_ids_from_excel(excel_path, col="ods_id")

    logging.info(f"{len(ids)} IDs geladen.")

    missing_rows = []  # Liste für fehlende Felder

    for ds_id in tqdm(ids, desc="Verarbeite Datensätze", unit="DS", total=len(ids)):
        logging.info(f"Verarbeite {ds_id}...")

        try:
            default = ods_utils.get_template_metadata(template_name="default", dataset_id=ds_id)
        except Exception as e:
            logging.warning(f"{ds_id}: Default konnte nicht geladen werden: {e}")
            missing_rows.append({"id": ds_id, "missing_field": "default", "note": f"nicht geladen: {e}"})
            continue

        try:
            default["modified"]["override_remote_value"] = False
        except Exception as e:
            missing_rows.append({"id": ds_id, "missing_field": "modified", "note": f"kein Zugriff: {e}"})

        # die beiden anderen nur, wenn vorhanden
        for key in ("modified_updates_on_metadata_change", "modified_updates_on_data_change"):
            if key in default and isinstance(default[key], dict) and "override_remote_value" in default[key]:
                default[key]["override_remote_value"] = False
            else:
                missing_rows.append({"id": ds_id, "missing_field": key, "note": "Feld fehlt oder Struktur unerwartet"})

        try:
            ods_utils.set_template_metadata(
                template_name="default",
                payload=default,
                dataset_id=ds_id,
                publish=True,
            )
        except Exception as e:
            logging.error(f"{ds_id}: Speichern fehlgeschlagen: {e}")
            missing_rows.append({"id": ds_id, "missing_field": "save", "note": f"Fehler beim Speichern: {e}"})

    # Report nur für fehlende Felder
    outpath = Path("missing_fields.csv")
    if missing_rows:
        pd.DataFrame(missing_rows).to_csv(outpath, index=False, encoding="utf-8")
        logging.info(f"Missing-Felder exportiert nach {outpath}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f"Executing {__file__}...")
    main()
    logging.info("Job completed successfully!")
