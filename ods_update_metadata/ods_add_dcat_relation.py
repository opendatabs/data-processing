import ods_utils_py as ods_utils
import json
import pandas as pd
import logging
from tqdm import tqdm   

EXCEL_PATH = "reference_to_relation.xlsx"


def pick_visible(field_obj):
    """Ermittelt den sichtbaren Wert aus einem Feldobjekt (override > remote_value > value)."""
    if not isinstance(field_obj, dict):
        return None
    orv = field_obj.get("override_remote_value", False)
    val = field_obj.get("value")
    rem = field_obj.get("remote_value")
    clean = lambda s: s.strip() if isinstance(s, str) and s.strip() else None
    return clean(val) if orv else (clean(rem) or clean(val))


def normalize_to_list(val):
    """Gibt eine Liste von Strings zurück (auch bei nur einem Link)."""
    if val is None:
        return None
    if isinstance(val, str):
        parts = [p.strip() for p in val.split(";") if p.strip()]
        return parts if parts else None
    if isinstance(val, list):
        return [p.strip() for p in val if isinstance(p, str) and p.strip()] or None
    return None


def hide_field(field):
    """
    Macht ein Feld unsichtbar:
    - value -> None
    - override_remote_value -> True
    - remote_value -> None (falls nicht vorhanden)
    """
    if not isinstance(field, dict):
        raise ValueError("field must be a dict")

    if  not field.get("remote_value"):
        field["remote_value"] = ""
        
    field["value"] = ""
    field["override_remote_value"] = True
    return field



def main():
    # ids = ["100375"] test
    ids = ods_utils.get_all_dataset_ids()
    logging.info(f"{len(ids)} IDs geladen.")

    rows = []
    for ds_id in tqdm(ids, desc="Verarbeite Datensätze", unit="DS", total=len(ids)):
        logging.info(f"Verarbeite {ds_id}...")

        try:
            references = ods_utils.get_template_metadata(template_name="default", field_name="references",
                                                   dataset_id=ds_id,)
        except Exception as e:
            logging.warning(f"{ds_id}: references konnte nicht geladen werden: {e}")
            rows.append({"id": ds_id, "Reference": None, "Relation": None})
            continue

        references_val = pick_visible(references)
        relation_value = normalize_to_list(references_val)

        if relation_value:
            payload = {"value": relation_value, "override_remote_value": True}
            try:
                ods_utils.set_template_metadata(template_name="dcat", field_name="relation", payload=payload,
                                                dataset_id=ds_id, publish=True)
            except Exception as e:
                logging.info(f"[{ds_id}] Fehler beim Speichern relation: {e}")
        else:
            logging.info(f"[{ds_id}] Kein Wert in references - relation unverändert.")

        # references unsichtbar machen
        references = hide_field(references)
        try:
            ods_utils.set_template_metadata(template_name="default", field_name="references", payload=references,
                dataset_id=ds_id, publish=True)
        except Exception as e:
            logging.info(f"[{ds_id}] Fehler beim Verstecken references: {e}")

        rows.append({"id": ds_id, "Reference": references_val, "Relation": relation_value})

    pd.DataFrame(rows, columns=["id", "Reference", "Relation"]).to_excel(EXCEL_PATH, index=False)
    logging.info(f"Fertig. Excel: {EXCEL_PATH}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format="%(levelname)s: %(message)s")
    logging.info(f"Executing {__file__}...")
    main()
    logging.info("Job completed successfully!")
