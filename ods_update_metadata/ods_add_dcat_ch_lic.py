import logging, time, json, os
import pandas as pd
from tqdm import tqdm
import ods_utils_py as ods_utils
from ods_utils_py._config import get_base_url
from ods_utils_py.get_uid_by_id import get_uid_by_id

EXCEL_PATH = "rights_to_licenses.xlsx"
OUT_DIR = "out_metadata"
TEMPLATE_NAME = "dcat_ap_ch"

# Mapping: rights -> license.value (terms_*)
RIGHTS_TO_LICENSE = {
    "NonCommercialAllowed-CommercialAllowed-ReferenceNotRequired": "terms_open",
    "NonCommercialAllowed-CommercialAllowed-ReferenceRequired": "terms_by",
    "NonCommercialAllowed-CommercialWithPermission-ReferenceNotRequired": "terms_ask",
    "NonCommercialAllowed-CommercialWithPermission-ReferenceRequired": "terms_by_ask",
}

def get_template_metadata(template_name, dataset_id=None, dataset_uid=None):
    """
    Holt das komplette Template (z. B. 'dcat_ap_ch') als JSON-Objekt.
    """
    if dataset_id is not None and dataset_uid is not None:
        raise ValueError(f"dataset_id ({dataset_id}) and dataset_uid ({dataset_uid}) can't both be specified.")
    if dataset_id is None and dataset_uid is None:
        raise ValueError("dataset_id or dataset_uid have to be specified.")
    if dataset_id is not None:
        dataset_uid = get_uid_by_id(dataset_id)

    base_url = get_base_url()
    url = f"{base_url}/datasets/{dataset_uid}/metadata/{template_name}/"
    r = ods_utils.requests_get(url=url)
    r.raise_for_status()
    return r.json()


def wait_until_idle(uid, timeout_sec=180):
    """
    Wartet, bis das Dataset idle ist (oder bricht bei error/timeout ab).
    """
    base_url = get_base_url()
    start = time.time()
    while True:
        status = ods_utils.requests_get(url=f"{base_url}/datasets/{uid}/status").json().get("status")
        if status == "idle":
            return
        if status == "error":
            logging.info(f"{uid}: Dataset im Fehlerzustand - übersprungen.")
            return
        if time.time() - start > timeout_sec:
            logging.warning(f"{uid}: Timeout beim Warten auf 'idle'. Fahre fort.")
            return
        time.sleep(3)


def set_template_metadata(template_name, template_payload, dataset_id=None, dataset_uid=None, publish=True):
    """
    Schreibt das komplette Template (PUT auf /metadata/{template_name}/).
    ACHTUNG: Felder, die im Payload fehlen, können serverseitig gelöscht werden.
    Deshalb Template vorher lesen, gezielt ändern, dann vollständig zurückschicken.
    """
    if dataset_id is not None and dataset_uid is not None:
        raise ValueError(f"dataset_id ({dataset_id}) and dataset_uid ({dataset_uid}) can't both be specified.")
    if dataset_id is None and dataset_uid is None:
        raise ValueError("dataset_id or dataset_uid have to be specified.")
    if dataset_id is not None:
        dataset_uid = get_uid_by_id(dataset_id)

    base_url = get_base_url()
    wait_until_idle(dataset_uid)
    url = f"{base_url}/datasets/{dataset_uid}/metadata/{template_name}/"
    r = ods_utils.requests_put(url=url, json=template_payload)
    r.raise_for_status()

    if publish:
        ods_utils.set_dataset_public(dataset_uid=dataset_uid)


def pick_visible_rights(dcat_ap_ch):
    """
    Ermittelt den im Portal sichtbaren rights-Wert:
    override=True -> value
    sonst: remote_value (falls vorhanden), sonst value
    """
    rights = dcat_ap_ch.get("rights")
    if not isinstance(rights, dict):
        return None
    orv = rights.get("override_remote_value", False)
    val = rights.get("value")
    rem = rights.get("remote_value")
    if orv is True:
        return val.strip() if isinstance(val, str) and val.strip() else None
    if isinstance(rem, str) and rem.strip():
        return rem.strip()
    if isinstance(val, str) and val.strip():
        return val.strip()
    return None


def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    ids = ods_utils.get_all_dataset_ids()
    # Testlauf: nur bestimmte IDs 
    #ids = ['100366']
    logging.info(f"{len(ids)} IDs geladen.")

    rows = []  # Excel: id | Rechte | License
    for ds_id in tqdm(ids, desc="Verarbeite Datensätze", unit="DS", total=len(ids)):
        logging.info(f"Verarbeite {ds_id}...")

        # 1) komplettes Template laden
        try:
            template= get_template_metadata(TEMPLATE_NAME, dataset_id=ds_id)
        except Exception as e:
            logging.warning(f"{ds_id}: Template konnte nicht geladen werden: {e}")
            rows.append({"id": ds_id, "Rechte": None, "License": None})
            continue

        if not isinstance(template, dict):
            logging.warning(f"{ds_id}: {TEMPLATE_NAME} nicht als dict - übersprungen.")
            rows.append({"id": ds_id, "Rechte": None, "License": None})
            continue

        # 2) sichtbaren rights-Wert lesen
        rights_val = pick_visible_rights(template)

        # 3) Mapping -> license.value
        license_value = RIGHTS_TO_LICENSE.get(rights_val) if rights_val else None

        if license_value:

            # 4) Nur das license-Feld im Template anfassen; Rest unverändert lassen
            license_obj = template.get("license")
            if not isinstance(license_obj, dict):
                license_obj = {}
            license_obj["value"] = license_value

            # Wir setzen explizit override_remote_value=True, damit der Wert sichtbar greift
            license_obj["override_remote_value"] = True
            template["license"] = license_obj

            # 5) Template zurückschreiben
            try:
                set_template_metadata(TEMPLATE_NAME, template_payload=template, dataset_id=ds_id, publish=True)
            except Exception as e:
                logging.info(f"[{ds_id}] Fehler beim Speichern des Templates: {e}")
        else:
            logging.info(f"[{ds_id}] Kein Mapping für rights='{rights_val}' – license unverändert.")

        # 6) Excel-Reportzeile
        rows.append({"id": ds_id, "Rechte": rights_val, "License": license_value})

    # 7) Excel-Report schreiben
    pd.DataFrame(rows, columns=["id", "Rechte", "License"]).to_excel(EXCEL_PATH, index=False)
    logging.info(f"Fertig. Excel: {EXCEL_PATH}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format="%(levelname)s: %(message)s")
    logging.info("Starting...")
    main()
    logging.info("Job completed successfully!")
