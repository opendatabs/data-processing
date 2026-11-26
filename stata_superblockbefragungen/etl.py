"""Superblock-Befragungen: Excel-Exporte als CSV publizieren und Spaltenlabels auf Huwise setzen.

1. Die Excel-Exporte in data_orig werden als CSV mit technischen Spaltennamen nach data geschrieben,
   dazu variablen.csv mit der Zuordnung Spalte -> Fragetext.
2. Geänderte CSV werden auf den FTP geladen und der Datensatz auf data.bs.ch publiziert.
3. Pro Datensatz werden Anzeigenamen und Beschreibungen der Spalten als Huwise-Prozessoren gesetzt,
   sofern sie sich seit dem letzten erfolgreichen Lauf geändert haben.
"""

from __future__ import annotations

import collections
import csv
import json
import logging
import re
import time
from datetime import date, datetime
from pathlib import Path

import common
import common.change_tracking as ct
import httpx
import openpyxl
from dotenv import load_dotenv
from huwise_utils_py import HuwiseDataset, get_uid_by_id, list_dataset_field_configurations
from huwise_utils_py.config import HuwiseConfig
from huwise_utils_py.http import HttpClient
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.datavalidation import DataValidation

load_dotenv()

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "data_orig"
OUT = ROOT / "data"
VARIABLEN_FILE = SRC / "variablen.xlsx"
VARIABLEN_HEADERS = ("Technischer Name", "Spaltenname", "Spaltenbeschreibung", "Gilt für", "Fragetext im Export (Info)")
ALL_PERIMETERS = "Beide"
FTP_REMOTE_PATH = "befragungen/superblock"
HUWISE_PROCESSOR_PREFIX = "superblock"
HUWISE_IDLE_TIMEOUT = 600
HUWISE_IDLE_POLL = 3

# Nullmessung hat im Export keine Fragenummern. Die Namen folgen der
# Vergleichsmessung 2026, wo dieselbe Frage vorkommt. f14j und f15h gibt
# es nur in der Nullmessung.
NULL_COLUMNS = [
    ("id", "Antwort ID", None),
    ("f1", "Jahrgang", None),
    ("f2", "Geschlecht", None),
    ("f3", "Wohnsituation", None),
    ("f4a", "0 bis 5", None),
    ("f4b", "6 bis 12", None),
    ("f4c", "13 bis 18", None),
    ("f4d", "19", None),
    ("f5", "Sprache", None),
    (
        "f12",
        "Strasse in Ihrer Freizeit",
        "In der Vergleichsmessung lautet die Frage auf den Superblock, nicht auf die Strasse.",
    ),
    ("f13", "Ihre Kinder", "In der Vergleichsmessung lautet die Frage auf den Superblock, nicht auf die Strasse."),
    ("f14a", "wohl", None),
    ("f14d", "treffen", None),
    ("f14e", "kennen", None),
    ("f14g", "Aktivitäten", None),
    ("f14j", "mitgestalten", "Nur in der Nullmessung. Kein gleichlautendes Item in der Vergleichsmessung."),
    ("f14i", "belegen", None),
    ("f15a", "sicher", None),
    ("f15b", "gefährlich", None),
    (
        "f15h",
        "zumutbar",
        "Nur in der Nullmessung. Nicht dasselbe Item wie f15c (weniger Verkehr) in der Vergleichsmessung.",
    ),
    ("f15e", "Nachtruhe", None),
    ("f15f", "Abfall", None),
    ("f17a", "Aufenthalt ein", None),
    ("f17b", "belebt", None),
    ("f17f", "begrünt", None),
    ("f17c", "Sitzmöglichkeiten", None),
    ("f17e", "Witterung", None),
    ("f18", "am häufigsten unterwegs", None),
    ("f18other", "Sonstiges", None),
    ("f20", "eigenes Auto", None),
    ("f21", "parkieren", None),
    ("f22", "Minuten", None),
    ("f8", "Einführung von Superblocks", None),
    ("f9sq001", "interessiert mich", None),
    ("f9sq002", "gefällt mir", None),
    ("f23", "noch etwas mitteilen", None),
]

DATASETS = [
    {
        "id": "100558",
        "slug": "anwohnende_nullmessung_stjohann",
        "source": "Rohdaten_Vorerhebung_Anwohner_StJohann - FHNW.xlsx",
        "kind": "null",
    },
    {
        "id": "100559",
        "slug": "anwohnende_nullmessung_matthaeus",
        "source": "Rohdaten_Vorerhebung_Anwohner_Matthäus_FHNW.xlsx",
        "kind": "null",
    },
    {
        "id": "100560",
        "slug": "anwohnende_vergleich_stjohann",
        "source": "St Johann Anwohnenden Vergleichsmessung 2026 FHNW.xlsx",
        "kind": "coded",
    },
    {
        "id": "100561",
        "slug": "anwohnende_vergleich_matthaeus",
        "source": "Matthäus Anwohnenden Vergleichsmessung 2026 FHNW.xlsx",
        "kind": "coded",
    },
    {
        "id": "100562",
        "slug": "nutzende_stjohann",
        "source": "Nutzendenbefragung_Export_St. Johann total_20.08.26 FHNW.xlsx",
        "kind": "coded",
    },
    {
        "id": "100563",
        "slug": "nutzende_matthaeus",
        "source": "Nutzendenbefragung_Matthäus_total 31_08_26 FHNW.xlsx",
        "kind": "coded",
    },
    {
        "id": "100564",
        "slug": "gewerbe_stjohann",
        "source": "St Johann Gewerbebefragung 2026 FHNW.xlsx",
        "kind": "coded",
    },
    {
        "id": "100565",
        "slug": "gewerbe_matthaeus",
        "source": "Matthäus Gewerbebefragung 2026 FHNW.xlsx",
        "kind": "coded",
    },
]

CODED_HEADER = re.compile(
    r"^(?P<code>id|startdate|[A-Za-z]+\d+)(?:\[(?P<sub>[^\]]+)\])?\.\s*(?P<rest>.*)$",
    re.DOTALL,
)
TECH_NAME = re.compile(r"^[a-z][a-z0-9]*$")

# Anonymisierung, Begründung in data/anonymisierung.md.
DROP_COLUMNS = {
    "alle": {"startdate"},
    "anwohnende": {"f4a", "f4b", "f4c", "f4d", "f18other", "f23"},
    "nutzende": {"f12other"},
    "gewerbe": {"f2", "f3", "f11"},
}
NULLMESSUNG_YEAR = 2025
MIN_BIRTH_YEAR = 1910
RARE_MAX = 4
PARKING_MERGE = {"Gemieteter Parkplatz", "Parkplatz auf Privatgrund"}
PARKING_MERGED = "Gemieteter Parkplatz oder Parkplatz auf Privatgrund"
NOTE_AGE = (
    "Aus Datenschutzgründen ist der Jahrgang in Altersklassen zusammengefasst: Unter 30 Jahre, "
    "Zwischen 30 und 59 Jahre, Über 60 Jahre (ab 60 Jahren). Das Alter ist Befragungsjahr minus Jahrgang. "
    f"Unplausible Jahrgänge (nicht vierstellig, vor {MIN_BIRTH_YEAR} oder nach dem Befragungsjahr) sind leer."
)
NOTE_LANGUAGE = (
    f"Aus Datenschutzgründen sind Sprachen mit {RARE_MAX} oder weniger Antworten unter «Andere» zusammengefasst."
)
NOTE_PARKING = (
    "Aus Datenschutzgründen sind «Gemieteter Parkplatz» und «Parkplatz auf Privatgrund» zu "
    f"«{PARKING_MERGED}» zusammengefasst."
)
NOTE_QUARTER = (
    f"Aus Datenschutzgründen sind Quartiere in Basel mit {RARE_MAX} oder weniger Antworten unter "
    "«Anderes Quartier in Basel» zusammengefasst."
)

# N/A ist im LimeSurvey-Export die Kennung für nicht gestellte Fragen.
# In der Vergleichsmessung fehlen bei der Zustimmungsskala Leerzeichen.
VALUE_FIXES = {
    "N/A": "",
    "Trifftzu": "Trifft zu",
    "Trifft eherzu": "Trifft eher zu",
    "Trifftnicht zu": "Trifft nicht zu",
    "Trifft ehernicht zu": "Trifft eher nicht zu",
    "Weissnicht": "Weiss nicht",
}

GENERIC_STEM = "bitte wählen sie die für sie zutreffende antwort"
GLUE_FIXES = (
    ("treffenund", "treffen und"),
    ("gefährlichenSituationen", "gefährlichen Situationen"),
    ("Strasseein", "Strasse ein"),
    ("Superblock(z.B.", "Superblock (z.B."),
    ("Kaffeekränzchen,Flohmarkt", "Kaffeekränzchen, Flohmarkt"),
    ("innnerhalb", "innerhalb"),
    ("hautpsächlich", "hauptsächlich"),
)
LABEL_OVERRIDES = {
    "Bitte geben Sie Ihren Jahrgang an (bitte vierstellig, z.B. 1980)": "Bitte geben Sie Ihren Jahrgang an",
}


def cell_to_text(value) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    if isinstance(value, str):
        text = value.strip()
        return VALUE_FIXES.get(text, text)
    return str(value)


def split_item(text: str) -> tuple[str, str]:
    match = re.search(r"\[([^\]]*)\]\s*$", text)
    if not match:
        return text.strip(), ""
    return text[: match.start()].strip(), match.group(1).strip()


def technical_from_coded(header: str) -> tuple[str, str, str]:
    match = CODED_HEADER.match(header.strip())
    if not match:
        raise ValueError(f"Kopfzeile ohne Fragenummer: {header!r}")
    code = match.group("code").lower()
    sub = match.group("sub")
    rest = match.group("rest").strip()
    if code in {"id", "startdate"}:
        return code, rest, ""
    name = f"{code}{re.sub(r'[^a-z0-9]+', '', sub.strip().lower())}" if sub else code
    frage, item = split_item(rest)
    return name, frage, item


def columns_for_null(headers: list[str]) -> list[dict]:
    if len(headers) != len(NULL_COLUMNS):
        raise ValueError(f"Nullmessung hat {len(headers)} Spalten, erwartet {len(NULL_COLUMNS)}")
    columns = []
    for header, (name, needle, note) in zip(headers, NULL_COLUMNS):
        if needle.lower() not in header.lower():
            raise ValueError(f"{name}: erwartete {needle!r} in {header!r}")
        frage, item = ("Antwort-ID", "") if name == "id" else split_item(header.strip())
        columns.append(
            {"technischer_name": name, "frage": frage, "item": item, "originalspalte": header, "anmerkung": note or ""}
        )
    return columns


def columns_for_coded(headers: list[str]) -> list[dict]:
    columns = []
    for header in headers:
        name, frage, item = technical_from_coded(header)
        columns.append(
            {"technischer_name": name, "frage": frage, "item": item, "originalspalte": header, "anmerkung": ""}
        )
    return columns


def load_sheet(path: Path) -> tuple[list[str], list[tuple]]:
    workbook = openpyxl.load_workbook(path, read_only=True, data_only=True)
    worksheet = workbook[workbook.sheetnames[0]]
    rows = list(worksheet.iter_rows(values_only=True))
    workbook.close()
    headers = ["" if cell is None else str(cell) for cell in rows[0]]
    return headers, rows[1:]


def csv_path(dataset: dict) -> Path:
    return OUT / f"{dataset['id']}_{dataset['slug']}.csv"


def group_of(dataset: dict) -> str:
    return dataset["slug"].split("_")[0]


def survey_year(row: dict[str, str]) -> int:
    return int(row["startdate"][:4]) if row.get("startdate") else NULLMESSUNG_YEAR


def age_class(row: dict[str, str]) -> str:
    value = row["f1"]
    year = survey_year(row)
    if not (len(value) == 4 and value.isdigit() and MIN_BIRTH_YEAR <= int(value) <= year):
        return ""
    age = year - int(value)
    if age < 30:
        return "Unter 30 Jahre"
    if age < 60:
        return "Zwischen 30 und 59 Jahre"
    return "Über 60 Jahre"


def merge_rare(rows: list[dict[str, str]], column: str, target: str, keep: tuple[str, ...] = ()) -> None:
    counts = collections.Counter(row[column] for row in rows if row[column])
    for row in rows:
        value = row[column]
        if value and value not in keep and counts[value] <= RARE_MAX:
            row[column] = target


def anonymize(dataset: dict, columns: list[dict], rows: list[dict[str, str]]) -> list[dict]:
    group = group_of(dataset)
    drop = DROP_COLUMNS["alle"] | DROP_COLUMNS[group]
    notes: dict[str, str] = {}
    labels: dict[str, str] = {}

    if group in ("anwohnende", "nutzende"):
        for row in rows:
            row["f1"] = age_class(row)
        labels["f1"] = "Altersklasse"
        notes["f1"] = NOTE_AGE
        language = "f5" if group == "anwohnende" else "f3"
        merge_rare(rows, language, "Andere")
        notes[language] = NOTE_LANGUAGE
    if group == "anwohnende":
        for row in rows:
            if row["f21"] in PARKING_MERGE:
                row["f21"] = PARKING_MERGED
        notes["f21"] = NOTE_PARKING
    if group == "nutzende":
        merge_rare(rows, "f4", "Anderes Quartier in Basel", keep=("Ich wohne nicht in Basel",))
        notes["f4"] = NOTE_QUARTER

    kept = []
    for column in columns:
        name = column["technischer_name"]
        if name in drop:
            continue
        note = " ".join(part for part in (column["anmerkung"], notes.get(name, "")) if part)
        kept.append({**column, "anmerkung": note, "anzeigename": labels.get(name, "")})
    for row in rows:
        for name in drop:
            row.pop(name, None)
    return kept


def write_dataset(dataset: dict) -> list[dict]:
    headers, data_rows = load_sheet(SRC / dataset["source"])
    columns = columns_for_null(headers) if dataset["kind"] == "null" else columns_for_coded(headers)

    names = [column["technischer_name"] for column in columns]
    if len(names) != len(set(names)):
        raise ValueError(f"Doppelte Namen in {dataset['id']}: {names}")
    for name in names:
        if not TECH_NAME.match(name):
            raise ValueError(f"Ungültiger technischer Name {name!r}")

    rows = [
        dict(zip(names, (cell_to_text(cell) for cell in row)))
        for row in data_rows
        if row is not None and not all(cell is None or cell == "" for cell in row)
    ]
    columns = anonymize(dataset, columns, rows)
    names = [column["technischer_name"] for column in columns]

    target = csv_path(dataset)
    with target.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=names)
        writer.writeheader()
        writer.writerows(rows)
    logging.info("%s: %s Zeilen, %s Spalten", target.name, len(rows), len(names))
    return [{"datensatz_id": dataset["id"], "dateiname": target.name, **column} for column in columns]


def sheet_of(dataset: dict) -> str:
    group = group_of(dataset)
    if group == "anwohnende":
        return "Anwohnende Nullmessung" if "nullmessung" in dataset["slug"] else "Anwohnende Vergleichsmessung"
    return group.capitalize()


def perimeter_of(dataset: dict) -> str:
    return "St. Johann" if dataset["slug"].endswith("stjohann") else "Matthäus"


def write_variablen_template(variables_by_dataset: dict[str, list[dict]]) -> None:
    """Erstellt data_orig/variablen.xlsx aus den generierten Labels, ein Blatt pro Befragungsart.

    Spalten, deren Text sich zwischen den Perimetern unterscheidet, erhalten eine Zeile pro Perimeter.
    """
    sheets: dict[str, dict[str, dict[str, dict]]] = {}
    for dataset in DATASETS:
        sheet = sheets.setdefault(sheet_of(dataset), {})
        for variable in variables_by_dataset[dataset["id"]]:
            sheet.setdefault(variable["technischer_name"], {})[perimeter_of(dataset)] = variable

    workbook = openpyxl.Workbook()
    workbook.remove(workbook.active)
    for title, columns in sheets.items():
        worksheet = workbook.create_sheet(title)
        worksheet.append(list(VARIABLEN_HEADERS))
        for name, by_perimeter in columns.items():
            texts = {perimeter: label_for(variable) for perimeter, variable in by_perimeter.items()}
            if len(set(texts.values())) == 1:
                variable = next(iter(by_perimeter.values()))
                worksheet.append([name, *texts[next(iter(texts))], ALL_PERIMETERS, clean(variable["originalspalte"])])
                continue
            for perimeter, variable in by_perimeter.items():
                worksheet.append([name, *texts[perimeter], perimeter, clean(variable["originalspalte"])])
        style_variablen_sheet(worksheet)
    workbook.save(VARIABLEN_FILE)
    logging.info("%s erstellt", VARIABLEN_FILE.name)


def style_variablen_sheet(worksheet) -> None:
    header_font = Font(bold=True, color="FFFFFF")
    header_fill = PatternFill("solid", fgColor="116A8D")
    info_font = Font(italic=True, color="6B6B6B")
    wrap = Alignment(wrap_text=True, vertical="top")
    for index, (width, is_info) in enumerate(((18, False), (55, False), (75, False), (14, False), (60, True)), 1):
        letter = get_column_letter(index)
        worksheet.column_dimensions[letter].width = width
        for cell in worksheet[letter][1:]:
            cell.alignment = wrap
            if is_info:
                cell.font = info_font
    for cell in worksheet[1]:
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = Alignment(wrap_text=True, vertical="center")
    worksheet.row_dimensions[1].height = 30
    worksheet.freeze_panes = "B2"
    worksheet.auto_filter.ref = worksheet.dimensions
    validation = DataValidation(type="list", formula1=f'"{ALL_PERIMETERS},St. Johann,Matthäus"', allow_blank=True)
    worksheet.add_data_validation(validation)
    validation.add(f"D2:D{max(worksheet.max_row, 2)}")


def read_variablen() -> dict[tuple[str, str, str], tuple[str, str]]:
    """(Blatt, Gilt für, technischer Name) -> (Spaltenname, Spaltenbeschreibung)."""
    workbook = openpyxl.load_workbook(VARIABLEN_FILE, read_only=True, data_only=True)
    texts = {}
    for worksheet in workbook.worksheets:
        rows = worksheet.iter_rows(values_only=True)
        headers = [str(cell or "").strip().casefold() for cell in next(rows)]
        index = {name: headers.index(name.casefold()) for name in VARIABLEN_HEADERS[:4]}
        for row in rows:
            name = str(row[index["Technischer Name"]] or "").strip()
            if not name:
                continue
            perimeter = str(row[index["Gilt für"]] or "").strip() or ALL_PERIMETERS
            label = clean(str(row[index["Spaltenname"]] or ""))
            description = clean(str(row[index["Spaltenbeschreibung"]] or ""))
            texts[(worksheet.title, perimeter, name)] = (label, description)
    workbook.close()
    return texts


def clean(value: str) -> str:
    text = (value or "").replace(" ", " ").replace(" ", " ")
    for old, new in GLUE_FIXES:
        text = text.replace(old, new)
    return " ".join(text.split())


def question_heading(frage: str) -> str:
    text = frage.strip()
    for suffix in (" (Mehrere Antworten möglich)", " (Bitte alle Veränderungen ankreuzen)"):
        text = text.removesuffix(suffix).strip()
    return text.replace("?", "").strip()


def label_for(variable: dict) -> tuple[str, str]:
    name = variable["technischer_name"]
    frage = clean(variable["frage"])
    item = clean(variable["item"])
    anmerkung = clean(variable["anmerkung"])
    if name == "id":
        return "Antwort-ID", "Eindeutige Antwort-ID aus dem Fragebogenexport."
    if variable.get("anzeigename"):
        return variable["anzeigename"], anmerkung
    if not item:
        return LABEL_OVERRIDES.get(frage, frage), anmerkung
    if frage.casefold().startswith(GENERIC_STEM):
        return item, " ".join(part for part in (frage, anmerkung) if part)
    return f"{question_heading(frage)}: {item}", " ".join(part for part in (frage, anmerkung) if part)


def field_spec(dataset: dict, variables: list[dict], texts: dict[tuple[str, str, str], tuple[str, str]]) -> list[dict]:
    sheet, perimeter = sheet_of(dataset), perimeter_of(dataset)
    fields = []
    seen: dict[str, str] = {}
    for variable in variables:
        name = variable["technischer_name"]
        entry = texts.get((sheet, perimeter, name)) or texts.get((sheet, ALL_PERIMETERS, name))
        if entry and entry[0]:
            label, description = entry
        else:
            logging.warning("%s: kein Spaltenname für %s (%s) in %s", sheet, name, perimeter, VARIABLEN_FILE.name)
            label, description = label_for(variable)
        if not label:
            raise ValueError(f"Leerer Anzeigename für {variable['datensatz_id']}.{name}")
        if label in seen:
            raise ValueError(f"Anzeigename {label!r} doppelt in {variable['datensatz_id']}: {seen[label]} und {name}")
        seen[label] = name
        fields.append({"column": name, "label": label, "description": description})
    return fields


def explore_field_names(config: HuwiseConfig, dataset_id: str) -> list[str]:
    try:
        response = httpx.get(
            f"https://{config.domain}/api/explore/v2.1/catalog/datasets/{dataset_id}",
            headers=config.headers,
            timeout=60,
        )
        response.raise_for_status()
        return [item.get("name", "") for item in response.json().get("fields", [])]
    except Exception as exc:
        logging.warning("Datensatz %s: keine Explore-Feldnamen (%s), verwende technische Namen", dataset_id, exc)
        return []


def huwise_field_name(column: str, explore_names: list[str]) -> str:
    return {name.lower(): name for name in explore_names}.get(column.lower(), column)


def wait_idle(client: HttpClient, dataset_uid: str, reason: str) -> None:
    deadline = time.monotonic() + HUWISE_IDLE_TIMEOUT
    while True:
        payload = client.get(f"/datasets/{dataset_uid}/status").json()
        status = payload.get("status")
        if status == "idle":
            return
        if status == "error":
            raise RuntimeError(f"Huwise-Fehler ({reason}): {payload.get('message') or status}")
        if time.monotonic() >= deadline:
            raise RuntimeError(f"Datensatz nach {HUWISE_IDLE_TIMEOUT}s nicht idle ({reason}), status={status}")
        time.sleep(HUWISE_IDLE_POLL)


def wait_idle_or_republish(client: HttpClient, dataset_uid: str) -> None:
    """Nach einem Schemawechsel verlangt Huwise unpublish und publish."""
    try:
        wait_idle(client, dataset_uid, "vor den Spalten")
    except RuntimeError as exc:
        if "unpublished and republished" not in str(exc):
            raise
        logging.info("Schemawechsel: unpublish und publish")
        client.post(f"/datasets/{dataset_uid}/unpublish/")
        wait_idle(client, dataset_uid, "nach unpublish")
        client.post(f"/datasets/{dataset_uid}/publish/")
        wait_idle(client, dataset_uid, "nach erneutem publish")


def list_field_processors(dataset_id: str) -> list[dict]:
    results: list[dict] = []
    offset = 0
    while True:
        page = list_dataset_field_configurations(dataset_id=dataset_id, limit=100, offset=offset)
        batch = page.get("results") or []
        results.extend(item for item in batch if isinstance(item, dict))
        if not batch or not page.get("next"):
            return results
        offset += 100


def push_huwise_fields(dataset_id: str, fields: list[dict]) -> None:
    """Identity-Rename setzt das Label, ein Description-Prozessor die Beschreibung.

    Technische Spaltennamen, Sichtbarkeit, Metadaten und Feldtypen bleiben unverändert.
    """
    config = HuwiseConfig.from_env()
    client = HttpClient(config)
    dataset_uid = get_uid_by_id(dataset_id=dataset_id)
    wait_idle_or_republish(client, dataset_uid)

    for processor in list_field_processors(dataset_id):
        if str(processor.get("label", "")).startswith(f"{HUWISE_PROCESSOR_PREFIX}: ") and processor.get("uid"):
            wait_idle(client, dataset_uid, "vor dem Löschen")
            client.delete(f"/datasets/{dataset_uid}/fields/{processor['uid']}/")

    explore_names = explore_field_names(config, dataset_id)
    for entry in fields:
        field = huwise_field_name(entry["column"], explore_names)
        processors = [
            {
                "type": "rename",
                "label": f"{HUWISE_PROCESSOR_PREFIX}: label {entry['column']}",
                "from_name": field,
                "to_name": field,
                "field_label": entry["label"],
            }
        ]
        if entry["description"]:
            processors.append(
                {
                    "type": "description",
                    "label": f"{HUWISE_PROCESSOR_PREFIX}: description {entry['column']}",
                    "field": field,
                    "description": entry["description"],
                }
            )
        for processor in processors:
            wait_idle(client, dataset_uid, processor["label"])
            client.post(f"/datasets/{dataset_uid}/fields/", json=processor)
    logging.info("Datensatz %s: %s Spalten gesetzt", dataset_id, len(fields))

    wait_idle(client, dataset_uid, "vor publish")
    HuwiseDataset(uid=dataset_uid).publish()
    wait_idle(client, dataset_uid, "nach publish")
    verify_labels(dataset_id, fields, explore_names)
    logging.info("Datensatz %s: publiziert", dataset_id)


def verify_labels(dataset_id: str, fields: list[dict], explore_names: list[str]) -> None:
    labels = {
        item.get("from_name"): item.get("field_label")
        for item in list_field_processors(dataset_id)
        if item.get("type") == "rename" and item.get("from_name") == item.get("to_name")
    }
    mismatches = [
        (entry["column"], labels.get(huwise_field_name(entry["column"], explore_names)), entry["label"])
        for entry in fields
        if labels.get(huwise_field_name(entry["column"], explore_names)) != entry["label"]
    ]
    if mismatches:
        sample = "; ".join(f"{name}: {got!r} != {want!r}" for name, got, want in mismatches[:8])
        raise RuntimeError(f"Datensatz {dataset_id}: {len(mismatches)} Anzeigenamen weichen ab: {sample}")


def push_changed_huwise_fields(variables_by_dataset: dict[str, list[dict]]) -> None:
    texts = read_variablen()
    for dataset in DATASETS:
        dataset_id = dataset["id"]
        spec = field_spec(dataset, variables_by_dataset[dataset_id], texts)
        spec_path = OUT / f"huwise_fields_{dataset_id}.json"
        spec_path.write_text(json.dumps(spec, ensure_ascii=False, indent=2), encoding="utf-8")
        if not ct.has_changed(str(spec_path)):
            logging.info("Datensatz %s: Spaltenlabels unverändert", dataset_id)
            continue
        try:
            push_huwise_fields(dataset_id, json.loads(spec_path.read_text(encoding="utf-8")))
        except Exception:
            logging.exception("Spaltenlabels für %s fehlgeschlagen, nächster Lauf versucht es erneut", dataset_id)
            continue
        ct.update_hash_file(str(spec_path))


def main() -> None:
    OUT.mkdir(exist_ok=True)
    variables_by_dataset = {dataset["id"]: write_dataset(dataset) for dataset in DATASETS}
    if not VARIABLEN_FILE.exists():
        write_variablen_template(variables_by_dataset)
    for dataset in DATASETS:
        common.update_ftp_and_odsp(str(csv_path(dataset)), FTP_REMOTE_PATH, dataset["id"])
    push_changed_huwise_fields(variables_by_dataset)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f"Executing {__file__}...")
    main()
    logging.info("Job successful!")
