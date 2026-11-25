"""The open data from data.bs.ch, mirrored into one table each."""

from __future__ import annotations

import csv
import hashlib
import io
import json
import sqlite3
from dataclasses import dataclass, field

import httpx
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

BASE_URL = "https://data.bs.ch/api/explore/v2.1"
# data.bs.ch answers a large export with a 5xx now and then; it is asked again after a pause.
ATTEMPTS = 4


@dataclass(frozen=True, slots=True)
class Spec:
    """A dataset: where it comes from, what its table is called and which columns it has."""

    dataset: str
    table: str
    columns: dict[str, str]
    key: tuple[str, ...] = field(default_factory=tuple)
    #: 'json' where a field holds several values; the CSV export keeps only one.
    format: str = "csv"
    #: Columns the re-index looks rows up by.
    indexes: tuple[str, ...] = ()

    def create_sql(self) -> str:
        """The CREATE TABLE belonging to this dataset."""
        columns = [f"    {name} TEXT" for name in self.columns]
        if self.key:
            columns.append(f"    PRIMARY KEY ({', '.join(self.key)})")
        return f"CREATE TABLE IF NOT EXISTS {self.table} (\n" + ",\n".join(columns) + "\n)"

    def index_sql(self) -> list[str]:
        """The CREATE INDEX statements belonging to this dataset."""
        return [f"CREATE INDEX IF NOT EXISTS idx_{self.table}_{name} ON {self.table}({name})" for name in self.indexes]

    def insert_sql(self) -> str:
        """The INSERT that writes or replaces a row."""
        names = ", ".join(self.columns)
        placeholders = ", ".join("?" * len(self.columns))
        return f"INSERT OR REPLACE INTO {self.table} ({names}) VALUES ({placeholders})"


DOCUMENTS = Spec(
    dataset="100313",
    table="dokumente",
    key=("dok_laufnr",),
    indexes=("signatur_ges",),
    columns={
        "dok_laufnr": "dok_laufnr",
        "signatur_dok": "signatur_dok",
        "signatur_ges": "signatur_ges",
        "titel_dok": "titel_dok",
        "dokudatum": "dokudatum",
        "url_dok": "url_dok",
    },
)

BUSINESS_ITEMS = Spec(
    dataset="100311",
    table="geschaefte",
    key=("signatur_ges",),
    columns={
        "signatur_ges": "signatur_ges",
        "titel_ges": "titel_ges",
        "ga_rr_gr": "ga_rr_gr",
        "beginn_ges": "beginn_ges",
        "ende_ges": "ende_ges",
        "status_ges": "status_ges",
        "departement_ges": "departement_ges",
        "urheber": "name_vorname_urheber",
        "urheber_nr": "nr_urheber",
        "gremientyp_urheber": "gremientyp_urheber",
        "partei_urheber": "partei_kname_urheber",
        "url_ges": "url_ges",
    },
)

ASSIGNMENTS = Spec(
    dataset="100312",
    table="zuweisungen",
    columns={
        "signatur_ges": "signatur_ges",
        "uni_nr_an": "uni_nr_an",
        "name_an": "name_an",
        "titel_zuw": "titel_zuw",
        "status_zuw": "status_zuw",
        "termin": "termin",
        "erledigt": "erledigt",
    },
)

PROCEEDINGS = Spec(
    dataset="100314",
    table="vorgaenge",
    columns={
        "signatur_ges": "signatur_ges",
        "siz_datum": "siz_datum",
        "nummer": "nummer",
        "beschlnr": "beschlnr",
        "vermerk": "vermerk",
    },
)

PEOPLE = Spec(
    dataset="100307",
    table="personen",
    key=("uni_nr", "gr_beginn"),
    columns={
        "uni_nr": "uni_nr",
        "name_vorname": "name_vorname",
        "partei": "partei_kname",
        "wahlkreis": "gr_wahlkreis",
        "gr_beginn": "gr_beginn",
        "gr_ende": "gr_ende",
        "ist_aktuell": "ist_aktuell_grossrat",
        "beruf": "gr_beruf",
        "url": "url",
    },
)

MEMBERSHIPS = Spec(
    dataset="100308",
    table="mitgliedschaften",
    columns={
        "uni_nr_gre": "uni_nr_gre",
        "name_gre": "name_gre",
        "gremientyp": "gremientyp",
        "uni_nr_adr": "uni_nr_adr",
        "name_vorname": "name_vorname",
        "funktion": "funktion_adr",
        "beginn": "beginn_mit",
        "ende": "ende_mit",
    },
)

INTERESTS = Spec(
    dataset="100309",
    table="interessen",
    columns={
        "uni_nr": "uni_nr",
        "name_vorname": "name_vorname",
        "rubrik": "rubrik",
        "organisation": "intr_bind",
        "funktion": "funktion",
        "text": "text",
    },
)

CO_AUTHORS = Spec(
    dataset="100311",
    table="miturheber",
    columns={
        "signatur_ges": "signatur_ges",
        "nr": "nr_miturheber",
        "name_vorname": "name_vorname_miturheber",
        "partei": "partei_kname_miturheber",
    },
)

COMMITTEES = Spec(
    dataset="100310",
    table="gremien",
    key=("uni_nr",),
    columns={
        "uni_nr": "uni_nr",
        "kurzname": "kurzname",
        "name": "name",
        "gremientyp": "gremientyp",
    },
)

MOTIONS = Spec(
    dataset="100086",
    table="vorstoesse",
    key=("signatur",),
    columns={
        "signatur": "signatur",
        "geschaeftstyp": "geschaftstyp",
        "partei": "partei",
        "urheber": "urheber",
        "titel": "titel",
        "beginn_datum": "beginn_datum",
        "thema_1": "thema_1",
        "thema_2": "thema_2",
        "schwerpunkt": "schwerpunkt",
        "status": "status",
    },
)

VOTES = Spec(
    dataset="100186",
    table="stimmen",
    key=("abst_nr", "datum", "gr_uni_nr"),
    columns={
        "abst_nr": "abst_nr",
        "datum": "datum",
        "gr_uni_nr": "gr_uni_nr",
        "signatur_ges": "signatur_ges",
        "name_vorname": "mitglied_name",
        "fraktion": "fraktion",
        "entscheid": "entscheid_mitglied",
    },
)

AGENDAS = Spec(
    dataset="100348",
    table="tagesordnungen",
    key=("traktanden_idnr",),
    columns={
        "traktanden_idnr": "traktanden_idnr",
        "tagesordnung_idnr": "tagesordnung_idnr",
        "titel": "titel",
        "status": "status",
        "kommission": "kommission",
        "departement": "departement",
        "signatur_ges": "signatur",
        "anr": "anr",
        "tag1": "tag1",
        "text1": "text1",
        "url_vollprotokoll": "url_vollprotokoll",
    },
    format="json",
)

CATALOG = (
    DOCUMENTS,
    BUSINESS_ITEMS,
    CO_AUTHORS,
    ASSIGNMENTS,
    PROCEEDINGS,
    PEOPLE,
    COMMITTEES,
    MEMBERSHIPS,
    INTERESTS,
    MOTIONS,
    VOTES,
    AGENDAS,
)


def _value(raw: object) -> str:
    """A field as it goes into the table: lists as JSON, missing as empty."""
    if raw is None:
        return ""
    if isinstance(raw, list | dict):
        return json.dumps(raw, ensure_ascii=False)
    return str(raw)


def _is_server_error(error: BaseException) -> bool:
    """Whether the portal failed rather than the request."""
    return isinstance(error, httpx.HTTPStatusError) and error.response.is_server_error


@retry(
    retry=retry_if_exception(_is_server_error),
    stop=stop_after_attempt(ATTEMPTS),
    wait=wait_exponential(multiplier=15, max=120),
    reraise=True,
)
def _export(client: httpx.Client, url: str, params: dict[str, str]) -> httpx.Response:
    """One export download; a server error is raised, and so retried."""
    response = client.get(url, params=params, timeout=300.0)
    response.raise_for_status()
    return response


def fetch(client: httpx.Client, spec: Spec) -> list[tuple[str, ...]]:
    """Fetch a dataset and return its rows in the table's column order."""
    params = {"select": ",".join(dict.fromkeys(spec.columns.values())), "delimiter": ";"}
    response = _export(client, f"{BASE_URL}/catalog/datasets/{spec.dataset}/exports/{spec.format}", params)
    if spec.format == "json":
        records: list[dict[str, object]] = response.json()
    else:
        # utf-8-sig: the export starts with a byte-order mark.
        text = response.content.decode("utf-8-sig")
        records = list(csv.DictReader(io.StringIO(text), delimiter=";"))
    return [tuple(_value(record.get(source)) for source in spec.columns.values()) for record in records]


def mirror(conn: sqlite3.Connection, client: httpx.Client, spec: Spec) -> int:
    """Mirror a dataset into its table."""
    rows = fetch(client, spec)
    # Replace the table: the export is always complete.
    with conn:
        conn.execute(f"DROP TABLE IF EXISTS {spec.table}")
        conn.execute(spec.create_sql())
        for statement in spec.index_sql():
            conn.execute(statement)
        conn.executemany(spec.insert_sql(), rows)
    return len(rows)


def digest(conn: sqlite3.Connection) -> str:
    """A hash over every mirrored table's rows: equal when the open data has not changed."""
    hashed = hashlib.sha256()
    for spec in CATALOG:
        hashed.update(spec.table.encode())
        for row in conn.execute(f"SELECT * FROM {spec.table} ORDER BY rowid"):
            hashed.update(repr(tuple(row)).encode())
    return hashed.hexdigest()


def mirror_all(conn: sqlite3.Connection, client: httpx.Client) -> dict[str, int]:
    """Mirror every dataset in the catalog."""
    return {spec.table: mirror(conn, client, spec) for spec in CATALOG}
