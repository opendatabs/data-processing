"""Search terms per chunk: the questions a chunk answers and other names for its matter.

Written from the sweep's summary of the chunk, indexed by the MCP, never quoted.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime

from openai import OpenAI, OpenAIError
from pydantic import BaseModel, Field

from corpus_extract import progress

# Short answers: vocabulary, not prose.
MAX_TOKENS = 600
CONCURRENCY = 16

SYSTEM = """\
Du hilfst dabei, Dokumente des Grossen Rates Basel-Stadt auffindbar zu machen.

Dir wird zusammengefasst, worum es in einem Geschäft geht. Schreibe auf, wie jemand danach
suchen würde, der das Dokument nicht kennt und die Amtssprache nicht benutzt.

Regeln:
- Antworte nur mit dem verlangten JSON. Alles auf Deutsch.
- `fragen`: 5 bis 8 Fragen in Alltagssprache, die dieses Dokument beantwortet. Ganze
  Fragen, wie man sie einem Suchfeld stellt, nicht Stichworte.
- `begriffe`: 6 bis 12 einzelne Wörter, unter denen jemand dieses Geschäft suchen würde.
  Ausdrücklich erwünscht sind andere Wörter als im Text: die umgangssprachlichen
  ("Fahrrad" neben "Velo", "Bericht" neben "Ratschlag"), Oberbegriffe, verwandte
  Sachgebiete, auch das Gegenteil, wenn danach gesucht würde.
- Erfinde keine Tatsachen: keine Namen, Zahlen, Orte oder Daten, die nicht in der
  Zusammenfassung stehen. Frage und Begriff müssen zum Inhalt passen — ein Begriff, unter
  dem dieses Dokument niemanden weiterbringt, schadet mehr, als er nützt.
"""

USER = """\
Titel: {titel}
Geschäft: {signatur}

Zusammenfassung:
{summary}
"""


class Expansion(BaseModel):
    """The words a searcher would use for this document."""

    fragen: list[str] = Field(default_factory=list, max_length=8)
    begriffe: list[str] = Field(default_factory=list, max_length=12)

    def text(self) -> str:
        """The expansion as the one text the index takes."""
        return "\n".join([*self.fragen, " ".join(self.begriffe)]).strip()


class ExpansionError(RuntimeError):
    """The model could not give a usable answer for this document."""


@dataclass(frozen=True, slots=True)
class Outcome:
    """What a run achieved."""

    written: int
    errors: int


# `seiten`: the pages the chunk covers.
SCHEMA = """
CREATE TABLE IF NOT EXISTS expansions (
    dok_laufnr    INTEGER NOT NULL,
    teil          INTEGER NOT NULL DEFAULT 1,
    seiten        TEXT NOT NULL DEFAULT '',
    fragen_json   TEXT,
    status        TEXT NOT NULL,
    error         TEXT,
    model         TEXT NOT NULL DEFAULT '',
    created_at    TEXT NOT NULL,
    PRIMARY KEY (dok_laufnr, teil)
);

CREATE INDEX IF NOT EXISTS idx_expansions_status ON expansions(status);
"""

# Chunks the sweep has read and this step has not.
PENDING = """
    SELECT e.dok_laufnr, e.teil, e.seiten, e.sweep_json
    FROM extractions e
    LEFT JOIN expansions x ON x.dok_laufnr = e.dok_laufnr AND x.teil = e.teil AND x.status IN ('ok', 'fehlt')
    WHERE e.status = 'ok'
      AND e.sweep_json IS NOT NULL
      AND x.dok_laufnr IS NULL
    ORDER BY e.dok_laufnr, e.teil
    LIMIT ?
"""


def summary_of(sweep_json: str) -> tuple[str, str]:
    """The summary and the core demand the sweep wrote, as the input to this step."""
    try:
        sweep = json.loads(sweep_json)
    except ValueError:
        return "", ""
    if not isinstance(sweep, dict):
        return "", ""
    return str(sweep.get("summary") or ""), str(sweep.get("core_demand") or "")


def ask(client: OpenAI, *, titel: str, signatur: str, summary: str, model: str) -> Expansion:
    """Ask the model for the words a searcher would use."""
    user = USER.format(titel=titel or "unbekannt", signatur=signatur or "unbekannt", summary=summary)
    try:
        answer = client.chat.completions.parse(
            model=model,
            temperature=0.0,
            max_tokens=MAX_TOKENS,
            messages=[{"role": "system", "content": SYSTEM}, {"role": "user", "content": user}],
            response_format=Expansion,
        )
    except OpenAIError as error:
        raise ExpansionError(f"request failed: {error}") from error
    except ValueError as error:
        raise ExpansionError(f"answer did not match the schema: {error}") from error
    if answer.choices[0].message.parsed is None:
        raise ExpansionError("the model returned nothing")
    return answer.choices[0].message.parsed


def _record(conn: sqlite3.Connection, row: tuple[object, ...]) -> None:
    """Record what happened to a chunk."""
    conn.execute(
        "INSERT OR REPLACE INTO expansions"
        " (dok_laufnr, teil, seiten, fragen_json, status, error, model, created_at)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        row,
    )


def one(
    conn: sqlite3.Connection,
    client: OpenAI,
    document: sqlite3.Row,
    *,
    model: str,
    titles: dict[int, tuple[str, str]] | None = None,
    lock: threading.Lock | None = None,
) -> bool:
    """Expand one chunk; True if it was written."""
    now = datetime.now(UTC).isoformat(timespec="seconds")
    writing = lock or threading.Lock()
    number = int(document["dok_laufnr"])
    teil = int(document["teil"])
    seiten = document["seiten"] or ""
    titel, signatur = (titles or {}).get(number, ("", ""))

    summary, demand = summary_of(document["sweep_json"] or "")
    whole = "\n".join(part for part in (summary, demand) if part)
    if not whole.strip():
        with writing, conn:
            _record(conn, (number, teil, seiten, None, "fehlt", "the sweep has no summary", model, now))
        return False

    try:
        expansion = ask(client, titel=titel, signatur=signatur, summary=whole, model=model)
    except ExpansionError as problem:
        with writing, conn:
            _record(conn, (number, teil, seiten, None, "fehler", str(problem)[:500], model, now))
        return False

    with writing, conn:
        _record(
            conn,
            (
                number,
                teil,
                seiten,
                json.dumps(expansion.model_dump(mode="json"), ensure_ascii=False),
                "ok",
                None,
                model,
                now,
            ),
        )
    return True


def _titles(conn: sqlite3.Connection) -> dict[int, tuple[str, str]]:
    """Title and business number per document, from the attached MCP database."""
    try:
        rows = conn.execute("SELECT dok_laufnr, titel_dok, signatur_ges FROM mcp.documents")
    except sqlite3.Error:
        return {}
    return {int(r[0]): (r[1] or "", r[2] or "") for r in rows}


def run(
    conn: sqlite3.Connection,
    llm_client: OpenAI,
    *,
    model: str,
    limit: int = 500,
    concurrency: int = CONCURRENCY,
) -> Outcome:
    """Expand the next chunks the sweep has read."""
    conn.executescript(SCHEMA)
    titles = _titles(conn)
    documents = conn.execute(PENDING, (limit,)).fetchall()
    if not documents:
        return Outcome(written=0, errors=0)
    lock = threading.Lock()
    with ThreadPoolExecutor(max_workers=max(1, concurrency)) as pool:
        answered = pool.map(
            lambda document: one(conn, llm_client, document, model=model, titles=titles, lock=lock), documents
        )
        results = list(progress.track(answered, label="expanding chunks", total=len(documents)))
    return Outcome(written=sum(results), errors=len(results) - sum(results))
