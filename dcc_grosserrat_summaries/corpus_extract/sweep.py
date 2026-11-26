"""Let the model read documents and record the quoted relations it finds."""

from __future__ import annotations

import sqlite3
import threading
from collections.abc import Collection
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from openai import OpenAI

from corpus_extract import chunking, corpus, llm, pages, progress
from corpus_extract.claims import Sweep
from corpus_extract.vocabulary import EntityType, allowed, geschaeft_id, node_id

MIN_CHARS = 200
CONCURRENCY = 8

# Reading order: motion texts first, decisions last.
PRIORITY = """
    CASE
        WHEN d.titel_dok LIKE 'Text %' THEN 10
        WHEN d.titel_dok IN ('Ratschlag des RR', 'Schreiben des RR', 'Stellungnahme des RR') THEN 20
        WHEN d.titel_dok LIKE 'Bericht%' THEN 30
        WHEN d.titel_dok = 'Vollprotokoll' THEN 40
        WHEN d.titel_dok LIKE '%Beschluss%' THEN 90
        ELSE 50
    END
"""

CANDIDATES = """
    SELECT d.dok_laufnr, d.titel_dok, d.signatur_ges, d.dokudatum, COALESCE(g.titel_ges, '') AS titel_ges
    FROM mcp.documents d
    LEFT JOIN mcp.geschaefte g ON g.signatur_ges = d.signatur_ges
    WHERE d.chars >= ?
      AND d.dok_laufnr NOT IN (SELECT dok_laufnr FROM extractions GROUP BY dok_laufnr HAVING MIN(status = 'ok') = 1)
    ORDER BY {priority}, d.dokudatum DESC, d.dok_laufnr DESC
    LIMIT ?
"""


@dataclass(frozen=True, slots=True)
class Outcome:
    """What a run over the documents achieved."""

    read: int
    chunks: int
    edges: int
    errors: int


class MissingTextError(Exception):
    """The corpus lacks the Markdown or the Docling structure of a document."""


def _texts(root: Path, dok_laufnr: int, dokudatum: str) -> tuple[str, list[chunking.Chunk]]:
    """The body of a document and its chunks along the Docling structure."""
    text = corpus.read(root, dok_laufnr, dokudatum)
    if text is None:
        raise MissingTextError("no Markdown in the corpus")
    stored = corpus.docling(root, dok_laufnr, dokudatum)
    if stored is None:
        raise MissingTextError("no Docling structure in the corpus")
    return pages.split(text)[0], chunking.merge(chunking.split(stored))


def _subject(document: sqlite3.Row) -> tuple[str, str]:
    """The node a statement is about: the business item, otherwise the document itself."""
    if document["signatur_ges"]:
        return geschaeft_id(document["signatur_ges"]), document["titel_ges"] or ""
    return f"dok:{document['dok_laufnr']}", document["titel_dok"] or f"Dokument {document['dok_laufnr']}"


def write(conn: sqlite3.Connection, document: sqlite3.Row, reading: Sweep, body: str, *, now: str) -> int:
    """Record the quoted relations of a document."""
    quoted = reading.quoted(body)
    subject_id, subject_name = _subject(document)
    for relation in quoted:
        is_business_item = relation.object_type is EntityType.BUSINESS_ITEM
        object_id = (
            geschaeft_id(relation.object_name)
            if is_business_item
            else node_id(relation.object_type, relation.object_name)
        )
        conn.execute(
            """
            INSERT OR REPLACE INTO relations
                (subject_id, subject_name, predicate, object_id, object_type, object_name,
                 dok_laufnr, quote, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                subject_id,
                subject_name,
                relation.predicate.value,
                object_id,
                relation.object_type.value,
                relation.object_name,
                document["dok_laufnr"],
                relation.quote,
                now,
            ),
        )
    return len(quoted)


def _record(conn: sqlite3.Connection, row: tuple[object, ...]) -> None:
    """Record what happened to a chunk."""
    conn.execute(
        """
        INSERT OR REPLACE INTO extractions
            (dok_laufnr, teil, teile, status, seiten, sweep_json, error, model, extracted_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        row,
    )


def _replace(
    conn: sqlite3.Connection, dok_laufnr: int, rows: list[tuple[object, ...]], kept: Collection[int] = ()
) -> None:
    """Replace a document's reading, relations and search terms; the kept chunks keep their search terms."""
    for table in ("extractions", "relations"):
        conn.execute(f"DELETE FROM {table} WHERE dok_laufnr = ?", (dok_laufnr,))
    conn.execute(
        f"DELETE FROM expansions WHERE dok_laufnr = ? AND teil NOT IN ({', '.join('?' * len(kept))})",
        (dok_laufnr, *kept),
    )
    for row in rows:
        _record(conn, row)


def _kept(conn: sqlite3.Connection, dok_laufnr: int, teile: int) -> dict[int, tuple[tuple[object, ...], Sweep]]:
    """The chunks an earlier reading read well, if it cut the document into as many chunks."""
    rows = conn.execute(
        "SELECT dok_laufnr, teil, teile, status, seiten, sweep_json, error, model, extracted_at"
        " FROM extractions WHERE dok_laufnr = ? AND status = 'ok'",
        (dok_laufnr,),
    ).fetchall()
    if any(row["teile"] != teile for row in rows):
        return {}
    kept = {}
    for row in rows:
        try:
            kept[row["teil"]] = (tuple(row), Sweep.model_validate_json(row["sweep_json"] or ""))
        except ValueError:
            continue
    return kept


def _looped(conn: sqlite3.Connection, dok_laufnr: int, teile: int) -> set[int]:
    """The chunks whose last reading ran to the limit; greedy decoding would loop on them again."""
    rows = conn.execute(
        "SELECT teil FROM extractions WHERE dok_laufnr = ? AND teile = ? AND status = 'fehler' AND error LIKE ?",
        (dok_laufnr, teile, f"{llm.TRUNCATED}%"),
    )
    return {row[0] for row in rows}


def one(
    conn: sqlite3.Connection,
    client: OpenAI,
    root: Path,
    document: sqlite3.Row,
    *,
    model: str,
    lock: threading.Lock | None = None,
) -> tuple[int, int, int]:
    """Read a document's chunks that have no good reading yet; returns chunks read, relations and errors."""
    now = datetime.now(UTC).isoformat(timespec="seconds")
    writing = lock or threading.Lock()
    number = document["dok_laufnr"]
    try:
        body, chunks = _texts(root, number, document["dokudatum"] or "")
    except MissingTextError as missing:
        chunks, problem = [], str(missing)
    else:
        problem = "the structure holds no text"
    if not chunks:
        with writing, conn:
            _replace(conn, number, [(number, 1, 1, "fehlt", "", None, problem, model, now)])
        return 0, 0, 1

    with writing:
        kept = _kept(conn, number, len(chunks))
        looped = _looped(conn, number, len(chunks))
    rows: list[tuple[object, ...]] = []
    readings: list[Sweep] = []
    for chunk in chunks:
        if chunk.order in kept:
            row, reading = kept[chunk.order]
            rows.append(row)
            readings.append(reading)
            continue
        request = llm.Request(
            body=chunk.text,
            titel_dok=document["titel_dok"],
            titel_ges=document["titel_ges"],
            signatur=document["signatur_ges"],
            chunk=chunk.order,
            chunks=len(chunks),
            pages=chunk.pages,
        )
        try:
            reading = llm.sweep(client, request, model=model, looped=chunk.order in looped)
        except llm.SweepError as error:
            rows.append((number, chunk.order, len(chunks), "fehler", chunk.pages, None, str(error)[:500], model, now))
            continue
        readings.append(reading)
        rows.append(
            (
                number,
                chunk.order,
                len(chunks),
                "ok",
                chunk.pages,
                llm.as_json(reading),
                None,
                model,
                now,
            )
        )
    with writing, conn:
        _replace(conn, number, rows, kept)
        edges = sum(write(conn, document, reading, body, now=now) for reading in readings)
    return len(chunks) - len(kept), edges, len(chunks) - len(readings)


def prune(conn: sqlite3.Connection) -> int:
    """Delete relations older than their document's latest reading or outside the vocabulary."""
    stale = conn.execute(
        """DELETE FROM relations WHERE created_at < (
               SELECT MAX(extracted_at) FROM extractions x WHERE x.dok_laufnr = relations.dok_laufnr)"""
    ).rowcount
    invalid = 0
    for predicate, object_type in conn.execute("SELECT DISTINCT predicate, object_type FROM relations").fetchall():
        if not allowed(predicate, object_type):
            invalid += conn.execute(
                "DELETE FROM relations WHERE predicate = ? AND object_type = ?", (predicate, object_type)
            ).rowcount
    return stale + invalid


def run(
    conn: sqlite3.Connection,
    llm_client: OpenAI,
    root: Path,
    *,
    model: str,
    limit: int = 10,
    min_chars: int = MIN_CHARS,
    concurrency: int = CONCURRENCY,
) -> Outcome:
    """Read the next documents with a chunk not read well yet; a reading by an earlier model counts."""
    with conn:
        prune(conn)
    documents = conn.execute(CANDIDATES.format(priority=PRIORITY), (min_chars, limit)).fetchall()
    lock = threading.Lock()
    read = chunks = edges = errors = 0
    with ThreadPoolExecutor(max_workers=max(1, concurrency)) as pool:
        results = pool.map(lambda document: one(conn, llm_client, root, document, model=model, lock=lock), documents)
        for c, e, f in progress.track(results, label="reading documents", total=len(documents)):
            read += 1
            chunks += c
            edges += e
            errors += f
    return Outcome(read=read, chunks=chunks, edges=edges, errors=errors)
