"""Build the full-text index from the corpus, with the documents' metadata from the open data."""

from __future__ import annotations

import itertools
import sqlite3
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from corpus_mcp import corpus, pages, progress

BATCH = 500

SOURCES = f"""
    SELECT CAST(dok_laufnr AS INTEGER) AS dok_laufnr, signatur_dok, signatur_ges, titel_dok,
           substr(dokudatum, 1, 10) AS dokudatum,
           CASE WHEN url_dok LIKE 'http%' THEN url_dok ELSE '' END AS pdf_url
    FROM dokumente
    WHERE dok_laufnr != ''
    UNION ALL
    SELECT {corpus.TRANSCRIPT_ID_BASE} + CAST(tagesordnung_idnr AS INTEGER), '', '', 'Vollprotokoll',
           substr(MIN(tag1), 1, 10), MIN(url_vollprotokoll)
    FROM tagesordnungen
    WHERE url_vollprotokoll LIKE 'http%'
    GROUP BY tagesordnung_idnr
"""


@dataclass(frozen=True, slots=True)
class Source:
    """A document the open data lists, with what the index keeps about it."""

    dok_laufnr: int
    signatur_dok: str
    signatur_ges: str
    titel_dok: str
    dokudatum: str
    pdf_url: str


def _authors(conn: sqlite3.Connection) -> dict[str, tuple[str, str]]:
    """For each business item, who submitted it and for which party."""
    return {
        row[0]: (row[1] or "", row[2] or "")
        for row in conn.execute("SELECT signatur_ges, urheber, partei_urheber FROM geschaefte")
    }


def sources(conn: sqlite3.Connection) -> list[Source]:
    """The documents the open data lists."""
    return [
        Source(row[0], row[1] or "", row[2] or "", row[3] or "", row[4] or "", row[5] or "")
        for row in conn.execute(SOURCES)
    ]


def build(conn: sqlite3.Connection, root: Path, *, context: Mapping[int, str] | None = None) -> int:
    """Index every listed document that has Markdown, with its context in a column of its own."""
    authors = _authors(conn)
    kontext = context or {}
    now = datetime.now(UTC).isoformat(timespec="seconds")
    written = 0
    listed = sources(conn)
    found = (
        (source, *pages.split(text), text)
        for source in progress.track(listed, label="indexing", total=len(listed))
        if (text := corpus.read(root, source.dok_laufnr, source.dokudatum)) is not None
    )
    for batch in itertools.batched(found, BATCH):
        with conn:
            conn.executemany(
                "INSERT INTO documents_fts (rowid, titel, urheber, partei, body, kontext) VALUES (?, ?, ?, ?, ?, ?)",
                [
                    (
                        s.dok_laufnr,
                        s.titel_dok,
                        *authors.get(s.signatur_ges, ("", "")),
                        body,
                        kontext.get(s.dok_laufnr, ""),
                    )
                    for s, body, _anchors, _text in batch
                ],
            )
            conn.executemany(
                "INSERT INTO document_text (dok_laufnr, markdown) VALUES (?, ?)",
                [(s.dok_laufnr, corpus.pack(text)) for s, _body, _anchors, text in batch],
            )
            conn.executemany(
                """
                INSERT INTO documents
                    (dok_laufnr, signatur_dok, signatur_ges, titel_dok, dokudatum, pdf_url, chars, pages, indexed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        s.dok_laufnr,
                        s.signatur_dok,
                        s.signatur_ges,
                        s.titel_dok,
                        s.dokudatum,
                        s.pdf_url or corpus.pdf_url(s.dok_laufnr),
                        len(body),
                        len(anchors),
                        now,
                    )
                    for s, body, anchors, _text in batch
                ],
            )
        written += len(batch)
    return written
