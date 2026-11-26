"""The extract job's text per chunk: indexed as extra vocabulary for a document, never quoted."""

from __future__ import annotations

import json
import logging
import sqlite3
from collections.abc import Iterable, Sequence
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)

# Sweep fields that are indexed; the relations become edges instead.
FIELDS = ("summary", "core_demand", "measures", "questions", "arguments")

# Maximum indexed context per document.
MAX_CHARS = 8_000


@dataclass(frozen=True, slots=True)
class Chunk:
    """What the model wrote about one chunk of a document, and where that chunk is."""

    dok_laufnr: int
    teil: int
    seiten: str
    text: str


def read(path: Path) -> list[Chunk]:
    """The extract job's text per chunk in document order; empty if the database is missing."""
    if not path.exists():
        logger.info("no extract database at %s; the index is built without context", path)
        return []
    try:
        with closing(sqlite3.connect(f"file:{path}?mode=ro", uri=True)) as conn:
            conn.row_factory = sqlite3.Row
            expansions = _expansions(conn)
            return _chunks(conn, expansions)
    except sqlite3.Error as error:
        logger.warning("extract database unreadable (%s); the index is built without context", error)
        return []


def _expansions(conn: sqlite3.Connection) -> dict[tuple[int, int], list[str]]:
    """The search terms per chunk; empty if the read task has not written any yet."""
    try:
        rows = conn.execute(
            "SELECT dok_laufnr, teil, fragen_json FROM expansions WHERE status = 'ok' AND fragen_json IS NOT NULL"
        ).fetchall()
    except sqlite3.Error:
        logger.info("no expansions in the extract database; the context is the summaries alone")
        return {}

    found: dict[tuple[int, int], list[str]] = {}
    for row in rows:
        try:
            expansion = json.loads(row["fragen_json"])
        except ValueError:
            continue
        if not isinstance(expansion, dict):
            continue
        parts = [
            item.strip()
            for field in ("fragen", "begriffe")
            for item in (expansion.get(field) or [])
            if isinstance(item, str) and item.strip()
        ]
        if parts:
            found[(int(row["dok_laufnr"]), int(row["teil"]))] = parts
    logger.info("expansions for %d chunks", len(found))
    return found


def _chunks(conn: sqlite3.Connection, expansions: dict[tuple[int, int], list[str]]) -> list[Chunk]:
    """One row per chunk: the searcher's words first, the summary behind them."""
    chunks: list[Chunk] = []
    rows = conn.execute(
        "SELECT dok_laufnr, teil, seiten, sweep_json FROM extractions"
        " WHERE status = 'ok' AND sweep_json IS NOT NULL ORDER BY dok_laufnr, teil"
    )
    for row in rows:
        try:
            sweep = json.loads(row["sweep_json"])
        except ValueError:
            continue
        if not isinstance(sweep, dict):
            continue
        key = (int(row["dok_laufnr"]), int(row["teil"]))
        parts = [*expansions.get(key, []), *_texts(sweep)]
        if parts:
            chunks.append(Chunk(dok_laufnr=key[0], teil=key[1], seiten=row["seiten"] or "", text="\n".join(parts)))
    return chunks


def _texts(sweep: dict[str, object]) -> Iterable[str]:
    """The readable parts of one sweep, in the order they were asked for."""
    for field in FIELDS:
        value = sweep.get(field)
        if isinstance(value, str) and value.strip():
            yield value.strip()
        elif isinstance(value, list):
            yield from (item.strip() for item in value if isinstance(item, str) and item.strip())


def per_document(chunks: Sequence[Chunk], *, max_chars: int = MAX_CHARS) -> dict[int, str]:
    """One context text per document for the index: each line once, within `max_chars`."""
    collected: dict[int, list[str]] = {}
    for chunk in chunks:
        collected.setdefault(chunk.dok_laufnr, []).extend(chunk.text.split("\n"))
    return {number: _join(parts, max_chars) for number, parts in collected.items() if parts}


def _join(parts: list[str], max_chars: int) -> str:
    """The parts in order, each line once, within the cap."""
    seen: set[str] = set()
    kept: list[str] = []
    length = 0
    for part in parts:
        key = " ".join(part.split()).casefold()
        if not key or key in seen:
            continue
        seen.add(key)
        if length + len(part) + 1 > max_chars:
            break
        kept.append(part)
        length += len(part) + 1
    return "\n".join(kept)


def store(conn: sqlite3.Connection, chunks: Sequence[Chunk]) -> int:
    """Keep the chunks beside the index, so a hit can be placed in its document."""
    with conn:
        conn.executemany(
            "INSERT INTO document_context (dok_laufnr, teil, seiten, text) VALUES (?, ?, ?, ?)",
            [(c.dok_laufnr, c.teil, c.seiten, c.text) for c in chunks],
        )
    return len(chunks)
