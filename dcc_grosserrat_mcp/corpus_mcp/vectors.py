"""The passage vectors in sqlite-vec, in a database of their own beside the index.

A document is embedded again when its text, the model or `passages.VERSION` changes. Cutting
runs in worker processes, embedding in batches across documents in the main process.
"""

from __future__ import annotations

import multiprocessing
import os
import sqlite3
import zlib
from collections.abc import Iterator
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import sqlite_vec

from corpus_mcp import corpus, embeddings, passages, progress
from corpus_mcp.embeddings import OnnxEmbedder

# Passages per call to the embedder.
BATCH = 1024
# Worker processes that cut documents.
CUTTERS = max(1, (os.cpu_count() or 4) // 2)
# Fewer documents are cut in process.
IN_PROCESS_UNDER = 64

SCHEMA = f"""
CREATE TABLE IF NOT EXISTS embedded (
    dok_laufnr  INTEGER PRIMARY KEY,
    chars       INTEGER NOT NULL,
    method      TEXT NOT NULL,
    passages    INTEGER NOT NULL,
    embedded_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS passages (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    dok_laufnr INTEGER NOT NULL,
    page       INTEGER,
    headings   TEXT NOT NULL,
    text       BLOB NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_passages_document ON passages(dok_laufnr);
CREATE VIRTUAL TABLE IF NOT EXISTS passage_vectors USING vec0(
    id INTEGER PRIMARY KEY,
    embedding float[{embeddings.DIMENSIONS}] distance_metric=cosine
);
"""


@dataclass(frozen=True, slots=True)
class Pending:
    """A document whose vectors are missing or out of date."""

    dok_laufnr: int
    dokudatum: str
    chars: int
    title: str


def method(model: str) -> str:
    """What a vector was made with: the embedding model and the cut."""
    return f"{model} · {passages.VERSION}"


def connect(path: Path) -> sqlite3.Connection:
    """Open the vector store with sqlite-vec and create what is missing. A store of another width is an error."""
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.enable_load_extension(True)
    sqlite_vec.load(conn)
    conn.enable_load_extension(False)
    conn.executescript(SCHEMA)
    _check_width(conn, path)
    return conn


def _check_width(conn: sqlite3.Connection, path: Path) -> None:
    """Stop if the store holds vectors of a different width than this model makes."""
    row = conn.execute("SELECT sql FROM sqlite_master WHERE name = 'passage_vectors'").fetchone()
    if row and f"float[{embeddings.DIMENSIONS}]" not in row[0]:
        raise RuntimeError(
            f"{path} holds vectors of another width than {embeddings.DIMENSIONS}; "
            "it was built with another embedding model. Move it aside and embed again."
        )


def pending(vectors: sqlite3.Connection, index: sqlite3.Connection, model: str) -> list[Pending]:
    """The indexed documents whose vectors are missing, or were made from other text, model or cut."""
    made_with = method(model)
    done = {row[0]: (row[1], row[2]) for row in vectors.execute("SELECT dok_laufnr, chars, method FROM embedded")}
    rows = index.execute(
        """
        SELECT d.dok_laufnr, d.dokudatum, d.chars, d.titel_dok, COALESCE(g.titel_ges, '')
        FROM documents d LEFT JOIN geschaefte g ON g.signatur_ges = d.signatur_ges
        ORDER BY d.dokudatum DESC, d.dok_laufnr DESC
        """
    )
    return [
        Pending(number, date or "", chars, _title(titel_ges, titel_dok, date or ""))
        for number, date, chars, titel_dok, titel_ges in rows
        if done.get(number) != (chars, made_with)
    ]


def build(
    vectors: sqlite3.Connection,
    index: sqlite3.Connection,
    root: Path,
    embedder: OnnxEmbedder,
    *,
    limit: int | None = None,
) -> dict[str, int]:
    """Cut and embed every document that needs it, newest first; one that yields no passage is skipped."""
    todo = pending(vectors, index, embedder.model)
    todo = todo[:limit] if limit else todo
    written = {"documents": 0, "passages": 0, "skipped": 0}
    made_with = method(embedder.model)
    waiting: list[tuple[Pending, list[passages.Passage]]] = []
    depth = 0
    for item, cut in progress.track(_cuts(root, todo), label="embedding", total=len(todo)):
        if not cut:
            # Recorded with no passages, so the next run cuts it again only when its text changes.
            _store(vectors, item, made_with, [], [])
            written["skipped"] += 1
            continue
        waiting.append((item, cut))
        depth += len(cut)
        if depth >= BATCH:
            _flush(vectors, embedder, made_with, waiting, written)
            waiting, depth = [], 0
    _flush(vectors, embedder, made_with, waiting, written)
    return written


def _cuts(root: Path, todo: list[Pending]) -> Iterator[tuple[Pending, list[passages.Passage]]]:
    """Each document with its passages, cut in worker processes once there are enough to pay for them."""
    if len(todo) < IN_PROCESS_UNDER:
        yield from ((item, _cut((root, item))) for item in todo)
        return
    # Spawn, so a worker starts clean.
    with ProcessPoolExecutor(max_workers=CUTTERS, mp_context=multiprocessing.get_context("spawn")) as pool:
        yield from zip(todo, pool.map(_cut, ((root, item) for item in todo), chunksize=8), strict=True)


def _flush(
    vectors: sqlite3.Connection,
    embedder: OnnxEmbedder,
    made_with: str,
    waiting: list[tuple[Pending, list[passages.Passage]]],
    written: dict[str, int],
) -> None:
    """Embed the passages gathered so far in one go and write each document that is complete."""
    if not waiting:
        return
    texts = [passage.for_embedding(item.title) for item, cut in waiting for passage in cut]
    packed = embedder.passages(texts)
    at = 0
    for item, cut in waiting:
        _store(vectors, item, made_with, cut, packed[at : at + len(cut)])
        at += len(cut)
        written["documents"] += 1
        written["passages"] += len(cut)


def stats(vectors: sqlite3.Connection) -> dict[str, int]:
    """How many documents and passages have vectors; a document without text has none."""
    documents, count = vectors.execute(
        "SELECT COUNT(*), COALESCE(SUM(passages), 0) FROM embedded WHERE passages > 0"
    ).fetchone()
    return {"documents": documents, "passages": count}


def _cut(work: tuple[Path, Pending]) -> list[passages.Passage]:
    """Cut a document with Docling; runs in a worker process, so it takes one picklable argument."""
    root, item = work
    document = corpus.docling(root, item.dok_laufnr, item.dokudatum)
    return passages.cut(document) if document else []


def _store(
    vectors: sqlite3.Connection, item: Pending, made_with: str, cut: list[passages.Passage], packed: list[bytes]
) -> None:
    """Replace a document's passages and vectors in one transaction."""
    with vectors:
        old = [row[0] for row in vectors.execute("SELECT id FROM passages WHERE dok_laufnr = ?", (item.dok_laufnr,))]
        vectors.executemany("DELETE FROM passage_vectors WHERE id = ?", ((i,) for i in old))
        vectors.execute("DELETE FROM passages WHERE dok_laufnr = ?", (item.dok_laufnr,))
        for passage, vector in zip(cut, packed, strict=True):
            cursor = vectors.execute(
                "INSERT INTO passages (dok_laufnr, page, headings, text) VALUES (?, ?, ?, ?)",
                (item.dok_laufnr, passage.page, passage.headings, zlib.compress(passage.text.encode("utf-8"))),
            )
            vectors.execute("INSERT INTO passage_vectors (id, embedding) VALUES (?, ?)", (cursor.lastrowid, vector))
        vectors.execute(
            "INSERT OR REPLACE INTO embedded (dok_laufnr, chars, method, passages, embedded_at) VALUES (?, ?, ?, ?, ?)",
            (item.dok_laufnr, item.chars, made_with, len(cut), datetime.now(UTC).isoformat(timespec="seconds")),
        )


def _title(titel_ges: str, titel_dok: str, dokudatum: str) -> str:
    """What stands above every passage of a document when it is embedded."""
    return " · ".join(part for part in (titel_ges, titel_dok, dokudatum) if part)
