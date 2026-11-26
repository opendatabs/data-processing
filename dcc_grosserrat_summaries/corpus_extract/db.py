"""The extract job's own database, and the MCP database it reads from."""

from __future__ import annotations

import logging
import sqlite3
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path

logger = logging.getLogger(__name__)

SCHEMA = """
PRAGMA journal_mode = DELETE;

CREATE TABLE IF NOT EXISTS extractions (
    dok_laufnr   INTEGER NOT NULL,
    teil         INTEGER NOT NULL DEFAULT 1,
    teile        INTEGER NOT NULL DEFAULT 1,
    status       TEXT NOT NULL,
    seiten       TEXT NOT NULL DEFAULT '',
    sweep_json   TEXT,
    error        TEXT,
    model        TEXT NOT NULL DEFAULT '',
    extracted_at TEXT NOT NULL,
    PRIMARY KEY (dok_laufnr, teil)
);

CREATE INDEX IF NOT EXISTS idx_extractions_status ON extractions(status);

-- What a searcher would call this document: questions in everyday German and the words
-- under which someone would look for it. Written by `expansion`, indexed by the MCP job,
-- never quoted.
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

CREATE TABLE IF NOT EXISTS relations (
    subject_id   TEXT NOT NULL,
    subject_name TEXT NOT NULL DEFAULT '',
    predicate    TEXT NOT NULL,
    object_id    TEXT NOT NULL,
    object_type  TEXT NOT NULL,
    object_name  TEXT NOT NULL,
    dok_laufnr   INTEGER NOT NULL,
    quote        TEXT NOT NULL,
    created_at   TEXT NOT NULL,
    PRIMARY KEY (subject_id, predicate, object_id, dok_laufnr)
);
"""


@contextmanager
def open_db(path: Path) -> Generator[sqlite3.Connection]:
    """Open the extract database, create what is missing, close it afterwards."""
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(f"file:{path}", uri=True, timeout=30.0, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        conn.executescript(SCHEMA)
        yield conn
    finally:
        conn.close()


def attach_mcp(conn: sqlite3.Connection, mcp_db: Path) -> None:
    """Attach the MCP database as `mcp`, as an immutable snapshot of a read-only mount."""
    if not mcp_db.exists():
        raise FileNotFoundError(f"no MCP database at {mcp_db}")
    conn.execute("ATTACH DATABASE ? AS mcp", (f"file:{mcp_db}?immutable=1",))
