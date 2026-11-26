"""The single database of this service: open, create, close."""

from __future__ import annotations

import logging
import os
import sqlite3
from collections.abc import Generator
from contextlib import contextmanager
from importlib import resources
from pathlib import Path

from corpus_mcp import ods

logger = logging.getLogger(__name__)

SCHEMA = "schema.sql"


def schema_sql() -> str:
    """The schema that ships next to this module."""
    return resources.files("corpus_mcp").joinpath(SCHEMA).read_text(encoding="utf-8")


def connect(path: Path, *, readonly: bool = False) -> sqlite3.Connection:
    """Open the database and create what is missing. Whoever opens it closes it."""
    if readonly:
        if not path.exists():
            raise FileNotFoundError(f"no database at {path}")
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=30.0, check_same_thread=False)
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
        # URI mode, so ATTACH can mount another database read-only.
        conn = sqlite3.connect(f"file:{path}", uri=True, timeout=30.0, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 30000")
    if not readonly:
        conn.executescript(schema_sql())
        for spec in ods.CATALOG:
            conn.execute(spec.create_sql())
            for statement in spec.index_sql():
                conn.execute(statement)
    return conn


def version(*paths: Path) -> tuple[tuple[int, int] | None, ...]:
    """What identifies the files as they are now: a re-index that writes or swaps one changes it."""
    return tuple((stat.st_mtime_ns, stat.st_size) if (stat := _stat(path)) else None for path in paths)


def _stat(path: Path) -> os.stat_result | None:
    try:
        return path.stat()
    except FileNotFoundError:
        return None


@contextmanager
def open_db(path: Path, *, readonly: bool = False) -> Generator[sqlite3.Connection]:
    """The same for a command that closes the connection afterwards."""
    conn = connect(path, readonly=readonly)
    try:
        yield conn
    finally:
        conn.close()
