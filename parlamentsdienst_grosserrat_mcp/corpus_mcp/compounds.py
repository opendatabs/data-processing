"""The index's vocabulary, so the server can look up German compounds that end in a query word."""

from __future__ import annotations

import sqlite3


def build(conn: sqlite3.Connection) -> int:
    """Write the index's vocabulary into `index_terms` and return how many terms it holds."""
    conn.execute("CREATE VIRTUAL TABLE IF NOT EXISTS temp.index_vocab USING fts5vocab(main, 'documents_fts', 'row')")
    with conn:
        conn.executemany(
            "INSERT INTO index_terms (term, reversed, docs) VALUES (?, ?, ?)",
            ((term, term[::-1], docs) for term, docs in conn.execute("SELECT term, doc FROM temp.index_vocab")),
        )
    conn.execute("DROP TABLE temp.index_vocab")
    return conn.execute("SELECT COUNT(*) FROM index_terms").fetchone()[0]
