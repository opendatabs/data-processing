"""Take the quoted relations of the extract job over into the graph as `llm` edges."""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from pathlib import Path

SOURCE = "llm"
ALIAS = "extract"

OBJECT_NODES = f"""
    INSERT INTO nodes (id, type, name, url, attrs_json, updated_at)
    SELECT object_id, MIN(object_type), MIN(object_name), NULL, json_object('quelle', 'llm'), ?
    FROM {ALIAS}.relations
    GROUP BY object_id
    ON CONFLICT(id) DO NOTHING
"""

DOCUMENT_NODES = f"""
    INSERT INTO nodes (id, type, name, url, attrs_json, updated_at)
    SELECT r.subject_id, 'dokument',
           COALESCE(
               NULLIF(TRIM(COALESCE(d.titel_dok, '') || ' ' || COALESCE(d.dokudatum, '')), ''),
               NULLIF(MIN(r.subject_name), ''),
               'Dokument ' || MIN(r.dok_laufnr)
           ),
           d.pdf_url, json_object('quelle', 'korpus'), ?
    FROM {ALIAS}.relations r
    -- On the number, so the join uses the primary key.
    LEFT JOIN documents d ON d.dok_laufnr = CAST(substr(r.subject_id, 5) AS INTEGER)
    WHERE r.subject_id LIKE 'dok:%'
    GROUP BY r.subject_id
    ON CONFLICT(id) DO UPDATE SET name = excluded.name, url = excluded.url
"""

EDGES = f"""
    INSERT OR REPLACE INTO triples
        (subject_id, predicate, object_id, source, qualifier, dok_laufnr,
         valid_from, valid_to, quote, weight, created_at)
    SELECT subject_id, predicate, object_id, '{SOURCE}', '', dok_laufnr, '', NULL, quote, NULL, created_at
    FROM {ALIAS}.relations
    WHERE subject_id IN (SELECT id FROM nodes) AND object_id IN (SELECT id FROM nodes)
"""


def load(conn: sqlite3.Connection, extract_db: Path) -> dict[str, int]:
    """Write the relations in the extract database as `llm` edges, with the nodes they need."""
    if not extract_db.exists():
        return {"nodes": 0, "edges": 0}
    now = datetime.now(UTC).isoformat(timespec="seconds")
    conn.execute(f"ATTACH DATABASE 'file:{extract_db}?mode=ro' AS {ALIAS}")
    try:
        with conn:
            nodes = conn.execute(OBJECT_NODES, (now,)).rowcount
            nodes += conn.execute(DOCUMENT_NODES, (now,)).rowcount
            edges = conn.execute(EDGES).rowcount
    finally:
        conn.execute(f"DETACH DATABASE {ALIAS}")
    return {"nodes": nodes, "edges": edges}
