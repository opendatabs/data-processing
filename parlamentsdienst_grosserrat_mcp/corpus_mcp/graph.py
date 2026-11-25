"""Build the graph from the mirrored data."""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime

from corpus_mcp.rules import EDGE_RULES, NODE_RULES, EdgeRule, NodeRule

NODES = """
    INSERT INTO nodes (id, type, name, url, attrs_json, updated_at)
    SELECT id, type, name, url, attrs_json, ? FROM ({sql}) WHERE id != '' AND name IS NOT NULL
    ON CONFLICT(id) DO UPDATE SET
        type = excluded.type, name = excluded.name,
        -- A mention must not overwrite the master record: whatever already has a URL keeps
        -- it. A master record (it has a URL) replaces its earlier attributes, so a member's
        -- new term replaces the last; among mentions the richer set of attributes wins.
        url = COALESCE(nodes.url, excluded.url),
        attrs_json = CASE WHEN excluded.url IS NOT NULL
                            OR length(COALESCE(excluded.attrs_json, '')) > length(COALESCE(nodes.attrs_json, ''))
                          THEN excluded.attrs_json ELSE nodes.attrs_json END,
        updated_at = excluded.updated_at
"""

EDGES = """
    INSERT OR REPLACE INTO triples
        (subject_id, predicate, object_id, source, qualifier, dok_laufnr,
         valid_from, valid_to, quote, weight, created_at)
    SELECT subject_id, ?, object_id, ?, COALESCE(qualifier, ''), 0,
           COALESCE(valid_from, ''), valid_to, NULL, weight, ?
    FROM ({sql})
    WHERE subject_id != '' AND object_id != ''
      AND subject_id IN (SELECT id FROM nodes) AND object_id IN (SELECT id FROM nodes)
"""


def _node_rule(conn: sqlite3.Connection, rule: NodeRule, now: str) -> int:
    """Run a node rule and return how many rows it wrote."""
    return conn.execute(NODES.format(sql=rule.sql), (now,)).rowcount


def _edge_rule(conn: sqlite3.Connection, rule: EdgeRule, now: str) -> int:
    """Run an edge rule; edges pointing nowhere are dropped."""
    return conn.execute(EDGES.format(sql=rule.sql), (rule.predicate, rule.source, now)).rowcount


def index_nodes(conn: sqlite3.Connection) -> int:
    """Build the name index, including the edge count per entry; once every edge is in."""
    with conn:
        conn.execute(
            """
            INSERT INTO nodes_fts (name, type, node_id, degree)
            SELECT n.name, n.type, n.id,
                   (SELECT COUNT(*) FROM triples t WHERE t.subject_id = n.id OR t.object_id = n.id)
            FROM nodes n
            """
        )
    return conn.execute("SELECT COUNT(*) FROM nodes_fts").fetchone()[0]


def build(conn: sqlite3.Connection) -> dict[str, int]:
    """Build nodes and edges from the open data."""
    now = datetime.now(UTC).isoformat(timespec="seconds")
    with conn:
        written = sum(_node_rule(conn, rule, now) for rule in NODE_RULES)
        edges = sum(_edge_rule(conn, rule, now) for rule in EDGE_RULES)
    return {"nodes": conn.execute("SELECT COUNT(*) FROM nodes").fetchone()[0], "written": written, "edges": edges}
