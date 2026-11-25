"""Take the topic job's `topics.csv` over: as a table, and as `classifier` edges in the graph."""

from __future__ import annotations

import csv
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

SOURCE = "classifier"

ASSIGNMENTS = """
    INSERT OR REPLACE INTO topic_assignments
        (signatur_ges, topic, confidence, topic_secondary, confidence_secondary, source, model)
    VALUES (?, ?, ?, ?, ?, ?, ?)
"""

TOPIC_NODES = """
    INSERT INTO nodes (id, type, name, url, attrs_json, updated_at)
    SELECT DISTINCT 'thema:' || lower(replace(topic, ' ', '_')), 'thema', topic, NULL, NULL, ?
    FROM topic_assignments WHERE source = ?
    ON CONFLICT(id) DO NOTHING
"""

EDGES = """
    INSERT OR REPLACE INTO triples
        (subject_id, predicate, object_id, source, qualifier, dok_laufnr,
         valid_from, valid_to, quote, weight, created_at)
    SELECT 'ges:' || signatur_ges, 'HAS_TOPIC', 'thema:' || lower(replace(topic, ' ', '_')), ?, model, 0,
           '', NULL, NULL, confidence, ?
    FROM topic_assignments
    WHERE source = ? AND 'ges:' || signatur_ges IN (SELECT id FROM nodes)
"""


def _rows(topics: Path) -> list[tuple]:
    """The topic job's rows: `topic_1`/`topic_2` become the topic and its runner-up."""
    with topics.open(encoding="utf-8-sig", newline="") as file:
        return [
            (
                row["signatur_ges"],
                row["topic_1"],
                float(row["confidence_1"]) if row["confidence_1"] else None,
                row["topic_2"] or None,
                float(row["confidence_2"]) if row["confidence_2"] else None,
                row["source"],
                row["model"],
            )
            for row in csv.DictReader(file, delimiter=";")
            if row["signatur_ges"] and row["topic_1"]
        ]


def load(conn: sqlite3.Connection, topics: Path) -> dict[str, int]:
    """Fill the topic table and write a `classifier` edge per assignment, from topics.csv."""
    if not topics.exists():
        return {"assignments": 0, "edges": 0}
    now = datetime.now(UTC).isoformat(timespec="seconds")
    rows = _rows(topics)
    with conn:
        conn.executemany(ASSIGNMENTS, rows)
        conn.execute(TOPIC_NODES, (now, SOURCE))
        edges = conn.execute(EDGES, (SOURCE, now, SOURCE)).rowcount
    return {"assignments": len(rows), "edges": edges}
