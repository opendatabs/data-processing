"""The documents a step could not handle, listed beside the database for whoever looks after the job."""

from __future__ import annotations

import csv
import logging
from collections import Counter
from contextlib import closing

from corpus_mcp.db import connect
from corpus_mcp.settings import Settings

logger = logging.getLogger(__name__)

COLUMNS = ["dok_laufnr", "dokudatum", "titel_dok", "step", "chunk", "reason"]

NO_MARKDOWN = """
    SELECT dok_laufnr, dokudatum, titel_dok, 'markdown', '',
           'no Markdown, not converted yet or refused by Docling: not searchable'
    FROM dokumente
    WHERE CAST(dok_laufnr AS INTEGER) NOT IN (SELECT dok_laufnr FROM documents)
    ORDER BY dokudatum DESC, dok_laufnr DESC
"""

NO_PASSAGES = """
    SELECT d.dok_laufnr, d.dokudatum, d.titel_dok, 'passages', '', 'no passage in the Docling JSON: no semantic search'
    FROM vectors.embedded e JOIN documents d ON d.dok_laufnr = e.dok_laufnr
    WHERE e.passages = 0
    ORDER BY d.dokudatum DESC, d.dok_laufnr DESC
"""

READINGS = """
    SELECT x.dok_laufnr, k.dokudatum, k.titel_dok, 'reading', x.teil || '/' || x.teile, x.error
    FROM extract.extractions x LEFT JOIN dokumente k ON CAST(k.dok_laufnr AS INTEGER) = x.dok_laufnr
    WHERE x.status != 'ok'
    ORDER BY k.dokudatum DESC, x.dok_laufnr DESC, x.teil
"""

SEARCH_WORDS = """
    SELECT x.dok_laufnr, k.dokudatum, k.titel_dok, 'search words', x.teil, x.error
    FROM extract.expansions x LEFT JOIN dokumente k ON CAST(k.dok_laufnr AS INTEGER) = x.dok_laufnr
    WHERE x.status != 'ok'
    ORDER BY k.dokudatum DESC, x.dok_laufnr DESC, x.teil
"""


def write(settings: Settings) -> dict[str, int]:
    """Replace failed.csv with what fails now: one row per document, or per chunk Gemma read."""
    with closing(connect(settings.db_path, readonly=True)) as conn:
        rows = conn.execute(NO_MARKDOWN).fetchall()
        if settings.vectors_path.exists():
            conn.execute("ATTACH DATABASE ? AS vectors", (f"file:{settings.vectors_path}?mode=ro",))
            rows += conn.execute(NO_PASSAGES).fetchall()
        if settings.extract_db.exists():
            conn.execute("ATTACH DATABASE ? AS extract", (f"file:{settings.extract_db}?mode=ro",))
            tables = {row[0] for row in conn.execute("SELECT name FROM extract.sqlite_master WHERE type = 'table'")}
            for table, query in (("extractions", READINGS), ("expansions", SEARCH_WORDS)):
                if table in tables:
                    rows += conn.execute(query).fetchall()
    partial = settings.failed_path.with_name(settings.failed_path.name + ".new")
    with partial.open("w", newline="", encoding="utf-8-sig") as file:
        out = csv.writer(file, delimiter=";")
        out.writerow(COLUMNS)
        out.writerows(tuple(row) for row in rows)
    partial.replace(settings.failed_path)
    counts = dict(Counter(row[3] for row in rows))
    logger.info("failed: %s, listed in %s", counts, settings.failed_path)
    return counts
