"""One run: Gemma reads the next documents, then writes the search terms for what it read."""

import json
import logging
import urllib.request
from dataclasses import asdict
from urllib.parse import urlsplit

from corpus_extract import expansion, llm, sweep
from corpus_extract.db import attach_mcp, open_db
from corpus_extract.settings import Settings, get_settings

OPEN_CHUNKS = """
    SELECT COUNT(*) FROM extractions e
    LEFT JOIN expansions x ON x.dok_laufnr = e.dok_laufnr AND x.teil = e.teil AND x.status IN ('ok', 'fehlt')
    WHERE e.status = 'ok' AND e.sweep_json IS NOT NULL AND x.dok_laufnr IS NULL
"""


def main(settings: Settings | None = None) -> None:
    """Read up to `EXTRACT_LIMIT` documents not read well yet, then expand every chunk without search terms."""
    settings = settings or get_settings()
    _check_files(settings)
    gemma = llm.client(settings.llm_url, api_key=settings.llm_api_key)
    logging.info(
        "asking %s for %s, key: %s, proxy: %s",
        settings.llm_url,
        settings.llm_model,
        f"{len(settings.llm_api_key)} characters" if settings.llm_api_key else "none",
        _proxy(settings.llm_url),
    )
    llm.check(gemma, settings.llm_model)
    logging.info("Gemma answers")
    with open_db(settings.db_path) as conn:
        attach_mcp(conn, settings.mcp_db)
        read = sweep.run(
            conn,
            gemma,
            settings.corpus_dir,
            model=settings.llm_model,
            limit=settings.limit,
            concurrency=settings.concurrency,
        )
        logging.info("read: %s", json.dumps(asdict(read)))
        open_chunks = conn.execute(OPEN_CHUNKS).fetchone()[0]
        logging.info("%d chunks to expand", open_chunks)
        expanded = expansion.run(
            conn, gemma, model=settings.llm_model, limit=open_chunks, concurrency=settings.concurrency
        )
        logging.info("expanded: %s", json.dumps(asdict(expanded)))


def _proxy(url: str) -> str:
    """The proxy a request to this URL goes through, without credentials; 'none' if it goes direct."""
    host = urlsplit(url).hostname or ""
    proxy = urllib.request.getproxies().get(urlsplit(url).scheme)
    if not proxy or urllib.request.proxy_bypass(host):
        return "none"
    parts = urlsplit(proxy)
    return f"{parts.hostname}:{parts.port}" if parts.port else str(parts.hostname)


def _check_files(settings: Settings) -> None:
    """Log whether the readings so far and the MCP database are there."""
    if settings.db_path.exists():
        logging.info("%s: %.0f MB", settings.db_path, settings.db_path.stat().st_size / 1e6)
    else:
        logging.warning("%s not found: every document is read from scratch; copy the seed first", settings.db_path)
    if not settings.mcp_db.exists():
        folder = settings.mcp_db.parent
        found = sorted(entry.name for entry in folder.iterdir()) if folder.is_dir() else "no such folder"
        raise RuntimeError(f"no MCP database at {settings.mcp_db} (the folder holds: {found})")
    logging.info("%s: %.0f MB", settings.mcp_db, settings.mcp_db.stat().st_size / 1e6)
