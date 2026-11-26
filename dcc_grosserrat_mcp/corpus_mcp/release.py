"""The release marker: written when the database or the vectors changed, for whoever copies the folder.

It lists each file with its size and modification time. rsync sends the files one at a time, so the
MCP server takes a release only once every file has arrived as listed.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime

from corpus_mcp.settings import Settings

logger = logging.getLogger(__name__)


def write(settings: Settings, report: dict[str, object]) -> None:
    """Mark the database and vectors as one finished release, so a reader never sees half of one."""
    files = {
        path.name: {"size": stat.st_size, "mtime": int(stat.st_mtime)}
        for path in (settings.db_path, settings.vectors_path, settings.index_path)
        if path.exists() and (stat := path.stat())
    }
    marker = {"released_at": datetime.now(UTC).isoformat(timespec="seconds"), **report, "files": files}
    partial = settings.release_path.with_name(settings.release_path.name + ".new")
    partial.write_text(json.dumps(marker, ensure_ascii=False, indent=2), encoding="utf-8")
    partial.replace(settings.release_path)
    logger.info("released %s", settings.release_path)
