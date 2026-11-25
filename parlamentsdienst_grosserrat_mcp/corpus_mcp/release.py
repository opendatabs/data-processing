"""The release marker: written when the database or the vectors changed, for whoever copies the folder."""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime

from corpus_mcp.settings import Settings

logger = logging.getLogger(__name__)


def write(settings: Settings, report: dict[str, object]) -> None:
    """Mark the database and vectors as one finished release, so a reader never sees half of one."""
    marker = {"released_at": datetime.now(UTC).isoformat(timespec="seconds"), **report}
    partial = settings.release_path.with_name(settings.release_path.name + ".new")
    partial.write_text(json.dumps(marker, ensure_ascii=False, indent=2), encoding="utf-8")
    partial.replace(settings.release_path)
    logger.info("released %s", settings.release_path)
