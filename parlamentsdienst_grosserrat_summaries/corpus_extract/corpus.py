"""The corpus on disk: Markdown and Docling JSON per document, transcripts named by their session date."""

from __future__ import annotations

import gzip
import json
from pathlib import Path
from typing import Any

METHOD = "docling-serve"
TRANSCRIPT_ID_BASE = 900_000_000


def _path(root: Path, kind: str, suffix: str, dok_laufnr: int, dokudatum: str) -> Path:
    """Where a document's Markdown or Docling JSON lives."""
    collection, name = (
        ("gr_vollprotokoll", dokudatum[:10]) if dok_laufnr >= TRANSCRIPT_ID_BASE else ("gr_dokumente", str(dok_laufnr))
    )
    folder = f"{collection}_md_{METHOD}" if kind == "markdown" else f"{collection}_{METHOD}"
    return root / kind / folder / f"{name}_{METHOD}{suffix}"


def read(root: Path, dok_laufnr: int, dokudatum: str = "") -> str | None:
    """A document's Markdown, or nothing if the corpus lacks it."""
    try:
        return _path(root, "markdown", ".md", dok_laufnr, dokudatum).read_text(encoding="utf-8")
    except OSError:
        return None


def docling(root: Path, dok_laufnr: int, dokudatum: str = "") -> dict[str, Any] | None:
    """A document's stored DoclingDocument, or nothing if the corpus lacks it."""
    path = _path(root, "docling", ".json.gz", dok_laufnr, dokudatum)
    return json.loads(gzip.decompress(path.read_bytes())) if path.exists() else None
