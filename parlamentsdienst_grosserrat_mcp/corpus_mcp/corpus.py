"""The corpus on disk: Markdown and Docling JSON per document, transcripts named by their session date."""

from __future__ import annotations

import gzip
import json
import zlib
from pathlib import Path
from typing import Any

METHOD = "docling-serve"
TRANSCRIPT_ID_BASE = 900_000_000
BUCKET = 1000
OFFSET = 100_000


def _collection_and_name(dok_laufnr: int, dokudatum: str) -> tuple[str, str]:
    """The collection a document belongs to and the name its files carry."""
    if dok_laufnr >= TRANSCRIPT_ID_BASE:
        return "gr_vollprotokoll", dokudatum[:10]
    return "gr_dokumente", str(dok_laufnr)


def markdown_path(root: Path, dok_laufnr: int, dokudatum: str = "") -> Path:
    """Where the Markdown of a document lives."""
    collection, name = _collection_and_name(dok_laufnr, dokudatum)
    return root / "markdown" / f"{collection}_md_{METHOD}" / f"{name}_{METHOD}.md"


def docling_path(root: Path, dok_laufnr: int, dokudatum: str = "") -> Path:
    """Where the Docling JSON of a document lives."""
    collection, name = _collection_and_name(dok_laufnr, dokudatum)
    return root / "docling" / f"{collection}_{METHOD}" / f"{name}_{METHOD}.json.gz"


def read(root: Path, dok_laufnr: int, dokudatum: str = "") -> str | None:
    """A document's Markdown, or nothing if the corpus lacks it."""
    try:
        return markdown_path(root, dok_laufnr, dokudatum).read_text(encoding="utf-8")
    except OSError:
        return None


def docling(root: Path, dok_laufnr: int, dokudatum: str = "") -> dict[str, Any] | None:
    """A document's stored DoclingDocument, or nothing if the corpus lacks it."""
    path = docling_path(root, dok_laufnr, dokudatum)
    return json.loads(gzip.decompress(path.read_bytes())) if path.exists() else None


def pack(text: str) -> bytes:
    """A document's Markdown as the database stores it."""
    return zlib.compress(text.encode("utf-8"))


def pdf_url(dok_laufnr: int) -> str:
    """The URL of a regular document's PDF."""
    return f"https://grosserrat.bs.ch/dokumente/{OFFSET + dok_laufnr // BUCKET}/{dok_laufnr:012d}.pdf"
