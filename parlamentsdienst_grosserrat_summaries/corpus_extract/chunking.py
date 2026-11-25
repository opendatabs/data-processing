"""Cut a long document into chunks the model reads in one go."""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from typing import Any

from docling_core.transforms.chunker.hybrid_chunker import HybridChunker
from docling_core.transforms.chunker.tokenizer.huggingface import HuggingFaceTokenizer
from docling_core.types.doc.document import DoclingDocument
from transformers import AutoTokenizer

TOKENIZER = "Qwen/Qwen3-Embedding-4B"
MAX_TOKENS = 6000
MERGED_CHARS = 12_000


@dataclass(frozen=True, slots=True)
class Chunk:
    """A piece of a document with the pages it comes from."""

    order: int
    text: str
    first_page: int | None
    last_page: int | None

    @property
    def pages(self) -> str:
        """The pages as a label for the hint to the model."""
        if self.first_page is None:
            return "?"
        return str(self.first_page) if self.first_page == self.last_page else f"{self.first_page}-{self.last_page}"


@lru_cache(maxsize=2)
def _chunker(tokenizer: str, max_tokens: int) -> HybridChunker:
    """Build the chunker once per setting; it loads its own tokenizer."""
    return HybridChunker(
        tokenizer=HuggingFaceTokenizer(tokenizer=AutoTokenizer.from_pretrained(tokenizer), max_tokens=max_tokens)
    )


def _pages(piece: Any) -> tuple[int | None, int | None]:
    """The pages a piece touches, from the provenance docling carries along."""
    pages = [
        prov.page_no
        for item in getattr(piece.meta, "doc_items", None) or []
        for prov in getattr(item, "prov", None) or []
        if isinstance(getattr(prov, "page_no", None), int)
    ]
    return (min(pages), max(pages)) if pages else (None, None)


def split(document: dict[str, Any], *, tokenizer: str = TOKENIZER, max_tokens: int = MAX_TOKENS) -> list[Chunk]:
    """Cut a DoclingDocument into chunks along its structure."""
    loaded = DoclingDocument.model_validate(document)
    chunker = _chunker(tokenizer, max_tokens)
    chunks: list[Chunk] = []
    for order, piece in enumerate(chunker.chunk(loaded), start=1):
        text = chunker.contextualize(piece)
        if not text.strip():
            continue
        first, last = _pages(piece)
        chunks.append(Chunk(order=order, text=text, first_page=first, last_page=last))
    return chunks


def merge(chunks: list[Chunk], *, max_chars: int = MERGED_CHARS) -> list[Chunk]:
    """Join consecutive pieces as long as they fit into one request."""
    merged: list[Chunk] = []
    for chunk in chunks:
        previous = merged[-1] if merged else None
        if previous is not None and len(previous.text) + len(chunk.text) + 2 <= max_chars:
            merged[-1] = Chunk(
                order=previous.order,
                text=f"{previous.text}\n\n{chunk.text}",
                first_page=previous.first_page if previous.first_page is not None else chunk.first_page,
                last_page=chunk.last_page if chunk.last_page is not None else previous.last_page,
            )
            continue
        merged.append(
            Chunk(order=len(merged) + 1, text=chunk.text, first_page=chunk.first_page, last_page=chunk.last_page)
        )
    return merged
