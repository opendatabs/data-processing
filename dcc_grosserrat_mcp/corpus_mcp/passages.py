"""Passages for the vectors: Docling's HybridChunker cut, each with its heading path and page."""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from typing import Any

from docling_core.transforms.chunker.hierarchical_chunker import DocChunk
from docling_core.transforms.chunker.hybrid_chunker import HybridChunker
from docling_core.transforms.chunker.tokenizer.huggingface import HuggingFaceTokenizer
from docling_core.types.doc.document import DoclingDocument
from pydantic import ValidationError
from transformers import AutoTokenizer

from corpus_mcp import embeddings

# The embedding model's tokenizer counts the token limit.
TOKENIZER = embeddings.MODEL
# Tokens per passage.
MAX_TOKENS = 512
# Cut version, stored with each vector; a change re-embeds every document.
VERSION = f"docling-hybrid-{MAX_TOKENS}"


@dataclass(frozen=True, slots=True)
class Passage:
    """A stretch of a document as Docling cut it: its words, its headings, its page."""

    text: str
    headings: str
    page: int | None

    def for_embedding(self, title: str) -> str:
        """What the embedding model reads: the document's title, the heading path, the text."""
        return "\n".join(part for part in (title, self.headings, self.text) if part)


def cut(document: dict[str, Any]) -> list[Passage]:
    """The passages of a DoclingDocument in reading order; none if it fails validation."""
    try:
        loaded = DoclingDocument.model_validate(document)
    except ValidationError:
        return []
    passages: list[Passage] = []
    for base in _chunker().chunk(loaded):
        chunk = DocChunk.model_validate(base)
        text = " ".join(chunk.text.split())
        if not text:
            continue
        headings = " > ".join(" ".join(h.split()) for h in (chunk.meta.headings or []) if h.strip())
        pages = [prov.page_no for item in chunk.meta.doc_items for prov in item.prov]
        passages.append(Passage(text=text, headings=headings, page=min(pages) if pages else None))
    return passages


@lru_cache(maxsize=1)
def _chunker() -> HybridChunker:
    """The chunker, built once: it loads the tokenizer."""
    tokenizer = HuggingFaceTokenizer(tokenizer=AutoTokenizer.from_pretrained(TOKENIZER), max_tokens=MAX_TOKENS)
    return HybridChunker(tokenizer=tokenizer, merge_peers=True)
