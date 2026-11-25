"""The corpus page markers: which PDF page starts where in the Markdown."""

from __future__ import annotations

PAGE_BREAK = "<!-- page break -->"

Anchor = tuple[int, int]


def split(body: str) -> tuple[str, list[Anchor]]:
    """Remove the page markers and remember where each page starts."""
    if PAGE_BREAK not in body:
        return body, [(1, 0)]
    parts = body.split(PAGE_BREAK)
    pieces = [parts[0]]
    anchors = [(1, 0)]
    length = len(parts[0])
    for number, part in enumerate(parts[1:], start=2):
        pieces.append("\n")
        length += 1
        anchors.append((number, length))
        pieces.append(part)
        length += len(part)
    return "".join(pieces), anchors
