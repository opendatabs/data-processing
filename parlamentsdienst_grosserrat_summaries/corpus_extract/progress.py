"""Progress of a long loop, as a log line every few percent."""

from __future__ import annotations

import logging
from collections.abc import Iterable, Iterator

logger = logging.getLogger(__name__)

# Log lines per loop.
STEPS = 20


def track[T](items: Iterable[T], *, label: str, total: int) -> Iterator[T]:
    """Pass the items through and log how far the loop has come."""
    every = max(1, total // STEPS)
    done = 0
    for done, item in enumerate(items, start=1):
        if done % every == 0 and done < total:
            logger.info("%s: %d/%d", label, done, total)
        yield item
    logger.info("%s: %d done", label, done)
