"""HTTP retry helpers shared by ETL and publish scripts."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import TypeVar

import httpx
from common.retry import retry

HTTP_ERRORS_TO_RETRY = (
    httpx.HTTPError,
    ConnectionError,
    TimeoutError,
    OSError,
)
HTTP_RETRY_TRIES = 6
HTTP_RETRY_DELAY = 5
HTTP_RETRY_BACKOFF = 1

T = TypeVar("T")


def with_http_retry[**P, R](func: Callable[P, R]) -> Callable[P, R]:
    """Retry a sync callable on transient HTTP/network failures."""
    return retry(
        HTTP_ERRORS_TO_RETRY,
        tries=HTTP_RETRY_TRIES,
        delay=HTTP_RETRY_DELAY,
        backoff=HTTP_RETRY_BACKOFF,
        logger=None,
    )(func)


async def await_with_http_retry(
    operation: Callable[[], Awaitable[T]],
    *,
    tries: int = HTTP_RETRY_TRIES,
    delay: float = HTTP_RETRY_DELAY,
    backoff: float = HTTP_RETRY_BACKOFF,
) -> T:
    """Retry an async operation on transient HTTP/network failures."""
    mtries = tries
    mdelay = delay
    while True:
        try:
            return await operation()
        except HTTP_ERRORS_TO_RETRY:
            mtries -= 1
            if mtries <= 0:
                raise
            await asyncio.sleep(mdelay)
            mdelay *= backoff
