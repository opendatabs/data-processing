"""HTTP GET helpers with retries."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

import httpx
from common.retry import retry

HTTP_TIMEOUT = httpx.Timeout(60.0, connect=20.0)
HTTP_TIMEOUT_LONG = httpx.Timeout(180.0, connect=30.0)
HTTP_LIMITS = httpx.Limits(max_connections=20, max_keepalive_connections=10)

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


@with_http_retry
def http_get_bytes(
    url: str,
    *,
    headers: dict[str, str] | None = None,
    timeout: httpx.Timeout | None = None,
) -> bytes:
    with httpx.Client(timeout=timeout or HTTP_TIMEOUT, limits=HTTP_LIMITS) as client:
        response = client.get(url, headers=headers)
    response.raise_for_status()
    return response.content


@with_http_retry
def http_get_json(
    url: str,
    *,
    headers: dict[str, str] | None = None,
    timeout: httpx.Timeout | None = None,
    allow_404: bool = False,
) -> dict[str, Any] | None:
    with httpx.Client(timeout=timeout or HTTP_TIMEOUT, limits=HTTP_LIMITS) as client:
        response = client.get(url, headers=headers)
    if response.status_code in {404, 410} and allow_404:
        return None
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        return None
    return payload


async def http_get_json_async(
    client: httpx.AsyncClient,
    url: str,
    *,
    headers: dict[str, str] | None = None,
    allow_404: bool = False,
) -> dict[str, Any] | None:
    async def _fetch() -> dict[str, Any] | None:
        response = await client.get(url, headers=headers)
        if response.status_code in {404, 410} and allow_404:
            return None
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            return None
        return payload

    return await await_with_http_retry(_fetch)
