"""Every call to a server outside the pipeline: a timeout, three attempts on a transient
failure, an error status raised. The Eudonet client (Paris) keeps its own retries."""

import time
from typing import Literal
from urllib.parse import urlsplit

import httpx
import requests
from loguru import logger

# (connect, read): a large layer weighs tens of megabytes, and without a read timeout a
# silent server holds the job until GitHub kills it.
TIMEOUT = (10, 600)
ATTEMPTS = 3
WAIT_SECONDS = 30
# Five minutes of waiting at most: during an outage every call would wait again.
RETRIES_PER_RUN = 10

_retries_left = RETRIES_PER_RUN


def get(url: str, **kwargs) -> requests.Response:
    return request("get", url, **kwargs)


def request(
    method: Literal["get", "put"], url: str, *, timeout=TIMEOUT, **kwargs
) -> requests.Response:
    """`requests.{method}`, retried, raising on an error status.

    Only for a call that can be sent twice: a read, or a write the server deduplicates.
    """
    attempt = 1
    while True:
        try:
            response = getattr(requests, method)(url, timeout=timeout, **kwargs)
            response.raise_for_status()
            return response
        except requests.RequestException as error:
            where = f"{method.upper()} {urlsplit(url).netloc}"
            if not (
                _is_transient_requests_error(error) and _wait_before_retry(where, error, attempt)
            ):
                raise
        attempt += 1


class RetryingTransport(httpx.HTTPTransport):
    """DiaLog's calls, sent again only when DiaLog cannot have acted on the first one."""

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        reading = request.method == "GET"
        where = f"DiaLog {request.method} {request.url.path}"
        attempt = 1
        while True:
            try:
                response = httpx.HTTPTransport.handle_request(self, request)
                # httpx reads the body above the transport, out of the retry's reach.
                if reading:
                    response.read()
            except httpx.TransportError as error:
                # Once sent, a write may land even without an answer: the router answers
                # 504 while the back end commits.
                never_sent = isinstance(error, (httpx.ConnectError, httpx.ConnectTimeout))
                resend = _is_transient_httpx_error(error) if reading else never_sent
                if not (resend and _wait_before_retry(where, error, attempt)):
                    raise
            else:
                status = response.status_code
                if not (
                    reading
                    and _is_transient_status(status)
                    and _wait_before_retry(where, status, attempt)
                ):
                    return response
                response.close()
            attempt += 1


def _wait_before_retry(where: str, cause: Exception | int, attempt: int) -> bool:
    global _retries_left
    if attempt >= ATTEMPTS:
        return False
    if isinstance(cause, requests.HTTPError) and cause.response is not None:
        cause = cause.response.status_code
    described = f"HTTP {cause}" if isinstance(cause, int) else type(cause).__name__
    if _retries_left == 0:
        logger.warning(f"{where} failed ({described}), not retried: the run spent its retries")
        return False
    _retries_left -= 1
    logger.warning(
        f"{where} failed ({described}), attempt {attempt}/{ATTEMPTS}, next one in {WAIT_SECONDS} s"
    )
    time.sleep(WAIT_SECONDS)
    return True


def _is_transient_status(status: int) -> bool:
    return status >= 500 or status == 429


def _is_transient_requests_error(error: requests.RequestException) -> bool:
    if isinstance(error, requests.HTTPError):
        return error.response is not None and _is_transient_status(error.response.status_code)
    if isinstance(error, (requests.exceptions.SSLError, requests.exceptions.ProxyError)):
        return False
    # A body cut mid-transfer raises ChunkedEncodingError, which is no ConnectionError.
    return isinstance(
        error,
        (requests.ConnectionError, requests.Timeout, requests.exceptions.ChunkedEncodingError),
    )


def _is_transient_httpx_error(error: httpx.TransportError) -> bool:
    return isinstance(
        error, (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError)
    )
