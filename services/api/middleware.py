"""
Request body size limiting (Phase 23).

Checked via `Content-Length` first (the cheap case -- most real
clients, including `scripts/load_test.py`, send one): a declared size
over the limit is rejected before a single byte of the body is read.
A request with no `Content-Length` (chunked transfer-encoding) falls
through to streaming the body via the ASGI `receive` callable and
counting bytes as they arrive, aborting as soon as the running total
crosses the limit -- so a body that lies about its own length (or
simply doesn't declare one) can't bypass this by streaming past it.

This is deliberately a raw ASGI middleware, not a Starlette
`BaseHTTPMiddleware` subclass: `BaseHTTPMiddleware` buffers the
entire request body into memory before the route handler (or any
inner middleware) ever sees it, which defeats the entire point of a
*body size* guard -- the oversized body would already be fully
read before this check could reject it. Working at the raw ASGI
`receive` level means an oversized streamed body is caught while
it's still arriving, never fully buffered.
"""

from starlette.types import ASGIApp, Receive, Scope, Send


class MaxBodySizeMiddleware:
    def __init__(self, app: ASGIApp, *, max_body_bytes: int) -> None:
        self._app = app
        self._max_body_bytes = max_body_bytes

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self._app(scope, receive, send)
            return

        headers = dict(scope.get("headers", []))
        content_length = headers.get(b"content-length")
        if content_length is not None:
            try:
                declared_size = int(content_length)
            except ValueError:
                declared_size = None
            if declared_size is not None and declared_size > self._max_body_bytes:
                await _reject_payload_too_large(send)
                return
            # A valid, in-range Content-Length is trustworthy enough
            # to skip the per-chunk counting below -- the server
            # itself (uvicorn) won't deliver more body bytes than
            # declared for a well-formed request.
            await self._app(scope, receive, send)
            return

        # No Content-Length (e.g. chunked transfer-encoding): wrap
        # `receive` so every chunk is counted as it arrives, and bail
        # out the moment the running total crosses the limit rather
        # than trusting the client's framing.
        total_received = 0

        async def _counting_receive() -> dict:
            nonlocal total_received
            message = await receive()
            if message["type"] == "http.request":
                total_received += len(message.get("body", b""))
                if total_received > self._max_body_bytes:
                    raise _BodyTooLarge()
            return message

        try:
            await self._app(scope, _counting_receive, send)
        except _BodyTooLarge:
            await _reject_payload_too_large(send)


class _BodyTooLarge(Exception):
    pass


async def _reject_payload_too_large(send: Send) -> None:
    await send(
        {
            "type": "http.response.start",
            "status": 413,
            "headers": [(b"content-type", b"application/json")],
        }
    )
    await send(
        {
            "type": "http.response.body",
            "body": b'{"detail":"request body exceeds the maximum allowed size"}',
        }
    )
