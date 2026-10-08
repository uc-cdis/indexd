import re

from starlette.types import ASGIApp, Receive, Scope, Send

_REPEATED_SLASHES = re.compile(r"/{2,}")
_REPEATED_SLASHES_BYTES = re.compile(rb"/{2,}")


class MergeSlashesMiddleware:
    """
    Collapse repeated slashes in the request path before it reaches the router.

    Werkzeug has done this since 0.15 (``Map(merge_slashes=True)``), so under
    Flask a caller hitting "//index/<guid>" got a 308 to "/index/<guid>" and the
    request succeeded. Starlette has no equivalent: its ``redirect_slashes`` only
    covers a single missing or extra *trailing* slash. Without this,
    "//index/<guid>" survives into routing and falls through to the
    "/{record:path}" catch-all in indexd/router.py, 404ing as an unknown record
    -- or 404ing outright on the verbs that catch-all does not serve.

    This rewrites in place rather than redirecting like Werkzeug did, so callers
    do not need to follow a redirect or re-send a request body.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http" and "//" in scope.get("path", ""):
            scope = dict(scope)
            scope["path"] = _REPEATED_SLASHES.sub("/", scope["path"])

            # Keep raw_path consistent for anything reading it downstream. It is
            # still percent-encoded, so an encoded slash (%2F) is untouched here
            # and only real separators collapse. Some ASGI servers append the
            # query string, so only the path portion is rewritten.
            raw_path = scope.get("raw_path")
            if raw_path:
                raw_path, sep, query = raw_path.partition(b"?")
                scope["raw_path"] = (
                    _REPEATED_SLASHES_BYTES.sub(b"/", raw_path) + sep + query
                )

        await self.app(scope, receive, send)
