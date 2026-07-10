import base64
from .errors import AuthzError
from ..errors import UserError

from functools import wraps
from fastapi import Depends, Request
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from typing import Optional

security = HTTPBasic(auto_error=False)


def authorize(method: str, resources: list, request: Request):
    if not isinstance(resources, list):
        raise UserError(f"'authz' must be a list, received '{resources}'.")

    credentials = _get_basic_credentials(request)
    if credentials:
        # Basic Auth present: only validate credentials, skip arborist
        request.app.auth.auth(credentials.username, credentials.password)
    else:
        # No Basic Auth: check arborist
        request.app.auth.authz(method, list(set(resources)))


def authorize_decorator(
    request: Request, credentials: Optional[HTTPBasicCredentials] = Depends(security)
):
    """
    FastAPI dependency for requiring auth on a route.
    Use via: dependencies=[Depends(authorize_decorator)]
    Raises AuthError if authorization fails.
    """
    if not credentials:
        raise AuthzError("Username / password required.")
    request.app.auth.auth(credentials.username, credentials.password)


def _get_basic_credentials(request: Request) -> Optional[HTTPBasicCredentials]:
    """
    Extract Basic Auth credentials from request if present.
    """
    import base64

    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.lower().startswith("basic "):
        return None
    try:
        decoded = base64.b64decode(auth_header[6:]).decode("utf-8")
        username, password = decoded.split(":", 1)
        return HTTPBasicCredentials(username=username, password=password)
    except Exception:
        return None
