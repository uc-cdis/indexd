from functools import wraps

from flask import current_app
from flask import request

from indexd.auth.errors import AuthError, AuthzError
from indexd.errors import UserError


def authorize(*p):
    """
    Decorator for requiring auth.
    Replaces the request authorization with a user context.
    Raises AuthError if authorization fails.

    If called with (method, resources), it will check with Arborist if HTTP Basic Auth is
    not present, or fallback to the previous check.
    """
    if len(p) == 1:
        (f,) = p

        @wraps(f)
        def check_auth(*args, **kwargs):
            if not request.authorization.parameters.get("username"):
                raise AuthError("Username / password required.")
            current_app.auth.auth(
                request.authorization.parameters.get("username"),
                request.authorization.parameters.get("password"),
            )

            return f(*args, **kwargs)

        return check_auth
    else:
        method, authz_resources = p
        if request.authorization and request.authorization.type == "basic":
            current_app.auth.auth(
                request.authorization.parameters.get("username"),
                request.authorization.parameters.get("password"),
            )
        else:
            if not isinstance(authz_resources, list):
                raise UserError(f"'authz' must be a list, received '{authz_resources}'.")
            return current_app.auth.authz(method, list(set(authz_resources)))


def get_authorized_resources():
    """
    Returns a list of resources the user has access to. Uses Arborist if available.
    """
    return current_app.auth.resources()
