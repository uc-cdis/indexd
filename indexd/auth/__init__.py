from functools import wraps

from flask import current_app
from flask import request

from .errors import AuthError


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
            if not request.authorization:
                raise AuthError("Username / password required.")
            current_app.auth.auth(
                request.authorization.username, request.authorization.password
            )

            return f(*args, **kwargs)

        return check_auth
    else:
        method, resources = p
        if request.authorization:
            current_app.auth.auth(
                request.authorization.username, request.authorization.password
            )
        else:
            if not isinstance(resources, list):
                raise UserError(f"'authz' must be a list, received '{resources}'.")
            current_app.auth.authz(method, list(set(resources)))
