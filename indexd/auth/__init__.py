from functools import wraps

from flask import current_app
from flask import request

from .errors import AuthError


def authorize(*p):
    """
    Decorator for requiring auth.
    Replaces the request authorization with a user context.
    Raises AuthError if authorization fails.

    If called with (method, resource), it will check with Arborist if HTTP Basic Auth is
    not present, or fallback to the previous check.
    """
    if len(p) == 1:
        f, = p

        @wraps(f)
        def check_auth(*args, **kwargs):
            if not request.authorization:
                raise AuthError("Username / password required.")
            user = current_app.auth.auth(
                request.authorization.username, request.authorization.password
            )
            request.authorization = user

            return f(*args, **kwargs)

        return check_auth
    else:
        method, resource = p
        if request.authorization:
            request.authorization = current_app.auth.auth(
                request.authorization.username, request.authorization.password
            )
        else:
            current_app.auth.authz(method, resource)
