from functools import wraps

from flask import current_app, request

from .errors import AuthError


def authorize(f):
    """
    Decorator for requiring auth.
    Replaces the request authorization with a user context.
    Raises AuthError if authorization fails.
    """

    @wraps(f)
    def check_auth(*args, **kwargs):
        if not request.authorization:
            raise AuthError("Username / password required.")
        user = current_app.auth.auth(
            request.authorization.username,
            request.authorization.password,
        )
        request.authorization = user

        return f(*args, **kwargs)

    return check_auth
