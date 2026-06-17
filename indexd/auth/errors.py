class BaseAuthError(Exception):
    """
    Base auth error.
    """


class AuthError(BaseAuthError):
    """
    Auth error.
    """


class AuthzError(BaseAuthError):
    """
    Authz error.
    """
