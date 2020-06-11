from .auth.errors import AuthError, AuthzError


class UserError(Exception):
    """
    User error.
    """


class ConfigurationError(Exception):
    """
    Configuration error.
    """


class UnexpectedError(Exception):
    """
    Unexpected Error
    """
