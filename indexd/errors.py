from .auth.errors import AuthError


class UserError(Exception):
    """
    User error.
    """


class ConfigurationError(Exception):
    """
    Configuration error.
    """
