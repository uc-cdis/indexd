from .auth.errors import AuthError, AuthzError


class UserError(Exception):
    """
    User error.
    """


class ConfigurationError(Exception):
    """
    Configuration error.
    """


class IndexdUnexpectedError(Exception):
    """
    Unexpected Error
    """

    def __init__(self, code=500, message="Unexpected Error"):
        self.code = code
        self.message = str(message)
