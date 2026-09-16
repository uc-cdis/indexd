from .auth.errors import AuthError, AuthzError


class UserError(Exception):
    """
    User error.
    """


class ConfigurationError(Exception):
    """
    Configuration error.
    """


class RequestTooLargeError(Exception):
    """
    Request Too Large Error
    """

    def __init__(self, code=413, message="Request Too Large"):
        self.code = code
        self.message = str(message)


class IndexdUnexpectedError(Exception):
    """
    Unexpected Error
    """

    def __init__(self, code=500, message="Unexpected Error"):
        self.code = code
        self.message = str(message)
