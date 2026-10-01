class BaseIndexDError(Exception):
    """
    Base IndexD error.
    """


class UserError(BaseIndexDError):
    """
    User error.
    """


class ConfigurationError(BaseIndexDError):
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
