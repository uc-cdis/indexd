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


class IndexdUnexpectedError(BaseIndexDError):
    """
    Unexpected Error
    """

    def __init__(self, code=500, message="Unexpected Error"):
        self.code = code
        self.message = str(message)
