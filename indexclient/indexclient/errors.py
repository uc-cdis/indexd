class BaseIndexError(Exception):
    """
    Base index error.
    """

    def __init__(self, code, msg):
        self.code = code
        self.msg = msg

    def __str__(self):
        return str(self.msg)
