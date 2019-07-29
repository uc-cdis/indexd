class BaseAliasError(Exception):
    """
    Base alias error.
    """


class NoRecordFound(BaseAliasError):
    """
    No record error.
    """


class MultipleRecordsFound(BaseAliasError):
    """
    Multiple recordss error.
    """


class RevisionMismatch(BaseAliasError):
    """
    Revision mismatch.
    """
