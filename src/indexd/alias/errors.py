class BaseAliasError(Exception):
    """
    Base alias error.
    """


class NoRecordFoundError(BaseAliasError):
    """
    No record error.
    """


class MultipleRecordsFoundError(BaseAliasError):
    """
    Multiple recordss error.
    """


class RevisionMismatchError(BaseAliasError):
    """
    Revision mismatch.
    """
