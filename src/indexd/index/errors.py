class BaseIndexError(Exception):
    """
    Base index error.
    """


class NoRecordFoundError(BaseIndexError):
    """
    No record error.
    """


class MultipleRecordsFoundError(BaseIndexError):
    """
    Multiple records error.
    """


class RevisionMismatchError(BaseIndexError):
    """
    Revision mismatch.
    """


class UnhealthyCheckError(BaseIndexError):
    """
    Health check failed.
    """


class AddExistedColumnError(BaseIndexError):
    """
    Existed column error.
    """


class AddExistedTableError(BaseIndexError):
    """
    Existed table error.
    """
