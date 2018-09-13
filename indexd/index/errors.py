class BaseIndexError(Exception):
    """
    Base index error.
    """


class NoRecordFound(BaseIndexError):
    """
    No record error.
    """


class MultipleRecordsFound(BaseIndexError):
    """
    Multiple recordss error.
    """


class RevisionMismatch(BaseIndexError):
    """
    Revision mismatch.
    """


class UnhealthyCheck(BaseIndexError):
    """
    Health check failed.
    """


class AddExistedColumn(BaseIndexError):
    """
    Existed column error.
    """


class AddExistedTable(BaseIndexError):
    """
    Existed table error.
    """
