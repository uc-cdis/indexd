class BaseIndexError(Exception):
    '''
    Base index error.
    '''

class NoRecordFound(BaseIndexError):
    '''
    No record error.
    '''

class MultipleRecordsFound(BaseIndexError):
    '''
    Multiple recordss error.
    '''

class RevisionMismatch(BaseIndexError):
    '''
    Revision mismatch.
    '''
