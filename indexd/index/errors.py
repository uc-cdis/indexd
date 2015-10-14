class BaseIndexError(Exception):
    '''
    Base index error.
    '''

class NoRecordError(BaseIndexError):
    '''
    No record error.
    '''

class MultipleRecordsError(BaseIndexError):
    '''
    Multiple recordss error.
    '''

class IndexConfigurationError(Exception):
    '''
    Index configuration error.
    '''
