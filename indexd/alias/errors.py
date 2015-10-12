class BaseAliasError(Exception):
    '''
    Base alias error.
    '''

class NoAliasError(BaseAliasError):
    '''
    No alias error.
    '''

class AliasExistsError(BaseAliasError):
    '''
    Alias exists error.
    '''
