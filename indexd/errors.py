from .index.errors import NoRecordError
from .index.errors import MultipleRecordsError

from .alias.errors import NoAliasError
from .alias.errors import AliasExistsError


class PermissionError(Exception):
    '''
    Permission error.
    '''

class UserError(Exception):
    '''
    User error.
    '''

class ConfigurationError(Exception):
    '''
    Configuration error.
    '''
