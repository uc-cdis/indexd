import abc

from . import errors


class AliasDriverABC(object):
    '''
    Alias Driver Abstract Base Class

    Driver interface for interacting with alias backends.
    '''
    __metaclass__ = abc.ABCMeta

    def add(self, alias, data):
        '''
        Creates alias if it does not already exist.
        Raises KeyError otherwise.
        '''
        if alias in self:
            raise errors.AliasExistsError('alias exists')
        
        self[alias] = data

    def get(self, alias, default=None):
        '''
        Returns data associated with alias if it exists.
        Raises KeyError otherwise.
        '''
        try: data = self[alias]
        except KeyError as err:
            data = default
        
        return data

    def update(self, alias, data):
        '''
        Replaces data associated with alias if it exists.
        Raises KeyError otherwise.
        '''
        if not alias in self:
            raise errors.NoAliasError('alias does not exist')
        
        self[alias] = data

    def delete(self, alias):
        '''
        Deletes alias if it exists.
        Raises KeyError otherwise.
        '''
        del self[alias]

    @abc.abstractmethod
    def aliass(self, limit=100, start=''):
        '''
        Returns a list of aliass stored by the backend.
        '''
        raise NotImplementedError('TODO')

    @abc.abstractmethod
    def __getitem__(self, record):
        '''
        Returns data associated with alias if it exists.
        Raises KeyError otherwise.
        '''
        raise NotImplementedError('TODO')

    @abc.abstractmethod
    def __setitem__(self, record, data):
        '''
        Adds or replaces data associated with alias.
        '''
        raise NotImplementedError('TODO')

    @abc.abstractmethod
    def __delitem__(self, record):
        '''
        Deletes alias if it exists.
        Raises KeyError otherwise.
        '''
        raise NotImplementedError('TODO')

    @abc.abstractmethod
    def __contains__(self, record):
        '''
        Returns True if alias exists.
        Returns False otherwise.
        '''
        raise NotImplementedError('TODO')

    @abc.abstractmethod
    def __iter__(self):
        '''
        Returns an iterator over aliass.
        '''
        raise NotImplementedError('TODO')

    @abc.abstractmethod
    def __len__(self):
        '''
        Returns the number of aliass.
        '''
        raise NotImplementedError('TODO')
