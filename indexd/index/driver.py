import abc
import uuid


class IndexDriverABC(object):
    '''
    Index Driver Abstract Base Class

    Driver interface for interacting with index backends.
    '''
    __metaclass__ = abc.ABCMeta

    def add(self, data):
        '''
        Creates record for given data.
        '''
        record = str(uuid.uuid4())
        
        self[record] = data
        
        return record

    def get(self, record):
        '''
        Returns data for record if stored by backend.
        Raises KeyError otherwise.
        '''
        return self[record]

    def update(self, record, data):
        '''
        Replaces record if stored by backend.
        Raises KeyError otherwise.
        '''
        if not record in self:
            raise KeyError('record does not exist')
        
        self[record] = data

    def delete(self, record):
        '''
        Deletes record if stored by backend.
        Raises KeyError otherwise.
        '''
        del self[record]

    @abc.abstractmethod
    def ids(self, limit=100, start=''):
        '''
        Returns a list of records stored by the backend.
        '''
        raise NotImplementedError('TODO')

    @abc.abstractmethod
    def __getitem__(self, record):
        '''
        Returns record if stored by backend.
        Raises KeyError otherwise.
        '''
        raise NotImplementedError('TODO')

    @abc.abstractmethod
    def __setitem__(self, record, data):
        '''
        Replaces record if stored by backend.
        Raises KeyError otherwise.
        '''
        raise NotImplementedError('TODO')

    @abc.abstractmethod
    def __delitem__(self, record):
        '''
        Removes record if stored by backend.
        Raises KeyError otherwise.
        '''
        raise NotImplementedError('TODO')

    @abc.abstractmethod
    def __contains__(self, record):
        '''
        Returns True if record is stored by backend.
        Returns False otherwise.
        '''
        raise NotImplementedError('TODO')

    @abc.abstractmethod
    def __iter__(self):
        '''
        Returns an iterator over unique records stored by backend.
        '''
        raise NotImplementedError('TODO')

    @abc.abstractmethod
    def __len__(self):
        '''
        Returns the number of unique records stored by backend.
        '''
        raise NotImplementedError('TODO')
