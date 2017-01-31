import abc

class AuthDriverABC(object):
    '''
    Auth Driver Abstract Base Class

    Driver interface for authorization.
    '''
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def auth(self, username, password):
        '''
        Returns a dict of user information.
        Raises AuthError otherwise.
        '''
        raise NotImplementedError('TODO')

    @abc.abstractmethod
    def add(self, username, password):
        '''
        Create an user.
        Raises AuthError if user already exists.
        '''
        raise NotImplementedError('TODO')

    @abc.abstractmethod
    def delete(self, username):
        '''
        Delete an user
        Raises AuthError if user doesn't exist.
        '''
        raise NotImplementedError('TODO')
