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
