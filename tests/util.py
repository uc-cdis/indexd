import os
import shutil
import time


class removes(object):
    '''
    Decorator to remove a path after function call.
    '''

    def __init__(self, path):
        self.path = path

    def __call__(self, f):
        '''
        Ensures path is removed after function call.
        '''
        def wrapper(*args, **kwargs):
            try:
                return f(*args, **kwargs)
            finally:
                if not os.path.exists(self.path):
                    pass
                elif os.path.isdir(self.path):
                    shutil.rmtree(self.path)
                elif os.path.isfile(self.path):
                    os.remove(self.path)

        return wrapper
