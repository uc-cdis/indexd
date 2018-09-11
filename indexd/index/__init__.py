from functools import wraps
from flask import request


def request_args_to_params(func):
    """ A decorator to extract query args from flask request and pass them as parameter"""

    @wraps(func)
    def wrapper(*args, **kwargs):

        try:
            args_dict = request.args
            for arg, value in args_dict.items():
                kwargs[arg] = value
            return func(*args, **kwargs)
        except Exception as e:
            print(e)

    return wrapper

