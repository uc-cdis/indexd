import functools
import time

import flask
import jwt


def request_auth_cache():
    """
    Decorator to cache the result of a function for a specified maximum TTL in seconds.
    Use the Authentication header's token and included the Authorization header is in the cache key.
    https://github.com/uc-cdis/indexd/pull/405#discussion_r2402579919
    """

    def decorator(func):
        cache = {}

        @functools.wraps(func)
        def wrapper(*args, **kwargs):

            now = time.time()

            # Clean up any old cache entries
            keys_to_delete = [k for k, (v, t) in cache.items() if t <= now]
            for k in keys_to_delete:
                del cache[k]

            # Use the Authorization header to the key.
            # This is useful for cases where the function does not require a token,
            # but still needs to cache based on the Authorization header.
            auth_header = flask.request.headers.get('Authorization', None)

            exp = None
            key = None
            if auth_header and auth_header.startswith('Bearer '):
                token = auth_header.split(' ', 1)[1]
                exp = get_expiration(token)
                # Add the Authorization header to the key
                key = functools._make_key(args + (auth_header,), kwargs, typed=False)

                # Check if the result is already cached
                if key in cache:
                    result, timestamp = cache[key]
                    return result

            # If not cached or expired, call the function and cache the result
            result = func(*args, **kwargs)
            # only cache if we have a valid key and expiration time
            if key and exp:
                cache[key] = (result, exp)

            return result

        def get_expiration(token) -> int:
            """
            Decode the JWT token without verifying the signature to get the 'exp' claim
            """
            # If the token is a string, decode it
            token = token.encode('utf-8') if isinstance(token, str) else token
            # we could check for jwt.exceptions.DecodeError here, but we assume the token is valid
            # and just decode it to get the expiration time
            try:
                payload = jwt.decode(token, options={"verify_signature": False})
                exp = payload.get("exp")
                return exp
            except jwt.exceptions.DecodeError:
                raise ValueError("Invalid JWT token")

        return wrapper

    return decorator
