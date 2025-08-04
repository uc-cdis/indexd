import functools
import time

import flask
import jwt


def request_auth_cache(maximum_ttl_seconds=1800):
    """
    Decorator to cache the result of a function for a specified maximum TTL in seconds.
    The actual cache duration is determined by the 'token' parameter's expiration.
    If no token is provided, the maximum TTL is used and the Authorization header is included in the cache key.
    """

    def decorator(func):
        cache = {}

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            key = functools._make_key(args, kwargs, typed=False)
            now = time.time()

            # Extract token from args or kwargs
            token = kwargs.get("token")
            if token is None:
                # print("No token provided in kwargs")
                if type(args[0]) is str:
                    # If the first argument is a string, assume it's the token
                    token = args[0]
                else:
                    token = args[1] if len(args) > 1 else None

            # Calculate token expiration duration
            if token:
                # Decode the JWT token without verifying the signature to get the 'exp' claim
                # If the token is a string, decode it
                token = token.encode('utf-8') if isinstance(token, str) else token

                # we could check for jwt.exceptions.DecodeError here, but we assume the token is valid
                # and just decode it to get the expiration time
                payload = jwt.decode(token, options={"verify_signature": False})

                exp = payload.get("exp", now + maximum_ttl_seconds)
                token_ttl = max(0, exp - now)
            else:
                # If no token is provided, use the maximum TTL and add the Authorization header to the key.
                # This is useful for cases where the function does not require a token,
                # but still needs to cache based on the Authorization header.
                auth_header = flask.request.headers.get('Authorization', '')
                # Add the Authorization header to the key
                key = functools._make_key(args + (auth_header,), kwargs, typed=False)
                token_ttl = maximum_ttl_seconds

            ttl = min(token_ttl, maximum_ttl_seconds)

            # Check if the result is already cached and still valid
            if key in cache:
                result, timestamp = cache[key]
                if now - timestamp < ttl:
                    return result

            # If not cached or expired, call the function and cache the result
            result = func(*args, **kwargs)
            cache[key] = (result, now)

            # Clean up any old cache entries
            keys_to_delete = [k for k, (v, t) in cache.items() if now - t >= ttl]
            for k in keys_to_delete:
                del cache[k]

            return result

        return wrapper

    return decorator
