import functools
import time


def timed_cache(ttl_seconds):
    """
    Decorator to cache the result of a function for a specified time-to-live (TTL) in seconds.
    """
    def decorator(func):
        cache = {}

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            key = functools._make_key(args, kwargs, typed=False)
            now = time.time()
            if key in cache:
                result, timestamp = cache[key]
                if now - timestamp < ttl_seconds:
                    return result
            result = func(*args, **kwargs)
            cache[key] = (result, now)
            return result
        return wrapper
    return decorator
