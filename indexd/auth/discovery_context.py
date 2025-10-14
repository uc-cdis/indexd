from contextvars import ContextVar
import inspect
from functools import wraps
from typing import Any, Mapping


import flask

from indexd import auth

discovery_context = ContextVar("discovery_context", default={})


def set_auth_context(**kwargs) -> None:
    """
    Set key-value pairs in the request context.
    You should not need to call this function directly, as
    ensure_auth_context() will set the context for you.
    However, this function is exposed for testing purposes.
    """
    discovery_context.set(kwargs)


def get_auth_context() -> dict:
    """
    Get the current request context.
    Returns a dict with variables:
        are_records_discoverable: application wide setting, ARE_RECORDS_DISCOVERABLE
        can_user_discover: for the current user, does this user have access to the GLOBAL_DISCOVERY_AUTHZ
        authorized_resources: what resources the current user has read access to (from Arborist)
    """
    return discovery_context.get()


def reset_auth_context() -> None:
    """
    Reset the request context to an empty dict.
    Primarily useful for testing.
    """
    discovery_context.set({})


def _get_authorized_resources() -> list[str]:
    """Retrieve the user's authorized resources from Arborist."""
    # Arborist allows no token to be sent on purpose, it allows assignment of anonymous access
    # See https://github.com/uc-cdis/indexd/pull/400#discussion_r2243298256
    authorized_resources = []
    for resource, permissions in auth.get_authorized_resources().items():
        for permission in permissions:
            method = permission['method'].lower().strip()
            service = permission['service'].lower().strip()
            if method == "read" and service in ["indexd", "*"]:
                authorized_resources.append(resource)
    return authorized_resources


def auth_context() -> tuple[bool, bool, list[str]]:
    """
    Returns a tuple of (are_records_discoverable, can_user_discover, authorized_resources)
    from the current request context.
    """
    are_records_discoverable = flask.current_app.config.get('ARE_RECORDS_DISCOVERABLE', True)

    # Does user have access to "GLOBAL_DISCOVERY_AUTHZ" resource?
    global_discovery_authz: list = flask.current_app.config.get('GLOBAL_DISCOVERY_AUTHZ', [])
    authorized_resources: list = []
    if not are_records_discoverable:
        authorized_resources = _get_authorized_resources()
    # if any of the global discovery authz resources are in the authorized resources, RBAC is not enabled
    can_user_discover = False
    if any(resource in authorized_resources for resource in global_discovery_authz):
        can_user_discover = True

    # Remove global discovery authz resources from authorized_resources to avoid confusion
    authorized_resources = [_ for _ in authorized_resources if _ not in global_discovery_authz]

    # if are_records_discoverable is True, then can_user_discover is True for everyone
    if are_records_discoverable:
        can_user_discover = True
    return are_records_discoverable, can_user_discover, authorized_resources


def ensure_auth_context() -> None:
    """
    Ensure that the request context has the authorized resources set.
    If not, retrieve them from Arborist and set them in the context.
    Sets the following variables:
        are_records_discoverable: application wide setting, ARE_RECORDS_DISCOVERABLE
        can_user_discover: for the current user, does this user have access to the GLOBAL_DISCOVERY_AUTHZ
        authorized_resources: what resources the current user has read access to (from Arborist)
    """
    are_records_discoverable, can_user_discover, authorized_resources = auth_context()

    set_auth_context(are_records_discoverable=are_records_discoverable,
                     can_user_discover=can_user_discover,
                     authorized_resources=authorized_resources)


def authorize_discovery(func):
    """
    Injects `can_user_discover` and `authorized_resources` into the wrapped callable
    from the ContextVar-backed auth context (set by ensure_auth_context).

    Injection rules:
      - Only inject for parameters actually present in the target signature.
      - Only fill when the argument is missing or None (does not override explicit values).
    """
    sig = inspect.signature(func)
    inject_params = ("can_user_discover", "authorized_resources")
    is_async = inspect.iscoroutinefunction(func)

    def _apply(bound: inspect.BoundArguments) -> None:
        ctx: Mapping[str, Any] = get_auth_context() or {}
        for name in inject_params:
            if name not in sig.parameters:
                continue
            current = bound.arguments.get(name, None)
            if current is None:
                if name in ctx:
                    bound.arguments[name] = ctx[name]

    if is_async:
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            bound = sig.bind_partial(*args, **kwargs)
            _apply(bound)
            # Use keyword-style call to avoid "multiple values" issues
            return await func(**bound.arguments)
        return async_wrapper

    @wraps(func)
    def wrapper(*args, **kwargs):
        bound = sig.bind_partial(*args, **kwargs)
        _apply(bound)
        return func(**bound.arguments)
    return wrapper
