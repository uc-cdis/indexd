from contextvars import ContextVar

import flask

from indexd import auth

request_ctx = ContextVar("request_ctx", default={})


def _set_request_ctx(**kwargs) -> None:
    """
    Set key-value pairs in the request context.
    """
    request_ctx.set(kwargs)


def get_auth_context() -> dict:
    """
    Get the current request context.
    Returns a dict with variables:
        are_records_discoverable: application wide setting, ARE_RECORDS_DISCOVERABLE
        can_user_discover: for the current user, does this user have access to the GLOBAL_DISCOVERY_AUTHZ
        authorized_resources: what resources the current user has read access to (from Arborist)
    """
    return request_ctx.get()


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


def ensure_auth_context() -> None:
    """
    Ensure that the request context has the authorized resources set.
    If not, retrieve them from Arborist and set them in the context.
    Sets the following variables:
        are_records_discoverable: application wide setting, ARE_RECORDS_DISCOVERABLE
        can_user_discover: for the current user, does this user have access to the GLOBAL_DISCOVERY_AUTHZ
        authorized_resources: what resources the current user has read access to (from Arborist)
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

    _set_request_ctx(are_records_discoverable=are_records_discoverable,
                     can_user_discover=can_user_discover,
                     authorized_resources=authorized_resources)
