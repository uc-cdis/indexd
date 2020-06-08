from indexd import get_app
import base64
import pytest
from flask import request
from unittest.mock import patch

import importlib

# indexd_server and indexd_client is needed as fixtures
from cdisutilstest.code.conftest import indexd_server, indexd_client  # noqa
from cdisutilstest.code.indexd_fixture import clear_database

from indexd import auth
from indexd.auth.errors import AuthError
from tests import default_test_settings


@pytest.fixture
def app():
    # this is to make sure sqlite is initialized
    # for every unittest
    from indexd import default_settings

    importlib.reload(default_settings)

    yield get_app(default_test_settings.settings)
    try:
        clear_database()

    except:
        pass


@pytest.fixture
def user(app):
    app.auth.add("test", "test")
    yield {
        "Authorization": ("Basic " + base64.b64encode(b"test:test").decode("ascii")),
        "Content-Type": "application/json",
    }
    app.auth.delete("test")


@pytest.fixture
def skip_authz():
    orig = auth.authorize
    auth.authorize = lambda *x: x
    yield
    auth.authorize = orig


@pytest.fixture(scope="function")
def use_mock_authz(request):
    """
    Fixture for enabling mocking of indexd authz system. Returns a function
    that, when called, will override indexd's authz to allow the specified permissions.
    The returned function takes a list of allowed permissions, in the form of
    a list of tuples of (method, resource). If allowed_permissions is not specified,
    all authz requests will succeed.

    Example: Calling `use_mock_authz([("update", "resource_1")])` inside a unit test
    will mock indexd's authz system to allow all requests to update resource_1 to succeed,
    and all other requests will fail, regardless of the user.
    The fixture can be called multiple times in a unit test to change the allowed permissions.
    """

    def _use_mock_authz(allowed_permissions=None):
        # If user does not specify any allowed permissions, authorize all requests
        if not allowed_permissions:
            mock_authz = lambda *x: x
        else:

            def mock_authz(method, resources):
                for r in resources:
                    resource = r.resource
                    if (method, resource) not in allowed_permissions:
                        raise AuthError(
                            "Mock indexd.auth.authz: ({},{}) is not one of the allowed permissions: {}".format(
                                method, resource, allowed_permissions
                            )
                        )

        patched_authz = patch("flask.current_app.auth.authz", mock_authz)
        patched_authz.start()
        request.addfinalizer(patched_authz.stop)

    return _use_mock_authz
