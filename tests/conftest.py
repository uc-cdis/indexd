from indexd import get_app
import base64
import pytest
from flask import request

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


@pytest.fixture
def use_mock_authz(mock_authz_permissions):
    """
    Fixture for enabling mocking of authz permission system. Takes as a parameter
    an authz configuration dict in the form of:
    ```
    {
        $username: {
            $method: [$resource]
        }
    }
    ```
    This configures which usernames have which permissions.
    The username is read from the HTTP Basic Auth header of incoming requests.
    """
    orig = auth.authorize
    def mock_authorize(method, resources):
        username = request.authorization.username
        user_permissions = mock_authz_permissions.get(username, None)
        if not user_permissions:
            raise AuthError("Permission denied.")
        for resource in resources:
            if resource not in user_permissions.get(method, []):
                raise AuthError("Permission denied.")
    auth.authorize = mock_authorize
    yield
    auth.authorize = orig
