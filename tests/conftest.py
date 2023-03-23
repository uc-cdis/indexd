import base64
import importlib
import pytest
import requests
import mock
from unittest.mock import patch

# indexd_server and indexd_client is needed as fixtures
from cdisutilstest.code.indexd_fixture import clear_database
from gen3authz.client.arborist.client import ArboristClient

from indexd import get_app
from indexd import auth
from indexd.auth.errors import AuthError
from tests import default_test_settings


@pytest.fixture(scope="function", autouse=True)
def app():
    from indexd import default_settings

    importlib.reload(default_settings)
    default_settings.settings = {
        **default_settings.settings,
        **default_test_settings.settings,
    }

    yield get_app()

    try:
        clear_database()
    except Exception:
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
    a list of tuples of (method, resource).
    - If allowed_permissions is not specified, all authz requests will succeed.
    - If allowed_permissions is an empty list, all authz requests will fail.

    Example: Calling `use_mock_authz([("update", "resource_1")])` inside a unit test
    will mock indexd's authz system to allow all requests to update resource_1 to succeed,
    and all other requests will fail, regardless of the user.
    The fixture can be called multiple times in a unit test to change the allowed permissions.

    How is this fixture different from `mock_arborist_requests`? It mocks the
    whole `indexd.auth.authz` logic, while `mock_arborist_requests` only mocks
    the requests to arborist.
    """

    def _use_mock_authz(allowed_permissions=None):
        """
        Args:
            allowed_permissions (list of (string, list) tuples), (optional):
                Only authorize the listed (method, resources) tuples.
                By default, authorize all requests.
        """
        if allowed_permissions is None:
            mock_authz = lambda *x: x
        else:
            assert isinstance(allowed_permissions, list)

            def mock_authz(method, resources):
                for resource in resources:
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


@pytest.fixture(scope="function")
def mock_arborist_requests(app, request):
    """
    This fixture returns a function which you call to mock the call to
    arborist client's auth_request method.
    It returns a 401 error unless the resource and method match the provided
    `resource_method_to_authorized` dict, in which case it returns a 401 error
    or a 200 response depending on the dict value.
    """
    arborist_base_url = "arborist"
    app.auth.arborist = ArboristClient(arborist_base_url=arborist_base_url)

    def do_patch(resource_method_to_authorized={}):
        # Resource/Method to authorized: { RESOURCE: { METHOD: True/False } }

        def make_mock_response(method, url, *args, **kwargs):
            method = method.upper()
            mocked_response = mock.MagicMock(requests.Response)

            if url != f"{arborist_base_url}/auth/request":
                mocked_response.status_code = 404
                mocked_response.text = "NOT FOUND"
            elif method != "POST":
                mocked_response.status_code = 405
                mocked_response.text = "METHOD NOT ALLOWED"
            else:
                authz_res, authz_met = None, None
                authz_requests = kwargs["json"]["requests"]
                if authz_requests:
                    authz_res = kwargs["json"]["requests"][0]["resource"]
                    authz_met = kwargs["json"]["requests"][0]["action"]["method"]
                authorized = resource_method_to_authorized.get(authz_res, {}).get(
                    authz_met, False
                )
                mocked_response.status_code = 200
                mocked_response.json.return_value = {"auth": authorized}
            return mocked_response

        mocked_method = mock.MagicMock(side_effect=make_mock_response)
        patch_method = patch(
            "gen3authz.client.arborist.client.httpx.Client.request", mocked_method
        )

        patch_method.start()
        request.addfinalizer(patch_method.stop)

    return do_patch
