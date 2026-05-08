import base64
import importlib
import pytest
import requests
from sqlalchemy import create_engine
import mock
from unittest.mock import patch

from cdislogging import get_logger

# indexd_server and indexd_client is needed as fixtures
from gen3authz.client.arborist.client import ArboristClient

from indexd import get_app
from indexd import auth
from indexd.auth.errors import AuthError
from indexd.index.drivers.alchemy import Base as index_base
from indexd.auth.drivers.alchemy import Base as auth_base
from indexd.alias.drivers.alchemy import Base as alias_base
from indexd.index.drivers.alchemy import SQLAlchemyIndexDriver
from indexd.alias.drivers.alchemy import SQLAlchemyAliasDriver
from indexd.auth.drivers.alchemy import SQLAlchemyAuthDriver
from indexd.index.drivers.single_table_alchemy import SingleTableSQLAlchemyIndexDriver

from starlette.testclient import TestClient

POSTGRES_CONNECTION = "postgresql://postgres:postgres@localhost:5432/indexd_tests"  # pragma: allowlist secret

logger = get_logger(__name__, log_level="info")


def clear_database():
    """
    Clean up test data from unit test
    """
    engine = create_engine(POSTGRES_CONNECTION)
    with engine.connect() as conn:
        # IndexD table needs to be delete in this order to avoid foreign key constraint error
        table_delete_order = [
            "index_record_url_metadata",
            "index_record_url",
            "index_record_hash",
            "index_record_authz",
            "index_record_ace",
            "index_record_alias",
            "index_record_metadata",
            "alias_record_hash",
            "alias_record_host_authority",
            "alias_record",
            "index_record",
            "drs_bundle_record",
            "base_version",
            "record",
        ]
        for table_name in table_delete_order:
            conn.execute(f"DELETE FROM {table_name}")
        for model in alias_base.__subclasses__():
            conn.execute(model.__table__.delete())
        for model in auth_base.__subclasses__():
            conn.execute(model.__table__.delete())


@pytest.fixture(scope="function", params=["default_settings", "single_table_settings"])
def combined_default_and_single_table_settings(request):
    """
    Fixture to run a unit test with both multi-table and single-table driver
    """
    from indexd import default_settings
    from tests import default_test_settings

    importlib.reload(default_settings)
    importlib.reload(default_test_settings)
    if request.param == "default_settings":
        default_settings.settings["use_single_table"] = False
        default_settings.settings["config"]["INDEX"] = {
            "driver": SQLAlchemyIndexDriver(
                POSTGRES_CONNECTION,
                echo=True,
                index_config={
                    "DEFAULT_PREFIX": "testprefix/",
                    "PREPEND_PREFIX": True,
                    "ADD_PREFIX_ALIAS": False,
                },
            )
        }
    # Load the single-table settings
    elif request.param == "single_table_settings":
        default_settings.settings["use_single_table"] = True
        default_settings.settings["config"]["INDEX"] = {
            "driver": SingleTableSQLAlchemyIndexDriver(
                POSTGRES_CONNECTION,
                echo=True,
                index_config={
                    "DEFAULT_PREFIX": "testprefix/",
                    "PREPEND_PREFIX": True,
                    "ADD_PREFIX_ALIAS": False,
                },
            )
        }
    default_settings.settings = {
        **default_settings.settings,
        **default_test_settings.settings,
    }
    app = get_app(default_settings.settings)
    client = TestClient(app)
    yield client
    try:
        clear_database()
    except Exception as e:
        logger.error(f"Failed to clear database with error {e}")


@pytest.fixture(scope="function", autouse=True)
def app():
    from indexd import default_settings
    from tests import default_test_settings

    importlib.reload(default_settings)
    default_settings.settings = {
        **default_settings.settings,
        **default_test_settings.settings,
    }
    appobj = get_app()
    client = TestClient(appobj)
    yield client
    try:
        clear_database()
    except Exception as e:
        logger.error(f"Failed to clear database with error {e}")


@pytest.fixture
def user(app):
    engine = create_engine(POSTGRES_CONNECTION)
    driver = SQLAlchemyAuthDriver(POSTGRES_CONNECTION)
    try:
        driver.add("test", "test")
    except Exception as e:
        logger.error(f"Failed to add test users with error {e}")
    header = {
        "Authorization": "Basic " + base64.b64encode(b"test:test").decode("ascii"),
        "Content-Type": "application/json",
    }
    yield header
    try:
        driver.delete("test")
    except Exception as e:
        logger.error(f"Failed to delete test user with error {e}")
    engine.dispose()


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
        if allowed_permissions is None:
            mock_authz = lambda *x: x
        else:
            assert isinstance(allowed_permissions, list)

            def mock_authz(method, resources):
                for resource in resources:
                    if (method, resource) not in allowed_permissions:
                        raise AuthError(
                            f"Mock indexd.auth.authz: ({method},{resource}) is not in allowed permissions ({allowed_permissions})"
                        )

        patched_authz = patch("indexd.auth.authz", mock_authz)
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
    app.app.auth.arborist = ArboristClient(arborist_base_url=arborist_base_url)

    def do_patch(resource_method_to_authorized={}):
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
                    authz_res = authz_requests[0]["resource"]
                    authz_met = authz_requests[0]["action"]["method"]
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
