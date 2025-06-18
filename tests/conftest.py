import base64
import datetime
import importlib
import os
import sys

import jwt
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
import datetime

POSTGRES_CONNECTION = "postgresql://postgres:postgres@localhost:5432/indexd_tests"  # pragma: allowlist secret

logger = get_logger(__name__, log_level="info")


def clear_database():
    """
    Clean up test data from unit test
    """
    engine = create_engine(POSTGRES_CONNECTION)

    with engine.connect() as conn:
        index_driver = SQLAlchemyIndexDriver(POSTGRES_CONNECTION)
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
            delete_statement = f"DELETE FROM {table_name}"
            conn.execute(delete_statement)

        # Clear the Alias records
        alias_driver = SQLAlchemyAliasDriver(POSTGRES_CONNECTION)
        for model in alias_base.__subclasses__():
            table = model.__table__
            delete_statement = table.delete()
            conn.execute(delete_statement)

        # Clear the Auth records
        auth_driver = SQLAlchemyAuthDriver(POSTGRES_CONNECTION)
        for model in auth_base.__subclasses__():
            table = model.__table__
            delete_statement = table.delete()
            conn.execute(delete_statement)


@pytest.fixture(scope="function", params=["default_settings", "single_table_settings", "rbac_settings"])
def combined_default_and_single_table_settings(request):
    """
    Fixture to run a unit test with both multi-table and single-table driver
    """

    # Load the default settings
    from indexd import default_settings
    from tests import default_test_settings

    importlib.reload(default_settings)
    importlib.reload(default_test_settings)

    if request.param == "default_settings" or request.param == "rbac_settings":
        default_settings.settings["use_single_table"] = False
        default_settings.settings["config"]["INDEX"] = {
            "driver": SQLAlchemyIndexDriver(
                "postgresql://postgres:postgres@localhost:5432/indexd_tests",  # pragma: allowlist secret
                echo=True,
                index_config={
                    "DEFAULT_PREFIX": "testprefix:",
                    "PREPEND_PREFIX": True,
                    "ADD_PREFIX_ALIAS": False,
                },
            )
        }

        if request.param == "rbac_settings":
            # Load RBAC settings
            default_settings.settings["config"]["RBAC"] = True
            default_test_settings.settings["config"]["RBAC"] = True

    # Load the single-table settings
    elif request.param == "single_table_settings":
        default_settings.settings["use_single_table"] = True
        default_settings.settings["config"]["INDEX"] = {
            "driver": SingleTableSQLAlchemyIndexDriver(
                "postgresql://postgres:postgres@localhost:5432/indexd_tests",  # pragma: allowlist secret
                echo=True,
                index_config={
                    "DEFAULT_PREFIX": "testprefix:",
                    "PREPEND_PREFIX": True,
                    "ADD_PREFIX_ALIAS": False,
                },
            )
        }

    default_settings.settings = {
        **default_settings.settings,
        **default_test_settings.settings,
    }
    from pprint import pprint
    pprint((f"DEBUG {request.param} conftest settings: default_settings", default_settings.settings), stream=sys.stderr)

    yield get_app(default_settings.settings)

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

    yield get_app()

    try:
        clear_database()
        print("DEBUG app cleared database", file=sys.stderr)
    except Exception as e:
        logger.error(f"Failed to clear database with error {e}")


@pytest.fixture
def user(app, combined_default_and_single_table_settings, mock_arborist_requests):

    engine = create_engine(POSTGRES_CONNECTION)
    driver = SQLAlchemyAuthDriver(POSTGRES_CONNECTION)
    try:
        driver.add("test", "test")
    except Exception as e:
        logger.error(f"Failed to add test users with error {e}")

    if "RBAC" in combined_default_and_single_table_settings.config and combined_default_and_single_table_settings.config["RBAC"]:
        print("DEBUG user fixture using RBAC Bearer", file=sys.stderr)
        mock_arborist_requests(
            resource_method_to_authorized={
                "/programs/bpa/projects/UChicago": {"read": True},
                "/programs/other/projects/project": {"read": True},
                "/programs/other/projects/project2": {"read": True},
                "/programs": {"create": True},
                "/services/indexd/admin": {"create": True, "update": True, "delete": True, "read": True, "file_upload": True},
            }
        )
        yield {
            "Authorization": f"Bearer {_user_with_token(app, user)}",
            "Content-Type": "application/json",
        }
    else:
        print("DEBUG user fixture using Basic", file=sys.stderr)
        yield {
            "Authorization": ("Basic " + base64.b64encode(b"test:test").decode("ascii")),
            "Content-Type": "application/json",
        }

    try:
        driver.delete("test")
    except Exception as e:
        logger.error(f"Failed to delete test user with error {e}")

    engine.dispose()


@pytest.fixture
def is_rbac_configured(combined_default_and_single_table_settings):
    """
    Fixture to skip tests that are not compatible with RBAC.
    This is used to mark tests that should only run or skipped when RBAC
    """
    return "RBAC" in combined_default_and_single_table_settings.config and combined_default_and_single_table_settings.config["RBAC"]


def _user_with_token(app, user):
    """
        Fixture to create a user with a token.
        Returns a dictionary with the Authorization header.
        """

    def create_mock_jwt(username="test", secret="test", algorithm="HS256"):
        payload = {
            "sub": username,
            "iat": datetime.datetime.now(datetime.UTC),
            "exp": datetime.datetime.now(datetime.UTC) + datetime.timedelta(hours=1),
        }
        token = jwt.encode(payload, secret, algorithm=algorithm)
        return token

    # Usage
    mock_token = create_mock_jwt()
    return mock_token


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

            if url not in [f"{arborist_base_url}/auth/request", f"{arborist_base_url}/auth/mapping"]:
                print(f"DEBUG make_mock_response bad url {url}", file=sys.stderr)
                mocked_response.status_code = 404
                mocked_response.text = "NOT FOUND"
            elif method != "POST":
                print(f"DEBUG make_mock_response bad method {method}", file=sys.stderr)
                mocked_response.status_code = 405
                mocked_response.text = "METHOD NOT ALLOWED"
            else:
                if url == f"{arborist_base_url}/auth/request":
                    authz_res, authz_met = None, None
                    print(f"DEBUG make_mock_response json {kwargs["json"]}", file=sys.stderr)
                    authz_requests = kwargs["json"]["requests"]
                    if authz_requests:
                        authz_res = kwargs["json"]["requests"][0]["resource"]
                        authz_met = kwargs["json"]["requests"][0]["action"]["method"]
                    authorized = resource_method_to_authorized.get(authz_res, {}).get(
                        authz_met, False
                    )
                    mocked_response.status_code = 200
                    mocked_response.json.return_value = {"auth": authorized}
                    print(
                        f"DEBUG make_mock_response auth/request response: {mocked_response.status_code} for {method} {url} with authz_res={authz_res}, authz_met={authz_met}",
                        file=sys.stderr)
                elif url == f"{arborist_base_url}/auth/mapping":
                    # Mock the auth mapping response
                    mocked_response.status_code = 200
                    mocked_response.json.return_value = {k: [{"service": "*", "method": "read"}, {"service": "*", "method": "read-storage"}] for k in resource_method_to_authorized.keys()}
                    print(
                        f"DEBUG make_mock_response auth/mapping response: {mocked_response.status_code} for {method} {url} with payload {mocked_response.json.return_value}",
                        file=sys.stderr)

            return mocked_response

        mocked_method = mock.MagicMock(side_effect=make_mock_response)
        patch_method = patch(
            "gen3authz.client.arborist.client.httpx.Client.request", mocked_method
        )

        patch_method.start()
        request.addfinalizer(patch_method.stop)

    return do_patch


@pytest.fixture
def rbac_deprecated_tests() -> tuple[list[str], list[str]]:
    """
    Fixture to skip tests that are not compatible with RBAC.
    returns a tuple of lists containing the names of the tests to skip and paths to skip.
    """
    return [], ["tests/postgres/migrations/test_legacy_schema_migration.py",
                "tests/test_deprecated_aliases_endpoints.py"]


@pytest.fixture(autouse=True)
def skip_if_rbac(is_rbac_configured, request, rbac_deprecated_tests):
    """Skip deprecated tests if RBAC is enabled."""
    if "single_table_settings" in request.node.name:
        pytest.skip("Not RBAC compatible, single_table_settings skipping test.")
    if "deprecated" in str(request.node.fspath):
        pytest.skip("Not RBAC compatible, deprecated skipping test.")

    if is_rbac_configured:
        names_to_skip, paths_to_skip = rbac_deprecated_tests
        test_item = request.node  # <--- This is the test item (Function instance)
        if test_item.name in names_to_skip or os.path.relpath(str(test_item.fspath)) in paths_to_skip:
            print(f"Skipping test: {test_item.name} due to RBAC configuration", file=sys.stderr)
            pytest.skip("Not RBAC compatible, skipping test.")
