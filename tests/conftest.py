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
            "stats",
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
    yield get_app(default_settings.settings)
    try:
        clear_database()
    except Exception as e:
        logger.error(f"Failed to clear database with error {e}")


@pytest.fixture(scope="function", autouse=True)
def app_client():
    from indexd import default_settings
    from tests import default_test_settings

    importlib.reload(default_settings)
    default_settings.settings = {
        **default_settings.settings,
        **default_test_settings.settings,
    }
    app = get_app()
    client = TestClient(app)
    yield app, client
    try:
        clear_database()
    except Exception as e:
        logger.error(f"Failed to clear database with error {e}")


@pytest.fixture
def user(app_client):
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


@pytest.fixture(scope="function")
def use_mock_authz(app_client, request):
    """
    Fixture for enabling mocking of indexd authz system. ...
    """
    app, _ = app_client

    def _use_mock_authz(allowed_permissions=None):
        if allowed_permissions is None:
            mock_authz = lambda method, resources: None
        else:
            assert isinstance(allowed_permissions, list)

            def mock_authz(method, resources):
                for resource in resources:
                    if (method, resource) not in allowed_permissions:
                        raise AuthError(
                            f"Mock indexd.auth.authz: ({method},{resource}) is not in allowed permissions ({allowed_permissions})"
                        )

        patched_authz = patch.object(app.auth, "authz", side_effect=mock_authz)
        patched_authz.start()
        request.addfinalizer(patched_authz.stop)

    return _use_mock_authz


@pytest.fixture(scope="function")
def mock_arborist_requests(app_client, request):
    appobj, client = app_client
    arborist_base_url = "arborist"
    appobj.auth.arborist = ArboristClient(arborist_base_url=arborist_base_url)

    # Patch auth once for the entire test
    patched_auth = patch.object(appobj.auth, "auth", return_value=None)
    patched_auth.start()
    request.addfinalizer(patched_auth.stop)

    # Also patch get_jwt_token to avoid token errors in tests
    patched_jwt = patch(
        "indexd.auth.drivers.alchemy.get_jwt_token", return_value="mock_token"
    )
    patched_jwt.start()
    request.addfinalizer(patched_jwt.stop)

    active_arborist_patch = []

    def do_patch(resource_method_to_authorized={}):
        for p in active_arborist_patch:
            p.stop()
        active_arborist_patch.clear()

        def mock_auth_request(token, service, method, resource):
            # resource can be a list or a string
            print(
                f"DEBUG mock_auth_request CALLED: method={method}, resource={resource}"
            )
            print(
                f"DEBUG resource_method_to_authorized={resource_method_to_authorized}"
            )
            resources = resource if isinstance(resource, list) else [resource]
            for res in resources:
                authorized = resource_method_to_authorized.get(res, {}).get(
                    method, False
                )
                if authorized:
                    return True
            return False

        patched_arborist = patch.object(
            appobj.auth.arborist, "auth_request", side_effect=mock_auth_request
        )
        patched_arborist.start()
        active_arborist_patch.append(patched_arborist)

    def stop_all():
        for p in active_arborist_patch:
            p.stop()
        active_arborist_patch.clear()

    request.addfinalizer(stop_all)

    return do_patch


@pytest.fixture
def skip_authz():
    orig = auth.authorize
    auth.authorize = lambda *x: x
    yield
    auth.authorize = orig
