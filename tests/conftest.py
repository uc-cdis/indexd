import asyncio
import base64
import importlib
import pytest
import requests

from fastapi import Request, HTTPException
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
import mock
from unittest.mock import patch, AsyncMock, MagicMock
from sqlalchemy.pool import NullPool

from cdislogging import get_logger

from gen3authz.client.arborist.client import ArboristClient

from indexd import get_app
from indexd import auth
from indexd.auth import Auth
from indexd.auth.errors import AuthError
from indexd.index.drivers.alchemy import Base as index_base
from indexd.auth.drivers.alchemy import Base as auth_base
from indexd.alias.drivers.alchemy import Base as alias_base
from indexd.index.drivers.alchemy import SQLAlchemyIndexDriver
from indexd.alias.drivers.alchemy import SQLAlchemyAliasDriver
from indexd.auth.drivers.alchemy import SQLAlchemyAuthDriver
from indexd.index.drivers.single_table_alchemy import SingleTableSQLAlchemyIndexDriver

from starlette.testclient import TestClient

POSTGRES_CONNECTION = "postgresql+asyncpg://postgres:postgres@localhost:5432/indexd_tests"  # pragma: allowlist secret

logger = get_logger(__name__, log_level="info")


def clear_database():
    """
    Clean up test data from unit test natively using the async engine
    """

    async def _async_clear():
        engine = create_async_engine(POSTGRES_CONNECTION, poolclass=NullPool)

        async with engine.begin() as conn:  # Use .begin() for an explicit transaction
            # IndexD table needs to be deleted in this order to avoid foreign key constraint error
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

            # Execute all deletes asynchronously
            for table_name in table_delete_order:
                await conn.execute(text(f"DELETE FROM {table_name}"))

            for model in alias_base.__subclasses__():
                await conn.execute(model.__table__.delete())

            for model in auth_base.__subclasses__():
                await conn.execute(model.__table__.delete())

        await engine.dispose()

    asyncio.run(_async_clear())


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
                poolclass=NullPool,
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
                poolclass=NullPool,
            )
        }

    default_settings.settings["config"]["ALIAS"] = {
        "driver": SQLAlchemyAliasDriver(POSTGRES_CONNECTION, poolclass=NullPool)
    }
    default_settings.settings["auth"] = SQLAlchemyAuthDriver(
        POSTGRES_CONNECTION, poolclass=NullPool
    )

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
    importlib.reload(default_test_settings)

    default_settings.settings = {
        **default_settings.settings,
        **default_test_settings.settings,
    }

    default_settings.settings["config"]["INDEX"] = {
        "driver": SQLAlchemyIndexDriver(
            POSTGRES_CONNECTION,
            echo=False,
            index_config={
                "DEFAULT_PREFIX": "testprefix/",
                "PREPEND_PREFIX": True,
                "ADD_PREFIX_ALIAS": False,
            },
            poolclass=NullPool,
        )
    }
    default_settings.settings["config"]["ALIAS"] = {
        "driver": SQLAlchemyAliasDriver(POSTGRES_CONNECTION, poolclass=NullPool)
    }
    default_settings.settings["auth"] = SQLAlchemyAuthDriver(
        POSTGRES_CONNECTION, poolclass=NullPool
    )

    # Pass the explicitly patched settings into the app!
    app = get_app(default_settings.settings)

    # Explicitly use TestClient as a context manager to trigger FastAPI's lifespan (migrations)
    with TestClient(app) as client:
        yield app, client
        try:
            clear_database()
        except Exception as e:
            logger.error(f"Failed to clear database with error {e}")


@pytest.fixture
def user(app_client):
    """
    Setup a test user using the async driver, but executed synchronously
    so it plays nicely with the standard pytest TestClient.
    """
    driver = SQLAlchemyAuthDriver(POSTGRES_CONNECTION, poolclass=NullPool)

    async def setup_user():
        try:
            await driver.add("test", "test")
        except Exception as e:
            logger.error(f"Failed to add test users with error {e}")

    asyncio.run(setup_user())

    header = {
        "Authorization": "Basic " + base64.b64encode(b"test:test").decode("ascii"),
        "Content-Type": "application/json",
    }

    yield header

    async def teardown_user():
        try:
            await driver.delete("test")
        except Exception as e:
            logger.error(f"Failed to delete test user with error {e}")
        finally:
            await driver.engine.dispose()

    asyncio.run(teardown_user())


@pytest.fixture(scope="function")
def use_mock_authz(app_client, request):
    """
    Fixture for enabling mocking of indexd authz system.
    Updated to directly patch the FastAPI Auth class.
    """
    app, _ = app_client

    def _use_mock_authz(allowed_permissions=None):
        # This fake function perfectly mimics your FastAPI Auth.authorize method
        async def mock_authorize(self_instance, method, resources, throw=True):
            if allowed_permissions is None:
                return True

            # If they didn't provide resources, fail safely
            if not resources:
                if throw:
                    raise HTTPException(status_code=403, detail="Permission denied")
                return False

            # Check if every requested resource is in the allowed list
            for resource in resources:
                if (method, resource) not in allowed_permissions:
                    if throw:
                        raise HTTPException(status_code=403, detail="Permission denied")
                    return False

            return True

        # Intercept the FastAPI dependency directly!
        patched_authz = patch("indexd.auth.Auth.authorize", new=mock_authorize)
        patched_authz.start()
        request.addfinalizer(patched_authz.stop)

    return _use_mock_authz

    return _use_mock_authz


@pytest.fixture(autouse=True, scope="function")
def access_token_patcher(app_client, request):
    app, client = app_client

    async def get_access_token(*args, **kwargs):
        return {"sub": "1", "context": {"user": {"name": "indexd-service-user"}}}

    access_token_mock = MagicMock()
    access_token_mock.return_value = get_access_token

    access_token_patch = patch("indexd.auth.access_token", access_token_mock)
    access_token_patch.start()

    yield access_token_mock

    access_token_patch.stop()


@pytest.fixture(scope="function")
def mock_arborist_requests(request):
    """
    This fixture returns a function which you call to mock the call to
    arborist client's auth_request method.
    By default, it returns a 200 response. If parameter "authorized" is set
    to False, it raises a 401 error.
    """

    def do_patch(authorized=True, resource_method_to_authorized={}):
        # URLs to reponses: { URL: { METHOD: ( content, code ) } }
        resource_method_to_authorized = {
            "http://arborist-service/auth/request": {
                "POST": ({"auth": authorized}, 200)
            },
            "http://arborist-service/auth/mapping": {
                "POST": (
                    {"/": [{"service": "*", "method": "*"}]} if authorized else {},
                    200,
                )
            },
            **resource_method_to_authorized,
        }

        def make_mock_response(method, url, *args, **kwargs):
            method = method.upper()
            mocked_response = MagicMock(requests.Response)

            if url not in resource_method_to_authorized:
                mocked_response.status_code = 404
                mocked_response.text = "NOT FOUND"
            elif method not in resource_method_to_authorized[url]:
                mocked_response.status_code = 405
                mocked_response.text = "METHOD NOT ALLOWED"
            else:
                content, code = resource_method_to_authorized[url][method]
                mocked_response.status_code = code
                if isinstance(content, dict):
                    mocked_response.json.return_value = content
                else:
                    mocked_response.text = content

            return mocked_response

        mocked_method = AsyncMock(side_effect=make_mock_response)
        patch_method = patch(
            "gen3authz.client.arborist.async_client.httpx.AsyncClient.request",
            mocked_method,
        )

        patch_method.start()
        request.addfinalizer(patch_method.stop)

    return do_patch


@pytest.fixture(autouse=True)
def arborist_authorized(mock_arborist_requests):
    """
    By default, mocked arborist calls return Authorized.
    To mock an unauthorized response, use fixture
    "mock_arborist_requests(authorized=False)" in the test itself.
    """
    mock_arborist_requests()


# Import the FastAPI Auth class we created earlier


@pytest.fixture(scope="function")
def skip_authz(app_client):
    app, _ = app_client

    # Create a mock class that matches the Auth dependency signature
    class MockAuth:
        def __init__(self, request: Request = None, basic_credentials=None):
            pass

        async def authorize(self, method: str = None, resources: list = None):
            # Silently pass without checking anything
            return None

    # Tell FastAPI to swap out the real dependency for the mock
    app.dependency_overrides[Auth] = MockAuth

    yield

    # Clean up the override after the test
    app.dependency_overrides.clear()
