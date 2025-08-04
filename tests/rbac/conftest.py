import base64
import hashlib
import importlib
import datetime
import uuid
from unittest import mock
from unittest.mock import patch

import pytest
import jwt
import requests
from gen3authz.client.arborist.client import ArboristClient
from sqlalchemy import create_engine

from indexd import get_app
from indexd.auth.drivers.alchemy import SQLAlchemyAuthDriver
from tests.conftest import clear_database, logger, POSTGRES_CONNECTION


@pytest.fixture
def public_authz():
    return "/programs/public/projects/open"


@pytest.fixture
def controlled_authz():
    return "/programs/controlled/projects/closed"


@pytest.fixture
def private_authz():
    return "/programs/private/projects/secret"


@pytest.fixture
def global_discovery_authz():
    return "/indexd/discovery"


@pytest.fixture()
def app_with_rbac():
    from indexd import default_settings
    from tests import default_test_settings

    importlib.reload(default_settings)
    importlib.reload(default_test_settings)

    default_test_settings.settings["config"]["ARE_RECORDS_DISCOVERABLE"] = False
    default_test_settings.settings["config"]["GLOBAL_DISCOVERY_AUTHZ"] = [
        "/indexd/discovery"
    ]

    default_settings.settings = {
        **default_settings.settings,
        **default_test_settings.settings,
    }
    assert default_settings.settings["config"]["ARE_RECORDS_DISCOVERABLE"] is False

    yield get_app(settings=default_settings.settings)

    try:
        clear_database()
    except Exception as e:
        logger.error(f"Failed to clear database with error {e}")

    default_settings.settings["config"]["ARE_RECORDS_DISCOVERABLE"] = True
    default_settings.settings["config"]["GLOBAL_DISCOVERY_AUTHZ"] = []


@pytest.fixture(scope="function")
def mock_arborist_requests(app_with_rbac, request):
    """
    Copied from gen3authz.client.arborist.tests.conftest.py
    This fixture mocks Arborist requests for testing purposes.
    """
    arborist_base_url = "arborist"
    app_with_rbac.auth.arborist = ArboristClient(arborist_base_url=arborist_base_url)

    def do_patch(resource_method_to_authorized={}):
        def make_mock_response(method, url, *args, **kwargs):
            method = method.upper()
            mocked_response = mock.MagicMock(requests.Response)

            if url not in [
                f"{arborist_base_url}/auth/request",
                f"{arborist_base_url}/auth/mapping",
            ]:
                mocked_response.status_code = 404
                mocked_response.text = "NOT FOUND"
            elif method != "POST":
                mocked_response.status_code = 405
                mocked_response.text = "METHOD NOT ALLOWED"
            else:
                if url == f"{arborist_base_url}/auth/request":
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
                elif url == f"{arborist_base_url}/auth/mapping":
                    mocked_response.status_code = 200
                    return_value = {}
                    for k, permissions in resource_method_to_authorized.items():
                        if "read" in permissions and permissions["read"]:
                            return_value[k] = [
                                {"service": "*", "method": "read"},
                                {"service": "*", "method": "read-storage"},
                            ]
                        for p in ["create", "update", "delete", "file_upload"]:
                            if p in permissions and permissions[p]:
                                if k not in return_value:
                                    return_value[k] = []
                                return_value[k].append({"service": "*", "method": p})
                    mocked_response.json.return_value = return_value
            return mocked_response

        mocked_method = mock.MagicMock(side_effect=make_mock_response)
        patch_method = patch(
            "gen3authz.client.arborist.client.httpx.Client.request", mocked_method
        )

        patch_method.start()
        request.addfinalizer(patch_method.stop)

    return do_patch


@pytest.fixture
def client(app_with_rbac):
    with app_with_rbac.test_client() as client:
        yield client


def user_with_token(username="test"):
    def create_mock_jwt(username=username, secret="test", algorithm="HS256"):
        payload = {
            "sub": username,
            "iat": datetime.datetime.now(datetime.timezone.utc),
            "exp": datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=1),
        }
        token = jwt.encode(payload, secret, algorithm=algorithm)
        return token

    mock_token = create_mock_jwt()
    return mock_token


@pytest.fixture
def power_user(mock_arborist_requests, public_authz, controlled_authz, private_authz):
    engine = create_engine(POSTGRES_CONNECTION)
    driver = SQLAlchemyAuthDriver(POSTGRES_CONNECTION)
    try:
        driver.add("test", "test")
    except Exception as e:
        logger.error(f"Failed to add test users with error {e}")

    yield {
        "permissions": {
            public_authz: {
                "read": True,
                "create": True,
                "update": True,
                "delete": True,
            },
            controlled_authz: {
                "read": True,
                "create": True,
                "update": True,
                "delete": True,
            },
            private_authz: {
                "read": True,
                "create": True,
                "update": True,
                "delete": True,
            },
            "/programs": {"create": True},
            "/services/indexd/admin": {
                "create": True,
                "update": True,
                "delete": True,
                "read": True,
                "file_upload": True,
            },
        },
        "header": {
            "Authorization": f"Bearer {user_with_token('power_user')}",
            "Content-Type": "application/json",
            "X-Test-User": "power_user",
        },
    }

    try:
        driver.delete("test")
    except Exception as e:
        logger.error(f"Failed to delete test user with error {e}")

    engine.dispose()


@pytest.fixture
def controlled_user(mock_arborist_requests, public_authz, controlled_authz):
    engine = create_engine(POSTGRES_CONNECTION)
    driver = SQLAlchemyAuthDriver(POSTGRES_CONNECTION)
    try:
        driver.add("test", "test")
    except Exception as e:
        logger.error(f"Failed to add test users with error {e}")

    yield {
        "permissions": {
            public_authz: {
                "read": True,
                "create": True,
                "update": True,
                "delete": True,
            },
            controlled_authz: {
                "read": True,
                "create": True,
                "update": True,
                "delete": True,
            },
            "/programs": {"create": True},
            "/services/indexd/admin": {
                "create": True,
                "update": True,
                "delete": True,
                "read": True,
                "file_upload": True,
            },
        },
        "header": {
            "Authorization": f"Bearer {user_with_token('controlled_user')}",
            "Content-Type": "application/json",
            "X-Test-User": "controlled_user",
        },
    }

    try:
        driver.delete("test")
    except Exception as e:
        logger.error(f"Failed to delete test user with error {e}")

    engine.dispose()


@pytest.fixture
def basic_user(mock_arborist_requests, public_authz):
    engine = create_engine(POSTGRES_CONNECTION)
    driver = SQLAlchemyAuthDriver(POSTGRES_CONNECTION)
    try:
        driver.add("test", "test")
    except Exception as e:
        logger.error(f"Failed to add test users with error {e}")

    yield {
        "permissions": {
            public_authz: {"read": True},
        },
        "header": {
            "Authorization": (
                "Basic " + base64.b64encode(b"test:test").decode("ascii")
            ),
            "Content-Type": "application/json",
            "X-Test-User": "basic_user",
        },
    }

    try:
        driver.delete("test")
    except Exception as e:
        logger.error(f"Failed to delete test user with error {e}")

    engine.dispose()


@pytest.fixture
def null_user(basic_user):
    yield {
        "permissions": basic_user["permissions"],
        "header": {
            "Content-Type": "application/json",
            "X-Test-User": "null_user",
        },
    }


@pytest.fixture
def discovery_user(mock_arborist_requests, global_discovery_authz):
    engine = create_engine(POSTGRES_CONNECTION)
    driver = SQLAlchemyAuthDriver(POSTGRES_CONNECTION)
    try:
        driver.add("test", "test")
    except Exception as e:
        logger.error(f"Failed to add test users with error {e}")

    yield {
        "permissions": {
            global_discovery_authz: {
                "read": True,
            },
        },
        "header": {
            "Authorization": f"Bearer {user_with_token('discovery_user')}",
            "Content-Type": "application/json",
            "X-Test-User": "discovery_user",
        },
    }

    try:
        driver.delete("test")
    except Exception as e:
        logger.error(f"Failed to delete test user with error {e}")

    engine.dispose()


def get_doc(
    authz=None,
    has_metadata=True,
    has_baseid=False,
    has_urls_metadata=False,
    has_version=False,
):
    assert authz, "Authz must be provided"
    if isinstance(authz, str):
        authz = [authz]
    program = project = project_id = None
    for item in authz:
        if item.startswith("/programs/"):
            _ = item.split("/")
            program = _[2]
            project = _[4]
            project_id = f"{program}-{project}"
            break
    assert program and project, "Authz must contain a valid program and project"
    md5_hash = hashlib.md5(project_id.encode("utf-8")).hexdigest()
    doc = {
        "form": "object",
        "size": 123,
        "urls": [f"s3://endpointurl/{program}/{project}/text.txt"],
        "hashes": {"md5": md5_hash},
        "authz": authz,
    }
    if has_metadata:
        doc["metadata"] = {"project_id": project_id}
    if has_baseid:
        doc["baseid"] = str(uuid.UUID(md5_hash + "-baseid"))
    if has_urls_metadata:
        doc["urls_metadata"] = {"s3://endpointurl/bucket/key": {"state": "uploaded"}}
    if has_version:
        doc["version"] = "1"
    return doc


def create_record(client, authz, power_user, mock_arborist_requests):
    mock_arborist_requests(resource_method_to_authorized=power_user["permissions"])
    record = get_doc(authz)
    response = client.post("/index/", json=record, headers=power_user["header"])
    assert (
        response.status_code == 200
    ), f"Failed to create record: {response.text} {power_user['header']}"
    record.update(response.json)
    aliases = {"aliases": [{"value": f"{authz}-alias"}]}
    record.update(aliases)
    response = client.post(
        f"/index/{record['did']}/aliases", json=aliases, headers=power_user["header"]
    )
    assert (
        response.status_code == 200
    ), f"Failed to add alias: {response.text} {power_user['header']}"
    return record


@pytest.fixture
def public_record(client, power_user, public_authz, mock_arborist_requests):
    yield create_record(client, public_authz, power_user, mock_arborist_requests)


@pytest.fixture
def controlled_record(client, power_user, controlled_authz, mock_arborist_requests):
    yield create_record(client, controlled_authz, power_user, mock_arborist_requests)


@pytest.fixture
def private_record(client, power_user, private_authz, mock_arborist_requests):
    yield create_record(client, private_authz, power_user, mock_arborist_requests)


@pytest.fixture
def all_records(public_record, controlled_record, private_record):
    yield {
        "public": public_record,
        "controlled": controlled_record,
        "private": private_record,
    }
