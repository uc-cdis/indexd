from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.pool import NullPool

from indexd.auth.errors import AuthError, AuthzError
from indexd.auth.drivers.alchemy import SQLAlchemyAuthDriver

POSTGRES_CONNECTION = "postgresql+asyncpg://postgres:postgres@localhost:5432/indexd_tests"  # pragma: allowlist secret

TOKEN = "fake.jwt.token"  # pragma: allowlist secret
ADMIN_RESOURCE = ["/services/indexd/admin"]
PROGRAMS_RESOURCE = ["/programs"]


@pytest.fixture
def jwt_token():
    """
    `get_jwt_token` reads from the request context, which doesn't exist in a
    unit test. Patch it in the module namespace where it was imported.
    """
    with patch(
        "indexd.auth.drivers.alchemy.get_jwt_token", return_value=TOKEN
    ) as mocked:
        yield mocked


@pytest.fixture
def driver():
    """
    Driver with a stubbed-out arborist client.

    NOTE: `auth_request` is called through `asyncio.to_thread`, so the mock must
    be synchronous (MagicMock). An AsyncMock would return a coroutine, which is
    always truthy, and every authorization check would pass vacuously.
    """
    driver = SQLAlchemyAuthDriver(POSTGRES_CONNECTION, poolclass=NullPool)
    driver.arborist = MagicMock()
    return driver


def auth_request_calls(driver):
    """Resources passed to each arborist.auth_request call, in order."""
    return [call.args[3] for call in driver.arborist.auth_request.call_args_list]


@pytest.mark.asyncio
async def test_authz_without_arborist_raises_auth_error(jwt_token):
    """
    No arborist configured => AuthError. This is raised before the try block,
    so it must NOT be wrapped in AuthzError.
    """
    driver = SQLAlchemyAuthDriver(POSTGRES_CONNECTION, poolclass=NullPool)
    assert driver.arborist is None

    with pytest.raises(AuthError, match="Arborist is not configured"):
        await driver.authz("read", ["/programs/A"])

    # should fail before even reaching out for a token
    jwt_token.assert_not_called()


@pytest.mark.asyncio
async def test_authz_authorized_on_resource(driver, jwt_token):
    """
    Arborist authorizes the resource => no exception, no admin fallback.
    """
    driver.arborist.auth_request.return_value = True

    await driver.authz("read", ["/programs/A"])

    driver.arborist.auth_request.assert_called_once_with(
        TOKEN, "indexd", "read", ["/programs/A"]
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "method", ["read", "create", "update", "delete", "file_upload"]
)
async def test_authz_passes_method_through(driver, jwt_token, method):
    """
    The method argument is forwarded to arborist verbatim.
    """
    driver.arborist.auth_request.return_value = True

    await driver.authz(method, ["/programs/A"])

    assert driver.arborist.auth_request.call_args.args[2] == method


@pytest.mark.asyncio
async def test_authz_falls_back_to_admin_resource(driver, jwt_token):
    """
    Denied on the resource but an indexd admin => allowed.
    """
    driver.arborist.auth_request.side_effect = [False, True]

    await driver.authz("delete", ["/programs/A"])

    assert auth_request_calls(driver) == [["/programs/A"], ADMIN_RESOURCE]


@pytest.mark.asyncio
async def test_authz_denied_when_not_admin(driver, jwt_token):
    """
    Denied on both the resource and the admin resource.

    The inner AuthError("Permission denied.") is swallowed by the outer
    `except Exception` and re-raised as AuthzError -- so AuthzError, not
    AuthError, is what callers see.
    """
    driver.arborist.auth_request.return_value = False

    with pytest.raises(AuthzError, match="Permission denied."):
        await driver.authz("delete", ["/programs/A"])

    # non-empty resource => no /programs fallback
    assert auth_request_calls(driver) == [["/programs/A"], ADMIN_RESOURCE]


@pytest.mark.asyncio
async def test_authz_arborist_error_falls_back_to_admin_check(driver, jwt_token):
    """
    A raising arborist call is treated as unauthorized, and the admin check
    still runs.
    """
    driver.arborist.auth_request.side_effect = [Exception("arborist is down"), True]

    await driver.authz("read", ["/programs/A"])

    assert auth_request_calls(driver) == [["/programs/A"], ADMIN_RESOURCE]


@pytest.mark.asyncio
async def test_authz_arborist_error_on_admin_check_raises_authz_error(
    driver, jwt_token
):
    """
    If the admin check itself raises, the error is wrapped in AuthzError.
    """
    driver.arborist.auth_request.side_effect = [False, Exception("arborist is down")]

    with pytest.raises(AuthzError, match="arborist is down"):
        await driver.authz("read", ["/programs/A"])


@pytest.mark.asyncio
@pytest.mark.parametrize("resource", [None, [], ""])
async def test_authz_empty_resource_falls_back_to_programs(driver, jwt_token, resource):
    """
    Deprecated backwards-compat path: with an empty `authz`, access to
    /programs is accepted as admin.
    """
    driver.arborist.auth_request.side_effect = [False, False, True]

    await driver.authz("read", resource)

    assert auth_request_calls(driver) == [resource, ADMIN_RESOURCE, PROGRAMS_RESOURCE]


@pytest.mark.asyncio
async def test_authz_empty_resource_logs_deprecation_warning(driver, jwt_token):
    """
    The /programs admin path warns that it is deprecated.
    """
    driver.arborist.auth_request.side_effect = [False, False, True]

    with patch("indexd.auth.drivers.alchemy.logger") as mock_logger:
        await driver.authz("read", [])

    assert any(
        "deprecated" in call.args[0] for call in mock_logger.warning.call_args_list
    )


@pytest.mark.asyncio
async def test_authz_empty_resource_denied_when_no_admin_access(driver, jwt_token):
    """
    Empty resource and neither admin resource grants access => denied, after
    all three checks.
    """
    driver.arborist.auth_request.return_value = False

    with pytest.raises(AuthzError, match="Permission denied."):
        await driver.authz("read", [])

    assert auth_request_calls(driver) == [[], ADMIN_RESOURCE, PROGRAMS_RESOURCE]


@pytest.mark.asyncio
async def test_authz_empty_resource_admin_skips_programs_fallback(driver, jwt_token):
    """
    If /services/indexd/admin already grants access, the deprecated /programs
    check is skipped.
    """
    driver.arborist.auth_request.side_effect = [False, True]

    await driver.authz("read", [])

    assert PROGRAMS_RESOURCE not in auth_request_calls(driver)


@pytest.mark.asyncio
async def test_authz_token_error_propagates_unwrapped(driver):
    """
    get_jwt_token() is called outside the try block, so its errors are not
    converted to AuthzError.
    """
    with patch(
        "indexd.auth.drivers.alchemy.get_jwt_token",
        side_effect=ValueError("no token in request"),
    ):
        with pytest.raises(ValueError, match="no token in request"):
            await driver.authz("read", ["/programs/A"])

    driver.arborist.auth_request.assert_not_called()
