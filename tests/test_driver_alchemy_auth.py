import hashlib
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

import tests.util as util

from indexd.auth.errors import AuthError
from indexd.auth.drivers.alchemy import SQLAlchemyAuthDriver

USERNAME = "abc"
PASSWORD = "123"
DIGESTED = SQLAlchemyAuthDriver.digest(PASSWORD)

POSTGRES_CONNECTION = "postgresql+asyncpg://postgres:postgres@localhost:5432/indexd_tests"  # pragma: allowlist secret


@pytest.mark.asyncio
async def test_driver_init_does_not_create_records():
    """
    Tests for creation of records after driver init.
    Tests driver init does not have unexpected side-effects.
    """
    driver = SQLAlchemyAuthDriver(POSTGRES_CONNECTION, poolclass=NullPool)

    engine = create_async_engine(POSTGRES_CONNECTION, poolclass=NullPool)
    async with engine.begin() as conn:  # .begin() automatically commits the transaction
        # Clean up in case a previous test left data behind
        await conn.execute(text("DELETE FROM auth_record"))

        result = await conn.execute(text("SELECT COUNT(*) FROM auth_record"))
        count = result.scalar()

        assert count == 0, "driver created records upon initilization"

    await engine.dispose()


@pytest.mark.asyncio
async def test_driver_auth_accepts_good_creds():
    """
    Tests driver accepts good creds.
    """
    driver = SQLAlchemyAuthDriver(POSTGRES_CONNECTION, poolclass=NullPool)
    engine = create_async_engine(POSTGRES_CONNECTION, poolclass=NullPool)

    async with engine.begin() as conn:
        await conn.execute(text("DELETE FROM auth_record"))
        await conn.execute(
            text(
                "INSERT INTO auth_record VALUES ('{}', '{}')".format(USERNAME, DIGESTED)
            )
        )

    await driver.auth(USERNAME, PASSWORD)

    await engine.dispose()


@pytest.mark.asyncio
async def test_driver_auth_rejects_bad_creds():
    """
    Test driver rejects bad creds.
    """
    driver = SQLAlchemyAuthDriver(POSTGRES_CONNECTION, poolclass=NullPool)
    engine = create_async_engine(POSTGRES_CONNECTION, poolclass=NullPool)

    async with engine.begin() as conn:
        await conn.execute(text("DELETE FROM auth_record"))
        await conn.execute(
            text(
                "INSERT INTO auth_record VALUES ('{}', '{}')".format(USERNAME, DIGESTED)
            )
        )

    # Pytest will catch the exception raised inside the async block
    with pytest.raises(AuthError):
        await driver.auth(USERNAME, "invalid_" + PASSWORD)

    with pytest.raises(AuthError):
        await driver.auth("invalid_" + USERNAME, PASSWORD)

    await engine.dispose()


@pytest.mark.asyncio
async def test_driver_auth_returns_user_context():
    """
    Tests driver accepts good creds.
    """
    driver = SQLAlchemyAuthDriver(POSTGRES_CONNECTION, poolclass=NullPool)
    engine = create_async_engine(POSTGRES_CONNECTION, poolclass=NullPool)

    async with engine.begin() as conn:
        await conn.execute(text("DELETE FROM auth_record"))
        await conn.execute(
            text(
                "INSERT INTO auth_record VALUES ('{}', '{}')".format(USERNAME, DIGESTED)
            )
        )

    user = await driver.auth(USERNAME, PASSWORD)

    assert user is not None, "user context was None"

    await engine.dispose()
