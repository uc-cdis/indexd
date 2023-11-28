import hashlib

import pytest
from sqlalchemy import create_engine

import tests.util as util

from indexd.errors import AuthError

from indexd.auth.drivers.alchemy import SQLAlchemyAuthDriver


USERNAME = "abc"
PASSWORD = "123"
DIGESTED = SQLAlchemyAuthDriver.digest(PASSWORD)
POSTGRES_CONNECTION = "postgres://postgres:postgres@localhost:5432/indexd_tests"  # pragma: allowlist secret

# TODO check if pytest has utilities for meta-programming of tests


def test_driver_init_does_not_create_records():
    """
    Tests for creation of records after driver init.
    Tests driver init does not have unexpected side-effects.
    """

    engine = create_engine(POSTGRES_CONNECTION)
    with engine.connect() as conn:
        result = conn.execute("SELECT COUNT(*) FROM auth_record")
        count = result.scalar()

        assert count == 0, "driver created records upon initilization"


def test_driver_auth_accepts_good_creds():
    """
    Tests driver accepts good creds.
    """

    driver = SQLAlchemyAuthDriver(POSTGRES_CONNECTION)
    engine = create_engine(POSTGRES_CONNECTION)
    with engine.connect() as conn:
        result = conn.execute(
            "INSERT INTO auth_record VALUES ('{}', '{}')".format(USERNAME, DIGESTED)
        )

    driver.auth(USERNAME, PASSWORD)


def test_driver_auth_rejects_bad_creds():
    """
    Test driver rejects bad creds.
    """
    driver = SQLAlchemyAuthDriver(
        "postgres://postgres:postgres@localhost:5432/indexd_tests"  # pragma: allowlist secret
    )

    engine = create_engine(POSTGRES_CONNECTION)

    with engine.connect() as conn:
        result = conn.execute(
            "INSERT INTO auth_record VALUES ('{}', '{}')".format(USERNAME, DIGESTED)
        )

    with pytest.raises(AuthError):
        driver.auth(USERNAME, "invalid_" + PASSWORD)

    with pytest.raises(AuthError):
        driver.auth("invalid_" + USERNAME, PASSWORD)


def test_driver_auth_returns_user_context():
    """
    Tests driver accepts good creds.
    """
    driver = SQLAlchemyAuthDriver(
        "postgres://postgres:postgres@localhost:5432/indexd_tests"  # pragma: allowlist secret
    )

    engine = create_engine(POSTGRES_CONNECTION)

    with engine.connect() as conn:
        result = conn.execute(
            "INSERT INTO auth_record VALUES ('{}', '{}')".format(USERNAME, DIGESTED)
        )

    user = driver.auth(USERNAME, PASSWORD)

    assert user is not None, "user context was None"
