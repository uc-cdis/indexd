import sqlite3
import hashlib

import pytest

import tests.util as util

from indexd.errors import AuthError

from indexd.auth.drivers.alchemy import SQLAlchemyAuthDriver


USERNAME = "abc"
PASSWORD = "123"
DIGESTED = SQLAlchemyAuthDriver.digest(PASSWORD)

# TODO check if pytest has utilities for meta-programming of tests


@util.removes("auth.sq3")
def test_driver_init_does_not_create_records():
    """
    Tests for creation of records after driver init.
    Tests driver init does not have unexpected side-effects.
    """
    driver = SQLAlchemyAuthDriver(
        "sqlite:///auth.sq3"
    )  # pylint: disable=unused-variable

    with sqlite3.connect("auth.sq3") as conn:
        count = conn.execute(
            """
            SELECT COUNT(*) FROM auth_record
        """
        ).fetchone()[0]

        assert count == 0, "driver created records upon initilization"


@util.removes("auth.sq3")
def test_driver_auth_accepts_good_creds():
    """
    Tests driver accepts good creds.
    """
    driver = SQLAlchemyAuthDriver("sqlite:///auth.sq3")

    with sqlite3.connect("auth.sq3") as conn:
        conn.execute(
            """
            INSERT INTO auth_record VALUES (?,?)
        """,
            (USERNAME, DIGESTED),
        )

    driver.auth(USERNAME, PASSWORD)


@util.removes("auth.sq3")
def test_driver_auth_rejects_bad_creds():
    """
    Test driver rejects bad creds.
    """
    driver = SQLAlchemyAuthDriver("sqlite:///auth.sq3")

    with sqlite3.connect("auth.sq3") as conn:
        conn.execute(
            """
            INSERT INTO auth_record VALUES (?, ?)
        """,
            (USERNAME, DIGESTED),
        )

    with pytest.raises(AuthError):
        driver.auth(USERNAME, "invalid_" + PASSWORD)

    with pytest.raises(AuthError):
        driver.auth("invalid_" + USERNAME, PASSWORD)


@util.removes("auth.sq3")
def test_driver_auth_returns_user_context():
    """
    Tests driver accepts good creds.
    """
    driver = SQLAlchemyAuthDriver("sqlite:///auth.sq3")

    with sqlite3.connect("auth.sq3") as conn:
        conn.execute(
            """
            INSERT INTO auth_record VALUES (?,?)
        """,
            (USERNAME, DIGESTED),
        )

    user = driver.auth(USERNAME, PASSWORD)

    assert user is not None, "user context was None"
