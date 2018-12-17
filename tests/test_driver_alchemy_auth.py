import hashlib
import sqlite3

import pytest

import tests.util as util
from indexd.auth.drivers.alchemy import SQLAlchemyAuthDriver
from indexd.errors import AuthError

USERNAME = 'abc'
PASSWORD = '123'
DIGESTED = SQLAlchemyAuthDriver.digest(PASSWORD)

# TODO check if pytest has utilities for meta-programming of tests

def test_driver_init_does_not_create_records(auth_driver):
    """
    Tests for creation of records after driver init.
    Tests driver init does not have unexpected side-effects.
    """
    with sqlite3.connect('auth.sq3') as conn:

        count = conn.execute("""
            SELECT COUNT(*) FROM auth_record
        """).fetchone()[0]

        assert count == 0, 'driver created records upon initilization'


def test_driver_auth_accepts_good_creds(auth_driver):
    """
    Tests driver accepts good creds.
    """
    with sqlite3.connect('auth.sq3') as conn:

        conn.execute("""
            INSERT INTO auth_record VALUES (?,?)
        """, (USERNAME, DIGESTED))

    auth_driver.auth(USERNAME, PASSWORD)


def test_driver_auth_rejects_bad_creds(auth_driver):
    """
    Test driver rejects bad creds.
    """
    with sqlite3.connect('auth.sq3') as conn:

        conn.execute("""
            INSERT INTO auth_record VALUES (?, ?)
        """, (USERNAME, DIGESTED))

    with pytest.raises(AuthError):
        auth_driver.auth(USERNAME, 'invalid_' + PASSWORD)

    with pytest.raises(AuthError):
        auth_driver.auth('invalid_' + USERNAME, PASSWORD)


def test_driver_auth_returns_user_context(auth_driver):
    """
    Tests driver accepts good creds.
    """
    with sqlite3.connect('auth.sq3') as conn:

        conn.execute("""
            INSERT INTO auth_record VALUES (?,?)
        """, (USERNAME, DIGESTED))

    user = auth_driver.auth(USERNAME, PASSWORD)

    assert user is not None, 'user context was None'
