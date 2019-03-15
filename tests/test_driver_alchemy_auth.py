import hashlib

import pytest

from indexd.auth.drivers.alchemy import SQLAlchemyAuthDriver
from indexd.errors import AuthError
from tests.util import make_sql_statement

USERNAME = 'abc'
PASSWORD = '123'
DIGESTED = SQLAlchemyAuthDriver.digest(PASSWORD)

# TODO check if pytest has utilities for meta-programming of tests

def test_driver_init_does_not_create_records(auth_driver, database_conn):
    """
    Tests for creation of records after driver init.
    Tests driver init does not have unexpected side-effects.
    """
    count = database_conn.execute(
        'SELECT COUNT(*) FROM auth_record').fetchone()[0]

    assert count == 0, 'driver created records upon initilization'


def test_driver_auth_accepts_good_creds(auth_driver, database_conn):
    """
    Tests driver accepts good creds.
    """
    database_conn.execute(make_sql_statement(
        "INSERT INTO auth_record VALUES (?,?)",
        (USERNAME, DIGESTED),
    ))

    auth_driver.auth(USERNAME, PASSWORD)


def test_driver_auth_rejects_bad_creds(auth_driver, database_conn):
    """
    Test driver rejects bad creds.
    """

    database_conn.execute(make_sql_statement(
        """INSERT INTO auth_record VALUES (?,?)""",
        (USERNAME, DIGESTED)))

    with pytest.raises(AuthError):
        auth_driver.auth(USERNAME, 'invalid_' + PASSWORD)

    with pytest.raises(AuthError):
        auth_driver.auth('invalid_' + USERNAME, PASSWORD)


def test_driver_auth_returns_user_context(auth_driver, database_conn):
    """
    Tests driver accepts good creds.
    """

    database_conn.execute(make_sql_statement(
        """INSERT INTO auth_record VALUES (?,?)""", (USERNAME, DIGESTED)))

    user = auth_driver.auth(USERNAME, PASSWORD)

    assert user is not None, 'user context was None'
