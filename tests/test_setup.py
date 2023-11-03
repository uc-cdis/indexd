import sqlite3

import tests.util as util

from indexd.index.drivers.alchemy import SQLAlchemyIndexDriver
from indexd.alias.drivers.alchemy import SQLAlchemyAliasDriver


OLD_SQLITE = sqlite3.sqlite_version_info < (3, 7, 16)

INDEX_HOST = "index.sq3"
ALIAS_HOST = "alias.sq3"

INDEX_TABLES = {
    "base_version": [(0, "baseid", "VARCHAR", 1, None, 1)],
    "index_record": [
        (0, "did", "VARCHAR", 1, None, 1),
        (1, "baseid", "VARCHAR", 0, None, 0),
        (2, "rev", "VARCHAR", 0, None, 0),
        (3, "form", "VARCHAR", 0, None, 0),
        (4, "size", "BIGINT", 0, None, 0),
        (5, "created_date", "DATETIME", 0, None, 0),
        (6, "updated_date", "DATETIME", 0, None, 0),
        (7, "file_name", "VARCHAR", 0, None, 0),
        (8, "version", "VARCHAR", 0, None, 0),
        (9, "uploader", "VARCHAR", 0, None, 0),
        (10, "description", "VARCHAR", 0, None, 0),
        (11, "content_created_date", "DATETIME", 0, None, 0),
        (12, "content_updated_date", "DATETIME", 0, None, 0),
    ],
    "index_record_hash": [
        (0, "did", "VARCHAR", 1, None, 1),
        (1, "hash_type", "VARCHAR", 1, None, 1 if OLD_SQLITE else 2),
        (2, "hash_value", "VARCHAR", 0, None, 0),
    ],
    "index_record_url": [
        (0, "did", "VARCHAR", 1, None, 1),
        (1, "url", "VARCHAR", 1, None, 1 if OLD_SQLITE else 2),
    ],
    "index_schema_version": [(0, "version", "INTEGER", 1, None, 1)],
    "drs_bundle_record": [
        (0, "bundle_id", "VARCHAR", 1, None, 1),
        (1, "name", "VARCHAR", 0, None, 0),
        (2, "created_time", "DATETIME", 0, None, 0),
        (3, "updated_time", "DATETIME", 0, None, 0),
        (4, "checksum", "VARCHAR", 0, None, 0),
        (5, "size", "BIGINT", 0, None, 0),
        (6, "bundle_data", "TEXT", 0, None, 0),
        (7, "description", "TEXT", 0, None, 0),
        (8, "version", "VARCHAR", 0, None, 0),
        (9, "aliases", "VARCHAR", 0, None, 0),
    ],
}

ALIAS_TABLES = {
    "alias_record": [
        (0, "name", "VARCHAR", 1, None, 1),
        (1, "rev", "VARCHAR", 0, None, 0),
        (2, "size", "BIGINT", 0, None, 0),
        (3, "release", "VARCHAR", 0, None, 0),
        (4, "metastring", "VARCHAR", 0, None, 0),
        (5, "keeper_authority", "VARCHAR", 0, None, 0),
    ],
    "alias_record_hash": [
        (0, "name", "VARCHAR", 1, None, 1),
        (1, "hash_type", "VARCHAR", 1, None, 1 if OLD_SQLITE else 2),
        (2, "hash_value", "VARCHAR", 0, None, 0),
    ],
    "alias_record_host_authority": [
        (0, "name", "VARCHAR", 1, None, 1),
        (1, "host", "VARCHAR", 1, None, 1 if OLD_SQLITE else 2),
    ],
    "alias_schema_version": [(0, "version", "INTEGER", 1, None, 1)],
}

INDEX_CONFIG = {"driver": SQLAlchemyIndexDriver("sqlite:///index.sq3")}

ALIAS_CONFIG = {"driver": SQLAlchemyAliasDriver("sqlite:///alias.sq3")}


@util.removes(INDEX_HOST)
def test_sqlite3_index_setup_tables():
    """
    Tests that the SQLite3 index database gets set up correctly.
    """
    SQLAlchemyIndexDriver("sqlite:///index.sq3")

    with sqlite3.connect(INDEX_HOST) as conn:
        c = conn.execute(
            """
            SELECT name FROM sqlite_master WHERE type = 'table'
        """
        )

        tables = [i[0] for i in c]

        for table in INDEX_TABLES:
            assert table in tables, "{table} not created".format(table=table)

        for table, schema in list(INDEX_TABLES.items()):
            # NOTE PRAGMA's don't work with parameters...
            c = conn.execute(
                """
                PRAGMA table_info ('{table}')
            """.format(
                    table=table
                )
            )

            assert schema == [i for i in c]


@util.removes(ALIAS_HOST)
def test_sqlite3_alias_setup_tables():
    """
    Tests that the SQLite3 alias database gets set up correctly.
    """
    SQLAlchemyAliasDriver("sqlite:///alias.sq3")

    with sqlite3.connect(ALIAS_HOST) as conn:
        c = conn.execute(
            """
            SELECT name FROM sqlite_master WHERE type = 'table'
        """
        )

        tables = [i[0] for i in c]

        for table in ALIAS_TABLES:
            assert table in tables, "{table} not created".format(table=table)

        for table, schema in list(ALIAS_TABLES.items()):
            # NOTE PRAGMA's don't work with parameters...
            c = conn.execute(
                """
                PRAGMA table_info ('{table}')
            """.format(
                    table=table
                )
            )

            assert schema == [i for i in c]
