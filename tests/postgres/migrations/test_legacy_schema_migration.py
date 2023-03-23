"""
These are tests for the old migration logic, which is DEPRECATED. It is still supported
for backwards compatibility, but any new migration should be added using Alembic.
"""


from sqlalchemy.ext.declarative import declarative_base
import sqlite3
import tests.util as util
from indexd.index.drivers.alchemy import (
    SQLAlchemyIndexDriver,
    IndexSchemaVersion,
    migrate_7,
)

from indexd.alias.drivers.alchemy import SQLAlchemyAliasDriver, AliasSchemaVersion
from indexd.index.drivers.alchemy import (
    CURRENT_SCHEMA_VERSION,
    SCHEMA_MIGRATION_FUNCTIONS,
)

Base = declarative_base()

INDEX_TABLES = {
    "base_version": [("baseid", "character varying")],
    "index_record": [
        ("did", "character varying"),
        ("rev", "character varying"),
        ("form", "character varying"),
        ("size", "bigint"),
        ("baseid", "character varying"),
        ("created_date", "timestamp without time zone"),
        ("updated_date", "timestamp without time zone"),
    ],
    "index_record_hash": [
        ("did", "character varying"),
        ("hash_type", "character varying"),
        ("hash_value", "character varying"),
    ],
    "index_record_url": [("did", "character varying"), ("url", "character varying")],
    "drs_bundle_record": [
        ("bundle_id", "character varying"),
        ("name", "character varying"),
        ("created_time", "timestamp without time zone"),
        ("updated_time", "timestamp without time zone"),
        ("checksum", "character varying"),
        ("size", "bigint"),
        ("bundle_data", "text"),
        ("description", "text"),
        ("version", "character varying"),
        ("aliases", "character varying"),
    ],
}


def update_version_table_for_testing(db, tb_name, val):
    with sqlite3.connect(db) as conn:
        conn.execute(
            """\
            CREATE TABLE IF NOT EXISTS {} (version INT)\
            """.format(
                tb_name
            )
        )
        conn.execute(
            """
                DELETE FROM {}
            """.format(
                tb_name
            )
        )
        conn.execute(
            """
                INSERT INTO {} (version) VALUES ({})
            """.format(
                tb_name, val
            )
        )
        conn.commit()


def test_migrate_acls(client, user, postgres_driver):
    data = {
        "form": "object",
        "size": 123,
        "urls": ["s3://endpointurl/bucket/key"],
        "metadata": {"acls": "a,b"},
        "hashes": {"md5": "8b9942cf415384b27cadf1f4d2d682e5"},
    }

    # create the record
    res = client.post("/index/", json=data, headers=user)
    rec = res.json
    assert res.status_code == 200

    # migrate
    with postgres_driver.session as session:
        migrate_7(session)

    # check that the record has been migrated
    res = client.get("/" + rec["did"])
    rec = res.json
    assert res.status_code == 200
    assert rec["acl"] == ["a", "b"]
    assert rec["metadata"] == {}


@util.removes("index.sq3")
def test_migrate_index():
    def test_migrate_index_internal(monkeypatch):
        called = []

        def mock_migrate(**kwargs):
            called.append(True)

        monkeypatch.setattr("indexd.index.drivers.alchemy.CURRENT_SCHEMA_VERSION", 2)
        monkeypatch.setattr("indexd.utils.check_engine_for_migrate", lambda _: True)

        monkeypatch.setattr(
            "indexd.index.drivers.alchemy.SCHEMA_MIGRATION_FUNCTIONS",
            [mock_migrate, mock_migrate],
        )

        update_version_table_for_testing("index.sq3", "index_schema_version", 0)
        driver = SQLAlchemyIndexDriver("sqlite:///index.sq3")

        assert len(called) == 2
        with driver.session as s:
            v = s.query(IndexSchemaVersion).first()
            assert v.version == 2
            s.delete(v)

    return test_migrate_index_internal


@util.removes("index.sq3")
def test_migrate_index_only_diff():
    def test_migrate_index_only_diff_internal(monkeypatch):
        called = []

        def mock_migrate(**kwargs):
            called.append(True)

        called_2 = []

        def mock_migrate_2(**kwargs):
            called_2.append(True)

        monkeypatch.setattr("indexd.utils.check_engine_for_migrate", lambda _: True)
        monkeypatch.setattr("indexd.index.drivers.alchemy.CURRENT_SCHEMA_VERSION", 1)
        monkeypatch.setattr(
            "indexd.index.drivers.alchemy.SCHEMA_MIGRATION_FUNCTIONS",
            [mock_migrate, mock_migrate_2],
        )

        update_version_table_for_testing("index.sq3", "index_schema_version", 0)

        driver = SQLAlchemyIndexDriver("sqlite:///index.sq3")
        assert len(called) == 1
        assert len(called_2) == 0

        called = []
        called_2 = []
        monkeypatch.setattr("indexd.index.drivers.alchemy.CURRENT_SCHEMA_VERSION", 2)

        update_version_table_for_testing("index.sq3", "index_schema_version", 1)
        driver = SQLAlchemyIndexDriver("sqlite:///index.sq3")
        assert len(called) == 0
        assert len(called_2) == 1

        with driver.session as s:
            v = s.query(IndexSchemaVersion).first()
            assert v.version == 2

    return test_migrate_index_only_diff_internal


@util.removes("alias.sq3")
def test_migrate_alias():
    def test_migrate_alias_internal(monkeypatch):
        called = []

        def mock_migrate(**kwargs):
            called.append(True)

        monkeypatch.setattr("indexd.alias.drivers.alchemy.CURRENT_SCHEMA_VERSION", 1)
        monkeypatch.setattr(
            "indexd.alias.drivers.alchemy.SCHEMA_MIGRATION_FUNCTIONS", [mock_migrate]
        )

        monkeypatch.setattr("indexd.utils.check_engine_for_migrate", lambda _: True)

        update_version_table_for_testing("alias.sq3", "alias_schema_version", 0)

        driver = SQLAlchemyAliasDriver("sqlite:///alias.sq3")
        assert len(called) == 1
        with driver.session as s:
            v = s.query(AliasSchemaVersion).first()
            assert v.version == 1

    return test_migrate_alias_internal


def test_schema_version():
    assert CURRENT_SCHEMA_VERSION == len(SCHEMA_MIGRATION_FUNCTIONS)
