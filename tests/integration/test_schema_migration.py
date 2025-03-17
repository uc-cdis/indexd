import uuid

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy_utils import database_exists, drop_database

from indexd import utils as indexd_utils
from indexd.alias.drivers.alchemy import AliasSchemaVersion
from indexd.index.drivers.alchemy import (
    CURRENT_SCHEMA_VERSION,
    SCHEMA_MIGRATION_FUNCTIONS,
    IndexSchemaVersion,
    SQLAlchemyIndexDriver,
    migrate_1,
    migrate_2,
    migrate_7,
    migrate_12,
)
from tests.integration.alchemy import SQLAlchemyIndexTestDriver
from tests.integration.util import make_sql_statement

Base = declarative_base()

TEST_DB = f"postgresql://{indexd_utils.IndexdConfig['root_auth']}@{indexd_utils.IndexdConfig['host']}/test_migration_db"

INDEX_TABLES = {
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
    "index_record_url": [
        ("did", "character varying"),
        ("url", "character varying"),
    ],
}


def update_version_table_for_testing(conn, table, val):
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (version INT)
        """
    )
    conn.execute(
        f"""
            DELETE FROM {table}
        """
    )
    conn.execute(
        make_sql_statement(
            f"""
            INSERT INTO {table} (version) VALUES (?)
        """,
            (val,),
        )
    )
    conn.commit()


def test_migrate_7(
    index_driver_no_migrate, create_indexd_tables_no_migrate, database_conn
):
    baseid = 1
    did = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    size = 123
    url = "s3://endpointurl/bucket/key"
    ace_key = "acls"
    ace_value = "a,b"
    hash_type = "md5"
    hash_value = "8b9942cf415384b27cadf1f4d2d682e5"

    # Setup the data manually because the schemas and drivers aren't preserved
    # when there is a breaking change.
    database_conn.execute(
        make_sql_statement(
            """
            INSERT INTO index_record (did, size, baseid) VALUES (?, ?, ?)
        """,
            (did, size, baseid),
        )
    )
    database_conn.execute(
        make_sql_statement(
            """
            INSERT INTO index_record_metadata VALUES (?, ?, ?)
        """,
            (ace_key, did, ace_value),
        )
    )
    database_conn.execute(
        make_sql_statement(
            """
            INSERT INTO index_record_url VALUES (?, ?)
        """,
            (did, url),
        )
    )
    database_conn.execute(
        make_sql_statement(
            """
            INSERT INTO index_record_hash VALUES (?, ?, ?)
        """,
            (did, hash_type, hash_value),
        )
    )

    with index_driver_no_migrate.session as session:
        migrate_7(session)

    rows = database_conn.execute(
        """
        SELECT ace
        FROM index_record_ace
    """
    )

    acls = ace_value.split(",")
    for row in rows:
        assert row["ace"] in acls
    rows = database_conn.execute(
        """
        SELECT *
        FROM index_record_metadata
    """
    )

    assert rows.rowcount == 0


def test_migrate_12(
    index_driver_no_migrate, create_indexd_tables_no_migrate, database_conn
):
    """
    Test that the information in the Metadata, and UrlsMetadata table are moved
    to the new UrlsMetadataJsonb table, and the main IndexRecord table.

    Some key:value pairs got moved out into their own columns. These are
    attributes that we use frequently enough to warrant their existence as
    columns.

    urls_metadata[state]     -> UrlsMetadataJsonb.state
    urls_metadata[type]      -> UrlsMetadataJsonb.type
    metadata[release_number] -> IndexRecord.release_number/IndexRecord.index_metadata
    """
    baseid = 1
    dida = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    didb = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
    url = "s3://host/bucket/key"
    url_key1 = "url type"
    url_value1 = "url just ok"
    url_key2 = "url not type"
    url_value2 = "url not just ok"
    u_type = "cleversafe"
    u_state = "validated"

    meta_key1 = "meta type"
    meta_value1 = "meta just ok"
    meta_key2 = "meta not type"
    meta_value2 = "meta not just ok"
    release_number = "12.0"

    # Setup the data manually because the schemas and drivers aren't preserved
    # when there is a breaking change.
    database_conn.execute(
        make_sql_statement(
            """
            INSERT INTO index_record (did, baseid) VALUES (?, ?), (?, ?)
        """,
            (dida, baseid, didb, baseid),
        )
    )
    database_conn.execute(
        make_sql_statement(
            """
            INSERT INTO index_record_url VALUES (?, ?), (?, ?)
        """,
            (dida, url, didb, url),
        )
    )
    database_conn.execute(
        make_sql_statement(
            """
            INSERT INTO index_record_metadata (did, key, value) VALUES
            (?, ?, ?),
            (?, ?, ?),
            (?, ?, ?),
            (?, ?, ?)
        """,
            (
                dida,
                meta_key1,
                meta_value1,
                dida,
                "release_number",
                release_number,
                dida,
                meta_key2,
                meta_value2,
                didb,
                meta_key1,
                meta_value1,
            ),
        )
    )
    database_conn.execute(
        make_sql_statement(
            """
            INSERT INTO index_record_url_metadata (did, url, key, value) VALUES
            (?, ?, ?, ?),
            (?, ?, ?, ?),
            (?, ?, ?, ?),
            (?, ?, ?, ?),
            (?, ?, ?, ?),
            (?, ?, ?, ?)
        """,
            (
                dida,
                url,
                url_key1,
                url_value1,
                dida,
                url,
                url_key2,
                url_value2,
                dida,
                url,
                "type",
                u_type,
                dida,
                url,
                "state",
                u_state,
                didb,
                url,
                "type",
                u_type,
                didb,
                url,
                "state",
                u_state,
            ),
        )
    )

    rows = database_conn.execute(
        """
        SELECT *
        FROM index_record_url_metadata
    """
    )

    # Each key:value pair is on a separate row at this point.
    assert rows.rowcount == 6

    with index_driver_no_migrate.session as session:
        migrate_12(session)

    # Check index table to see if the data transferred from the metadata table.
    rows = database_conn.execute(
        f"""
        SELECT release_number, index_metadata
        FROM index_record
        WHERE did = '{dida}'
    """
    )
    assert rows.rowcount == 1

    for row in rows:
        assert row.release_number == release_number
        assert row.index_metadata == {
            meta_key1: meta_value1,
            meta_key2: meta_value2,
        }

    rows = database_conn.execute(
        f"""
        SELECT release_number, index_metadata
        FROM index_record
        WHERE did = '{didb}'
    """
    )
    assert rows.rowcount == 1

    for row in rows:
        assert row.release_number is None
        assert row.index_metadata == {
            meta_key1: meta_value1,
        }

    # Check url_metadata table to see if the data transferred to the jsonb table.
    rows = database_conn.execute(
        f"""
        SELECT *
        FROM index_record_url_metadata_jsonb
        WHERE did='{dida}'
    """
    )
    assert rows.rowcount == 1

    for row in rows:
        assert row.did == dida
        assert row.url == url
        assert row.state == u_state
        assert row.type == u_type
        assert row.urls_metadata == {
            url_key1: url_value1,
            url_key2: url_value2,
        }

    rows = database_conn.execute(
        f"""
        SELECT *
        FROM index_record_url_metadata_jsonb
        WHERE did='{didb}'
    """
    )
    assert rows.rowcount == 1

    for row in rows:
        assert row.did == didb
        assert row.url == url
        assert row.state == u_state
        assert row.type == u_type
        assert row.urls_metadata is None


def test_migrate_index(index_driver_no_migrate, database_conn):
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

        update_version_table_for_testing(database_conn, "index_schema_version", 0)

        assert len(called) == 2
        with index_driver_no_migrate.session as s:
            v = s.query(IndexSchemaVersion).first()
            assert v.version == 2
            s.delete(v)

    return test_migrate_index_internal


def test_migrate_index_only_diff(index_driver_no_migrate, database_conn):
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

        update_version_table_for_testing(database_conn, "index_schema_version", 0)

        assert len(called) == 1
        assert len(called_2) == 0

        called = []
        called_2 = []
        monkeypatch.setattr("indexd.index.drivers.alchemy.CURRENT_SCHEMA_VERSION", 2)

        update_version_table_for_testing(database_conn, "index_schema_version", 1)
        assert len(called) == 0
        assert len(called_2) == 1

        with index_driver_no_migrate.session as s:
            v = s.query(IndexSchemaVersion).first()
            assert v.version == 2

    return test_migrate_index_only_diff_internal


def test_migrate_alias(alias_driver, database_conn):
    def test_migrate_alias_internal(monkeypatch):
        called = []

        def mock_migrate(**kwargs):
            called.append(True)

        monkeypatch.setattr("indexd.alias.drivers.alchemy.CURRENT_SCHEMA_VERSION", 1)
        monkeypatch.setattr(
            "indexd.alias.drivers.alchemy.SCHEMA_MIGRATION_FUNCTIONS", [mock_migrate]
        )

        monkeypatch.setattr("indexd.utils.check_engine_for_migrate", lambda _: True)

        update_version_table_for_testing(database_conn, "alias_schema_version", 0)

        assert len(called) == 1
        with alias_driver.session as s:
            v = s.query(AliasSchemaVersion).first()
            assert v.version == 1

    return test_migrate_alias_internal


def test_migrate_index_versioning(monkeypatch, index_driver_no_migrate, database_conn):
    engine = create_engine(TEST_DB)
    if database_exists(engine.url):
        drop_database(engine.url)

    driver = SQLAlchemyIndexTestDriver(TEST_DB)
    monkeypatch.setattr("indexd.index.drivers.alchemy.CURRENT_SCHEMA_VERSION", 2)
    monkeypatch.setattr(
        "indexd.index.drivers.alchemy.SCHEMA_MIGRATION_FUNCTIONS",
        [migrate_1, migrate_2],
    )

    monkeypatch.setattr("indexd.utils.check_engine_for_migrate", lambda _: True)

    conn = driver.engine.connect()
    for _ in range(10):
        did = str(uuid.uuid4())
        rev = str(uuid.uuid4())[:8]
        size = 512
        form = "object"
        conn.execute(
            f"""
            INSERT INTO index_record(did, rev, form, size)
            VALUES ('{did}','{rev}','{form}',{size})
        """
        )
    conn.execute("commit")
    conn.close()
    engine.dispose()

    # TODO: unify this in similar databases.
    driver = SQLAlchemyIndexDriver(TEST_DB)
    with driver.session as s:
        v = s.query(IndexSchemaVersion).first()
        assert v.version == 2
        s.delete(v)

    Base.metadata.reflect(bind=driver.engine)
    tables = Base.metadata.tables.keys()

    for table in INDEX_TABLES:
        assert table in tables, f"{table} not created"

    conn = driver.engine.connect()
    for table, schema in INDEX_TABLES.items():
        cols = conn.execute(
            f"\
            SELECT column_name, data_type \
            FROM information_schema.columns \
            WHERE table_schema = 'public' AND table_name = '{table}'"
        )
        assert schema == [i for i in cols]

    vids = conn.execute("SELECT baseid FROM index_record").fetchall()

    for baseid in vids:
        c = conn.execute(
            f"\
            SELECT COUNT(*) AS number_rows \
            FROM index_record \
            WHERE baseid = '{baseid[0]}'"
        ).fetchone()[0]
        assert c == 1
    conn.close()


def test_schema_version():
    assert CURRENT_SCHEMA_VERSION == len(SCHEMA_MIGRATION_FUNCTIONS)
