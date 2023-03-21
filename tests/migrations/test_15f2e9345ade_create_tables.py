from alembic.config import main as alembic_main


def test_upgrade(postgres_driver):
    conn = postgres_driver.engine.connect()

    # state before migration
    alembic_main(["--raiseerr", "downgrade", "base"])

    # the database should be empty except for the `alembic_version` table
    tables_res = conn.execute(
        "SELECT * FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema';"
    )
    tables = [i[1] for i in tables_res]
    assert tables == ["alembic_version"]

    # state after migration
    alembic_main(["--raiseerr", "upgrade", "15f2e9345ade"])

    # check that all the tables were created
    tables_res = conn.execute(
        "SELECT * FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema';"
    )
    tables = [i[1] for i in tables_res]
    assert sorted(tables) == sorted(
        [
            "alembic_version",
            # index driver
            "base_version",
            "index_record",
            "drs_bundle_record",
            "index_record_url",
            "index_record_url_metadata",
            "index_record_authz",
            "index_record_alias",
            "index_record_hash",
            "index_schema_version",
            "index_record_metadata",
            "index_record_ace",
            # alias driver
            "alias_record",
            "alias_record_hash",
            "alias_record_host_authority",
            "alias_schema_version",
            # auth driver
            "auth_record",
        ]
    )

    # check one of the tables (`index_record`) to see if the columns were created
    cols = conn.execute(
        "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'index_record'"
    )
    expected_schema = [
        ("did", "character varying"),
        ("baseid", "character varying"),
        ("rev", "character varying"),
        ("form", "character varying"),
        ("size", "bigint"),
        ("created_date", "timestamp without time zone"),
        ("updated_date", "timestamp without time zone"),
        ("file_name", "character varying"),
        ("version", "character varying"),
        ("uploader", "character varying"),
    ]
    assert sorted(expected_schema) == sorted([i for i in cols])


def test_downgrade(postgres_driver):
    conn = postgres_driver.engine.connect()

    # state after migration
    alembic_main(["--raiseerr", "downgrade", "base"])

    # the database should be empty except for the `alembic_version` table
    tables_res = conn.execute(
        "SELECT * FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema';"
    )
    tables = [i[1] for i in tables_res]
    assert tables == ["alembic_version"]


def test_reject_non_alembic_migrations():
    """
    The old migration logic is still supported for backwards compatibility, but any
    new migration should be added using Alembic.
    """
    from indexd.index.drivers.alchemy import CURRENT_SCHEMA_VERSION as INDEX_N_VERSIONS
    from indexd.alias.drivers.alchemy import CURRENT_SCHEMA_VERSION as ALIAS_N_VERSIONS

    # DO NOT EDIT THIS!!!
    assert INDEX_N_VERSIONS <= 13, "New migrations should be added using Alembic"
    assert ALIAS_N_VERSIONS <= 1, "New migrations should be added using Alembic"
