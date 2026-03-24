from alembic.config import main as alembic_main


def test_upgrade(postgres_driver):
    """
    Make sure single table migration created record table and has the correct schema.
    """
    conn = postgres_driver.engine.connect()

    # state before migration
    alembic_main(["--raiseerr", "downgrade", "a72f117515c5"])

    # state after migration
    alembic_main(["--raiseerr", "upgrade", "bb3d7586a096"])  # pragma: allowlist secret

    get_columns = "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'record'"

    expected_schema = [
        ("guid", "character varying"),
        ("baseid", "character varying"),
        ("rev", "character varying"),
        ("form", "character varying"),
        ("size", "bigint"),
        ("created_date", "timestamp without time zone"),
        ("updated_date", "timestamp without time zone"),
        ("file_name", "character varying"),
        ("version", "character varying"),
        ("uploader", "character varying"),
        ("description", "character varying"),
        ("content_created_date", "timestamp without time zone"),
        ("content_updated_date", "timestamp without time zone"),
        ("hashes", "jsonb"),
        ("acl", "ARRAY"),
        ("authz", "ARRAY"),
        ("urls", "ARRAY"),
        ("record_metadata", "jsonb"),
        ("url_metadata", "jsonb"),
        ("alias", "ARRAY"),
    ]

    table_res = conn.execute(get_columns)
    actual_schema = sorted([i for i in table_res])
    assert sorted(expected_schema) == actual_schema


def test_downgrade(postgres_driver):
    """
    Test downgrade to before single table. record table should not exist before this upgrade
    """
    conn = postgres_driver.engine.connect()
    alembic_main(["--raiseerr", "downgrade", "a72f117515c5"])

    # the database should not contain the 'record' table
    tables_res = conn.execute(
        "SELECT * FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema';"
    )
    tables = [i[1] for i in tables_res]
    assert "record" not in tables
