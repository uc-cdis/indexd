from alembic.config import main as alembic_main


def test_upgrade(postgres_driver):
    """
    Ensure the migration correctly adds indexes to the record table
    """
    conn = postgres_driver.engine.connect()

    # Downgrade to previous state before migration
    alembic_main(
        ["--raiseerr", "downgrade", "bb3d7586a096"]  # pragma: allowlist secret
    )

    # Upgrade to apply index changes
    alembic_main(["--raiseerr", "upgrade", "b61e2d4aa93d"])  # pragma: allowlist secret

    # Query to check indexes on the record table
    get_indexes = """
    SELECT indexname, indexdef FROM pg_indexes
    WHERE schemaname = 'public' AND tablename = 'record';
    """

    index_res = conn.execute(get_indexes)
    indexes = {row[0] for row in index_res}

    expected_indexes = {
        "ix_record_size",
        "ix_record_file_name",
        "ix_record_version",
        "ix_record_uploader",
        "ix_record_hashes",
        "ix_record_acl",
        "ix_record_authz",
        "ix_record_urls",
        "ix_record_record_metadata",
        "ix_record_alias",
    }

    assert expected_indexes.issubset(indexes)


def test_downgrade(postgres_driver):
    """
    Ensure the downgrade removes the added indexes from the record table.
    """
    conn = postgres_driver.engine.connect()

    # Apply migration to ensure indexes are created
    alembic_main(["--raiseerr", "upgrade", "b61e2d4aa93d"])  # pragma: allowlist secret

    # Downgrade migration
    alembic_main(
        ["--raiseerr", "downgrade", "bb3d7586a096"]  # pragma: allowlist secret
    )

    # Query to check indexes on the record table
    get_indexes = """
    SELECT indexname FROM pg_indexes
    WHERE schemaname = 'public' AND tablename = 'record';
    """

    index_res = conn.execute(get_indexes)
    indexes = {row[0] for row in index_res}

    expected_indexes = {
        "ix_record_size",
        "ix_record_file_name",
        "ix_record_version",
        "ix_record_uploader",
        "ix_record_hashes",
        "ix_record_acl",
        "ix_record_authz",
        "ix_record_urls",
        "ix_record_record_metadata",
        "ix_record_alias",
    }

    assert not expected_indexes.intersection(indexes)
