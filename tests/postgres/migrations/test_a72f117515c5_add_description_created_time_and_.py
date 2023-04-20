from alembic.config import main as alembic_main


def test_upgrade(postgres_driver):
    conn = postgres_driver.engine.connect()
    get_columns = "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'index_record'"

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

    alembic_main(["--raiseerr", "downgrade", "15f2e9345ade"])
    cols = conn.execute(get_columns)
    actual_schema = sorted([i for i in cols])
    assert sorted(expected_schema) == actual_schema

    alembic_main(["--raiseerr", "upgrade", "a72f117515c5"])
    cols = conn.execute(get_columns)

    expected_schema += [
        ("description", "character varying"),
        ("content_created_date", "timestamp without time zone"),
        ("content_updated_date", "timestamp without time zone"),
    ]

    actual_schema = sorted([i for i in cols])
    assert sorted(expected_schema) == actual_schema


def test_downgrade(postgres_driver):
    conn = postgres_driver.engine.connect()
    alembic_main(["--raiseerr", "downgrade", "15f2e9345ade"])
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
    actual_schema = sorted([i for i in cols])
    assert sorted(expected_schema) == actual_schema
