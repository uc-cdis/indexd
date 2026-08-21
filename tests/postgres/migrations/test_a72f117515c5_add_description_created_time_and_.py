from alembic.config import main as alembic_main
import pytest
import asyncio
from sqlalchemy import text


@pytest.mark.asyncio
async def test_upgrade(postgres_driver):
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

    await asyncio.to_thread(alembic_main, ["--raiseerr", "downgrade", "15f2e9345ade"])

    async with postgres_driver.engine.begin() as conn:
        cols = await conn.execute(text(get_columns))
        actual_schema = sorted([i for i in cols])
        assert sorted(expected_schema) == actual_schema

    await asyncio.to_thread(alembic_main, ["--raiseerr", "upgrade", "a72f117515c5"])

    expected_schema += [
        ("description", "character varying"),
        ("content_created_date", "timestamp without time zone"),
        ("content_updated_date", "timestamp without time zone"),
    ]

    async with postgres_driver.engine.begin() as conn:
        cols = await conn.execute(text(get_columns))
        actual_schema = sorted([i for i in cols])
        assert sorted(expected_schema) == actual_schema


@pytest.mark.asyncio
async def test_downgrade(postgres_driver):
    await asyncio.to_thread(alembic_main, ["--raiseerr", "downgrade", "15f2e9345ade"])

    async with postgres_driver.engine.begin() as conn:
        cols = await conn.execute(
            text(
                "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'index_record'"
            )
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
