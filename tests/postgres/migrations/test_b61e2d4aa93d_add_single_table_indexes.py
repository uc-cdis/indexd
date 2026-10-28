from alembic.config import main as alembic_main
import pytest
import asyncio
from sqlalchemy import text


@pytest.mark.asyncio
async def test_upgrade(postgres_driver):
    """
    Ensure the migration correctly adds indexes to the record table
    """
    await asyncio.to_thread(
        alembic_main,
        ["--raiseerr", "downgrade", "bb3d7586a096"],  # pragma: allowlist secret
    )

    await asyncio.to_thread(
        alembic_main,
        ["--raiseerr", "upgrade", "b61e2d4aa93d"],  # pragma: allowlist secret
    )

    async with postgres_driver.engine.begin() as conn:
        # Query to check indexes on the record table
        get_indexes = """
        SELECT indexname, indexdef FROM pg_indexes
        WHERE schemaname = 'public' AND tablename = 'record';
        """

        index_res = await conn.execute(text(get_indexes))
        indexes = {row[0] for row in index_res}

        expected_indexes = {
            "ix_record_size",
            "ix_record_hashes",
        }

        assert expected_indexes.issubset(indexes)


@pytest.mark.asyncio
async def test_downgrade(postgres_driver):
    """
    Ensure the downgrade removes the added indexes from the record table.
    """
    await asyncio.to_thread(
        alembic_main,
        ["--raiseerr", "upgrade", "b61e2d4aa93d"],  # pragma: allowlist secret
    )

    await asyncio.to_thread(
        alembic_main,
        ["--raiseerr", "downgrade", "bb3d7586a096"],  # pragma: allowlist secret
    )

    async with postgres_driver.engine.begin() as conn:
        # Query to check indexes on the record table
        get_indexes = """
        SELECT indexname FROM pg_indexes
        WHERE schemaname = 'public' AND tablename = 'record';
        """

        index_res = await conn.execute(text(get_indexes))
        indexes = {row[0] for row in index_res}

        expected_indexes = {
            "ix_record_size",
            "ix_record_hashes",
        }

        assert not expected_indexes.intersection(indexes)
