"""
Tests for indexd stats table feature.
"""

import datetime
import uuid
import asyncio
import pytest
from unittest.mock import MagicMock

from sqlalchemy import text, select
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.pool import NullPool

from indexd.index.drivers.alchemy import (
    BaseVersion,
    IndexRecord,
    StatsRecord,
    update_stats,
)
from indexd.index.drivers.single_table_alchemy import Record
from indexd.stats_utils import seed_stats, seed_stats_from_connection
from tests.conftest import POSTGRES_CONNECTION


def get_doc(size=123):
    return {
        "form": "object",
        "size": size,
        "urls": ["s3://endpointurl/bucket/key"],
        "hashes": {
            "md5": "8b9942cf415384b27cadf1f4d2d682e5"  # pragma: allowlist secret
        },
    }


async def _get_stats(client):
    """Fetch current stats from the API."""
    res = client.get("/_stats")
    assert res.status_code == 200
    data = res.json()
    count = data["fileCount"]
    size = data["totalFileSize"]
    return count, size


async def _create_record(client, user, size=123):
    """Create a record and return the response json."""
    data = get_doc(size=size)
    res = client.post("/index/", json=data, headers=user)
    assert res.status_code == 200
    return res.json()


async def _delete_record(client, user, did, rev):
    """Delete a record."""
    res = client.delete(
        f"/index/{did}?rev={rev}",
        headers=user,
    )
    assert res.status_code == 200


async def _add_single_table_record(session, size):
    """Insert a minimal single-table Record for reconciliation tests."""
    session.add(Record(guid=str(uuid.uuid4()), size=size, form="object"))


async def _add_index_record(session, size):
    """Insert a minimal IndexRecord for reconciliation tests."""
    bid = str(uuid.uuid4())
    session.add(BaseVersion(baseid=bid))
    await session.flush()
    session.add(
        IndexRecord(
            did=str(uuid.uuid4()),
            baseid=bid,
            rev=str(uuid.uuid4())[:8],
            form="object",
            size=size,
        )
    )


@pytest.mark.asyncio
async def test_stat_updates(
    app_client, user, combined_default_and_single_table_settings
):
    """
    Verify that multiple record creates result in correct stats.
    """
    _, client = app_client
    num_records = 10
    record_size = 50

    for _ in range(num_records):
        await _create_record(client, user, size=record_size)

    final_count, final_size = await _get_stats(client)
    assert final_count == num_records
    assert final_size == num_records * record_size


@pytest.mark.asyncio
async def test_concurrent_stat_updates(
    combined_default_and_single_table_settings,
):
    """
    Test the SELECT FOR UPDATE locking by calling update_stats()
    concurrently using asyncio tasks on the same db.
    """
    engine = create_async_engine(POSTGRES_CONNECTION, poolclass=NullPool)
    AsyncSession = async_sessionmaker(bind=engine, expire_on_commit=False)

    now = datetime.datetime.now()

    async with AsyncSession() as session:
        stmt = select(StatsRecord).filter(
            StatsRecord.month == now.month, StatsRecord.year == now.year
        )
        existing = (await session.execute(stmt)).scalars().first()

        if not existing:
            session.add(
                StatsRecord(
                    total_record_count=0,
                    total_record_bytes=0,
                    month=now.month,
                    year=now.year,
                )
            )
            await session.commit()

        baseline = (await session.execute(stmt)).scalars().first()
        baseline_count = baseline.total_record_count
        baseline_bytes = baseline.total_record_bytes

    num_tasks = 5
    increments_per_task = 4

    async def worker():
        """Each worker increments stats in its own async session."""
        for _ in range(increments_per_task):
            async with AsyncSession() as s:
                try:
                    await update_stats(s, 1, 100)
                    await s.commit()
                except Exception:
                    await s.rollback()
                    raise

    tasks = [worker() for _ in range(num_tasks)]
    await asyncio.gather(*tasks)

    async with AsyncSession() as session:
        stmt = select(StatsRecord).filter(
            StatsRecord.month == now.month, StatsRecord.year == now.year
        )
        row = (await session.execute(stmt)).scalars().first()

        expected_count = baseline_count + (num_tasks * increments_per_task)
        expected_bytes = baseline_bytes + (num_tasks * increments_per_task * 100)

        assert (
            row.total_record_count == expected_count
        ), "Concurrent count update failed"
        assert row.total_record_bytes == expected_bytes, "Concurrent byte update failed"

    await engine.dispose()


@pytest.mark.asyncio
async def test_update_stats_carries_over_from_previous_month(
    combined_default_and_single_table_settings,
):
    """
    When update_stats() runs and the most recent stats row is from a previous
    month, it should create a new row for the current month whose totals are
    the previous row's totals + the new increment.
    """
    engine = create_async_engine(POSTGRES_CONNECTION, poolclass=NullPool)
    AsyncSession = async_sessionmaker(bind=engine, expire_on_commit=False)

    now = datetime.datetime.now()

    async with AsyncSession() as session:
        session.add(
            StatsRecord(
                total_record_count=10,
                total_record_bytes=1000,
                month=1,
                year=2000,
            )
        )
        await session.commit()

    async with AsyncSession() as async_session:
        await update_stats(async_session, 3, 300)
        await async_session.commit()

    async with AsyncSession() as session:
        stmt = select(StatsRecord).filter(
            StatsRecord.month == now.month, StatsRecord.year == now.year
        )
        row = (await session.execute(stmt)).scalars().first()

        assert row is not None, "Failed to create a new row for the current month"
        assert row.total_record_count == 13
        assert row.total_record_bytes == 1300

    await engine.dispose()


@pytest.mark.asyncio
async def test_size_update(
    app_client, user, combined_default_and_single_table_settings
):
    """
    Create a blank record, size=None, then fill it with a size with
    PUT /index/blank/{did}, and verify that stats reflect correctly.
    """
    _, client = app_client

    blank_data = {"uploader": "testuser", "file_name": "test_size_change.txt"}
    res = client.post("/index/blank/", json=blank_data, headers=user)
    assert res.status_code == 201
    blank_rec = res.json()

    after_blank_count, after_blank_size = await _get_stats(client)
    assert after_blank_count == 1
    assert after_blank_size == 0

    fill_data = {
        "size": 250,
        "hashes": {
            "md5": "8b9942cf415384b27cadf1f4d2d682e5"  # pragma: allowlist secret
        },
        "urls": ["s3://endpointurl/bucket/key"],
    }
    res = client.put(
        f"/index/blank/{blank_rec['did']}?rev={blank_rec['rev']}",
        json=fill_data,
        headers=user,
    )
    assert res.status_code == 200

    after_fill_count, after_fill_size = await _get_stats(client)

    assert after_fill_count == 1
    assert after_fill_size == 250


@pytest.mark.asyncio
async def test_historical_queries(
    app_client, user, combined_default_and_single_table_settings
):
    """
    Insert StatsRecord rows for past months, then query
    /_stats?month=X&year=Y and verify correct values.
    """
    _, client = app_client

    engine = create_async_engine(POSTGRES_CONNECTION, poolclass=NullPool)
    AsyncSession = async_sessionmaker(bind=engine, expire_on_commit=False)

    now = datetime.datetime.now()

    past = now - datetime.timedelta(days=180)
    past_month = past.month
    past_year = past.year

    async with AsyncSession() as session:
        session.add(
            StatsRecord(
                total_record_count=42,
                total_record_bytes=999999,
                month=past_month,
                year=past_year,
            )
        )
        await session.commit()
    await engine.dispose()

    res = client.get(f"/_stats?month={past_month}&year={past_year}")
    assert res.status_code == 200
    data = res.json()
    assert data["fileCount"] == 42
    assert data["totalFileSize"] == 999999


@pytest.mark.asyncio
async def test_historical_adjacent_months(
    app_client, combined_default_and_single_table_settings
):
    """
    Insert stats rows for months M-1, M, and M+1 with distinct values.
    Query for month M and verify the correct row is returned.
    """
    _, client = app_client

    engine = create_async_engine(POSTGRES_CONNECTION, poolclass=NullPool)
    AsyncSession = async_sessionmaker(bind=engine, expire_on_commit=False)

    months = [
        (2, 2020, 10, 1000),
        (3, 2020, 20, 2000),
        (4, 2020, 30, 3000),
    ]

    async with AsyncSession() as session:
        for m, y, count, total in months:
            session.add(
                StatsRecord(
                    total_record_count=count,
                    total_record_bytes=total,
                    month=m,
                    year=y,
                )
            )
        await session.commit()
    await engine.dispose()

    res = client.get("/_stats?month=3&year=2020")
    assert res.status_code == 200
    data = res.json()
    assert data["fileCount"] == 20
    assert data["totalFileSize"] == 2000


@pytest.mark.asyncio
async def test_historical_gap_query(
    app_client, combined_default_and_single_table_settings
):
    """
    Insert stats rows for January and March 2020 (skip February).
    Query for February 2020, should return January's row.
    """
    _, client = app_client

    engine = create_async_engine(POSTGRES_CONNECTION, poolclass=NullPool)
    AsyncSession = async_sessionmaker(bind=engine, expire_on_commit=False)

    gap_months = [
        (1, 2020, 50, 5000),
        (3, 2020, 80, 8000),
    ]

    async with AsyncSession() as session:
        for m, y, count, total in gap_months:
            session.add(
                StatsRecord(
                    total_record_count=count,
                    total_record_bytes=total,
                    month=m,
                    year=y,
                )
            )
        await session.commit()
    await engine.dispose()

    res = client.get("/_stats?month=2&year=2020")
    assert res.status_code == 200
    data = res.json()
    assert data["fileCount"] == 50
    assert data["totalFileSize"] == 5000


@pytest.mark.asyncio
async def test_query_before_first_row(
    app_client, combined_default_and_single_table_settings
):
    """
    Insert a stats row for 2020. Querying for 2010 should return counts of 0.
    """
    _, client = app_client

    engine = create_async_engine(POSTGRES_CONNECTION, poolclass=NullPool)
    AsyncSession = async_sessionmaker(bind=engine, expire_on_commit=False)

    async with AsyncSession() as session:
        session.add(
            StatsRecord(
                total_record_count=50,
                total_record_bytes=5000,
                month=1,
                year=2020,
            )
        )
        await session.commit()
    await engine.dispose()

    res = client.get("/_stats?month=2&year=2010")
    assert res.status_code == 200
    data = res.json()
    assert data["fileCount"] == 0
    assert data["totalFileSize"] == 0


@pytest.mark.asyncio
async def test_query_requires_both_month_and_year(
    app_client, combined_default_and_single_table_settings
):
    """
    Verify the API returns an error when only month or only year is provided.
    """
    _, client = app_client
    res = client.get("/_stats?month=6")
    assert res.status_code == 400

    res = client.get("/_stats?year=2025")
    assert res.status_code == 400

    res = client.get("/_stats?month=6&year=2025")
    assert res.status_code == 200


@pytest.mark.asyncio
async def test_stats_empty_table(
    app_client, combined_default_and_single_table_settings
):
    """
    When the stats table is empty, get_stats() should return (0, 0)
    """
    _, client = app_client

    engine = create_async_engine(POSTGRES_CONNECTION, poolclass=NullPool)
    AsyncSession = async_sessionmaker(bind=engine, expire_on_commit=False)

    async with AsyncSession() as session:
        await session.execute(text("DELETE FROM stats"))
        await session.commit()

    res = client.get("/_stats")
    assert res.status_code == 200
    data = res.json()

    assert data["fileCount"] == 0
    assert data["totalFileSize"] == 0

    await engine.dispose()


@pytest.mark.asyncio
async def test_seed_stats_empty_table(combined_default_and_single_table_settings):
    """
    seed_stats on an empty index_record table should create a stats row
    with count=0 and bytes=0.
    """
    engine = create_async_engine(POSTGRES_CONNECTION, poolclass=NullPool)
    AsyncSession = async_sessionmaker(bind=engine, expire_on_commit=False)

    async with AsyncSession() as session:
        count, total_bytes = await seed_stats(session)
        await session.commit()

        assert count == 0
        assert total_bytes == 0

        now = datetime.datetime.now()
        stmt = select(StatsRecord).filter(
            StatsRecord.month == now.month, StatsRecord.year == now.year
        )
        row = (await session.execute(stmt)).scalars().first()

        assert row is not None, "seed_stats should create a row even when counts are 0"
        assert row.total_record_count == 0
        assert row.total_record_bytes == 0

    await engine.dispose()


@pytest.mark.asyncio
async def test_seed_stats_accurate_counts(combined_default_and_single_table_settings):
    """
    seed_stats should report the correct count and total bytes matching
    the index_record table.
    """
    engine = create_async_engine(POSTGRES_CONNECTION, poolclass=NullPool)
    AsyncSession = async_sessionmaker(bind=engine, expire_on_commit=False)

    async with AsyncSession() as session:
        sizes = [100, 250, 650]
        for s in sizes:
            await _add_index_record(session, s)
        await session.commit()

        count, total_bytes = await seed_stats(session)
        await session.commit()

        assert count == 3
        assert total_bytes == 1000

        now = datetime.datetime.now()
        stmt = select(StatsRecord).filter(
            StatsRecord.month == now.month, StatsRecord.year == now.year
        )
        row = (await session.execute(stmt)).scalars().first()

        assert row.total_record_count == 3
        assert row.total_record_bytes == 1000

    await engine.dispose()


@pytest.mark.asyncio
async def test_seed_stats_corrects_drifted_values(
    combined_default_and_single_table_settings,
):
    """
    If the stats table has drifted, seed_stats should overwrite with the correct values.
    """
    engine = create_async_engine(POSTGRES_CONNECTION, poolclass=NullPool)
    AsyncSession = async_sessionmaker(bind=engine, expire_on_commit=False)

    async with AsyncSession() as session:
        await _add_index_record(session, 200)
        await _add_index_record(session, 300)
        await session.commit()

        now = datetime.datetime.now()
        session.add(
            StatsRecord(
                total_record_count=999,
                total_record_bytes=999999,
                month=now.month,
                year=now.year,
            )
        )
        await session.commit()

        count, total_bytes = await seed_stats(session)
        await session.commit()

        assert count == 2
        assert total_bytes == 500

        stmt = select(StatsRecord).filter(
            StatsRecord.month == now.month, StatsRecord.year == now.year
        )
        row = (await session.execute(stmt)).scalars().first()

        assert row.total_record_count == 2
        assert row.total_record_bytes == 500

    await engine.dispose()


@pytest.mark.asyncio
async def test_seed_stats_after_deletions(combined_default_and_single_table_settings):
    """
    If records are deleted but the stats weren't decremented,
    seed_stats should produce correct values.
    """
    engine = create_async_engine(POSTGRES_CONNECTION, poolclass=NullPool)
    AsyncSession = async_sessionmaker(bind=engine, expire_on_commit=False)

    async with AsyncSession() as session:
        await _add_index_record(session, 100)
        await _add_index_record(session, 200)
        await _add_index_record(session, 300)
        await session.commit()

        await seed_stats(session)
        await session.commit()

        now = datetime.datetime.now()
        stmt = select(StatsRecord).filter(
            StatsRecord.month == now.month, StatsRecord.year == now.year
        )
        row = (await session.execute(stmt)).scalars().first()

        assert row.total_record_count == 3
        assert row.total_record_bytes == 600

        await session.execute(text("DELETE FROM index_record"))
        await session.execute(text("DELETE FROM base_version"))
        await session.commit()

        count, total_bytes = await seed_stats(session)
        await session.commit()

        assert count == 0
        assert total_bytes == 0

        row = (await session.execute(stmt)).scalars().first()

        assert row.total_record_count == 0
        assert row.total_record_bytes == 0

    await engine.dispose()


@pytest.mark.asyncio
async def test_seed_stats_from_connection_accurate(
    combined_default_and_single_table_settings,
):
    """
    seed_stats_from_connection should produce
    the same results as the ORM path.
    """
    engine = create_async_engine(POSTGRES_CONNECTION, poolclass=NullPool)
    AsyncSession = async_sessionmaker(bind=engine, expire_on_commit=False)

    async with AsyncSession() as session:
        await _add_index_record(session, 150)
        await _add_index_record(session, 350)
        await session.commit()

        await seed_stats_from_connection(session)
        await session.commit()

        now = datetime.datetime.now()
        stmt = select(StatsRecord).filter(
            StatsRecord.month == now.month, StatsRecord.year == now.year
        )
        row = (await session.execute(stmt)).scalars().first()

        assert row is not None, "seed_stats_from_connection should create a stats row"
        assert row.total_record_count == 2
        assert row.total_record_bytes == 500

    await engine.dispose()


@pytest.mark.asyncio
async def test_seed_stats_from_connection_empty_table(
    combined_default_and_single_table_settings,
):
    """
    seed_stats_from_connection on an empty index_record table should still
    create a row.
    """
    engine = create_async_engine(POSTGRES_CONNECTION, poolclass=NullPool)
    AsyncSession = async_sessionmaker(bind=engine, expire_on_commit=False)

    async with AsyncSession() as session:
        await seed_stats_from_connection(session)
        await session.commit()

        now = datetime.datetime.now()
        stmt = select(StatsRecord).filter(
            StatsRecord.month == now.month, StatsRecord.year == now.year
        )
        row = (await session.execute(stmt)).scalars().first()

        assert (
            row is not None
        ), "seed_stats_from_connection should insert a row when counts are 0"
        assert row.total_record_count == 0
        assert row.total_record_bytes == 0

    await engine.dispose()


@pytest.mark.asyncio
async def test_seed_stats_prefers_active_single_table_data(
    combined_default_and_single_table_settings,
):
    """
    When both storage schemas exist but have different values, seed_stats should
    use the higher record count.
    """
    engine = create_async_engine(POSTGRES_CONNECTION, poolclass=NullPool)
    AsyncSession = async_sessionmaker(bind=engine, expire_on_commit=False)

    async with AsyncSession() as session:
        await _add_index_record(session, 10)

        await _add_single_table_record(session, 100)
        await _add_single_table_record(session, 200)
        await _add_single_table_record(session, 300)
        await session.commit()

        count, total_bytes = await seed_stats(session)
        await session.commit()

        assert count == 3
        assert total_bytes == 600

        now = datetime.datetime.now()
        stmt = select(StatsRecord).filter(
            StatsRecord.month == now.month, StatsRecord.year == now.year
        )
        row = (await session.execute(stmt)).scalars().first()

        assert row.total_record_count == 3
        assert row.total_record_bytes == 600

    await engine.dispose()


@pytest.mark.asyncio
async def test_seed_stats_from_connection_prefers_active_single_table_data(
    combined_default_and_single_table_settings,
):
    """
    The raw connection seeding should also resolve based on record count.
    """
    engine = create_async_engine(POSTGRES_CONNECTION, poolclass=NullPool)
    AsyncSession = async_sessionmaker(bind=engine, expire_on_commit=False)

    async with AsyncSession() as session:
        await _add_index_record(session, 10)
        await _add_index_record(session, 10)
        await _add_index_record(session, 10)
        await _add_single_table_record(session, 50)
        await _add_single_table_record(session, 70)
        await session.commit()

        await seed_stats_from_connection(session)
        await session.commit()

        now = datetime.datetime.now()
        stmt = select(StatsRecord).filter(
            StatsRecord.month == now.month, StatsRecord.year == now.year
        )
        row = (await session.execute(stmt)).scalars().first()

        assert row is not None
        assert row.total_record_count == 3
        assert row.total_record_bytes == 30

    await engine.dispose()


@pytest.mark.asyncio
async def test_index_stats(
    app_client, user, combined_default_and_single_table_settings
):
    """
    create records, verify counts, query future month.
    """
    _, client = app_client

    await _create_record(client, user, size=123)
    await _create_record(client, user, size=77)
    await _create_record(client, user, size=300)
    expected_size = 123 + 77 + 300

    count, size = await _get_stats(client)
    assert count == 3
    assert size == expected_size

    now = datetime.datetime.now()
    future_month = now.month + 1 if now.month < 12 else 1
    future_year = now.year if now.month < 12 else now.year + 1
    res = client.get(f"/_stats?month={future_month}&year={future_year}")
    assert res.status_code == 200
    future_data = res.json()
    assert future_data["fileCount"] == 3
    assert future_data["totalFileSize"] == expected_size


@pytest.mark.asyncio
async def test_stats_decrease_on_delete(
    app_client, user, combined_default_and_single_table_settings
):
    """
    Verify that deleting records correctly decrements stats.
    """
    _, client = app_client

    rec1 = await _create_record(client, user, size=100)
    rec2 = await _create_record(client, user, size=200)
    rec3 = await _create_record(client, user, size=300)

    count, size = await _get_stats(client)
    assert count == 3
    assert size == 600

    await _delete_record(client, user, rec1["did"], rec1["rev"])
    count, size = await _get_stats(client)
    assert count == 2
    assert size == 500

    await _delete_record(client, user, rec2["did"], rec2["rev"])
    await _delete_record(client, user, rec3["did"], rec3["rev"])
    count, size = await _get_stats(client)
    assert count == 0
    assert size == 0
