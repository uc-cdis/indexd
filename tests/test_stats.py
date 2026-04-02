"""
Tests for indexd stats table feature.
"""

import datetime
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from unittest.mock import MagicMock

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from indexd.index.drivers.alchemy import (
    BaseVersion,
    IndexRecord,
    StatsRecord,
    update_stats,
)
from indexd.stats_table_migration import seed_stats, seed_stats_from_connection
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


def _get_stats(client):
    """Fetch current stats from the API."""
    res = client.get("/_stats/")
    assert res.status_code == 200
    data = res.json
    count = data["fileCount"]
    size = data["totalFileSize"]
    return count, size


def _create_record(client, user, size=123):
    """Create a record and return the response json."""
    data = get_doc(size=size)
    res = client.post("/index/", json=data, headers=user)
    assert res.status_code == 200
    return res.json


def _delete_record(client, user, did, rev):
    """Delete a record."""
    res = client.delete(
        f"/index/{did}?rev={rev}",
        headers=user,
    )
    assert res.status_code == 200


def test_stat_updates(client, user, combined_default_and_single_table_settings):
    """
    Verify that multiple record creates result in correct stats.
    """
    num_records = 10
    record_size = 50

    for _ in range(num_records):
        _create_record(client, user, size=record_size)

    final_count, final_size = _get_stats(client)
    assert final_count == num_records
    assert final_size == num_records * record_size


def test_concurrent_stat_updates(
    combined_default_and_single_table_settings,
):
    """
    Test the SELECT FOR UPDATE locking by calling update_stats()
    concurrently from multiple threads on the same db.
    """
    engine = create_engine(POSTGRES_CONNECTION)
    Session = sessionmaker(bind=engine)

    # Ensure a row exists to force update logic
    session = Session()
    now = datetime.datetime.now()
    existing = (
        session.query(StatsRecord)
        .filter(StatsRecord.month == now.month, StatsRecord.year == now.year)
        .first()
    )
    if not existing:
        session.add(
            StatsRecord(
                total_record_count=0,
                total_record_bytes=0,
                month=now.month,
                year=now.year,
            )
        )
        session.commit()
    session.close()

    # Record baseline
    session = Session()
    baseline = (
        session.query(StatsRecord)
        .filter(StatsRecord.month == now.month, StatsRecord.year == now.year)
        .first()
    )
    baseline_count = baseline.total_record_count
    baseline_bytes = baseline.total_record_bytes
    session.close()

    num_threads = 5
    increments_per_thread = 4

    def worker():
        """Each worker increments stats in its own session."""
        for _ in range(increments_per_thread):
            s = Session()
            try:
                update_stats(s, 1, 100)
                s.commit()
            except Exception:
                s.rollback()
                raise
            finally:
                s.close()

    with ThreadPoolExecutor(max_workers=num_threads) as pool:
        futures = [pool.submit(worker) for _ in range(num_threads)]
        for f in as_completed(futures):
            f.result()

    # Verify totals
    session = Session()
    row = (
        session.query(StatsRecord)
        .filter(StatsRecord.month == now.month, StatsRecord.year == now.year)
        .first()
    )
    expected_count = baseline_count + (num_threads * increments_per_thread)
    expected_bytes = baseline_bytes + (num_threads * increments_per_thread * 100)
    assert row.total_record_count == expected_count
    assert row.total_record_bytes == expected_bytes
    session.close()
    engine.dispose()


def test_update_stats_carries_over_from_previous_month(
    combined_default_and_single_table_settings,
):
    """
    When update_stats() runs and the most recent stats row is from a previous
    month, it should create a new row for the current month whose totals are
    the previous row's totals + the new increment.
    """
    engine = create_engine(POSTGRES_CONNECTION)
    Session = sessionmaker(bind=engine)
    session = Session()

    now = datetime.datetime.now()

    session.add(
        StatsRecord(
            total_record_count=10,
            total_record_bytes=1000,
            month=1,
            year=2000,
        )
    )
    session.commit()

    update_stats(session, 3, 300)
    session.commit()

    row = (
        session.query(StatsRecord)
        .filter(StatsRecord.month == now.month, StatsRecord.year == now.year)
        .first()
    )
    assert row is not None
    assert row.total_record_count == 13
    assert row.total_record_bytes == 1300

    session.close()
    engine.dispose()


def test_size_update(client, user, combined_default_and_single_table_settings):
    """
    Create a blank record, size=None, then fill it with a size with
    PUT /index/blank/{did}, and verify that stats reflect correctly.
    """
    blank_data = {"uploader": "testuser", "file_name": "test_size_change.txt"}
    res = client.post("/index/blank/", json=blank_data, headers=user)
    assert res.status_code == 201
    blank_rec = res.json

    after_blank_count, after_blank_size = _get_stats(client)
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

    after_fill_count, after_fill_size = _get_stats(client)

    assert after_fill_count == 1
    assert after_fill_size == 250


def test_historical_queries(client, user, combined_default_and_single_table_settings):
    """
    Insert StatsRecord rows for past months, then query
    /_stats?month=X&year=Y and verify correct values.
    """
    engine = create_engine(POSTGRES_CONNECTION)
    Session = sessionmaker(bind=engine)
    session = Session()

    now = datetime.datetime.now()

    past = now - datetime.timedelta(days=180)
    past_month = past.month
    past_year = past.year

    session.add(
        StatsRecord(
            total_record_count=42,
            total_record_bytes=999999,
            month=past_month,
            year=past_year,
        )
    )
    session.commit()
    session.close()
    engine.dispose()

    res = client.get(f"/_stats/?month={past_month}&year={past_year}")
    assert res.status_code == 200
    data = res.json
    assert data["fileCount"] == 42
    assert data["totalFileSize"] == 999999


def test_historical_adjacent_months(client, combined_default_and_single_table_settings):
    """
    Insert stats rows for months M-1, M, and M+1 with distinct values.
    Query for month M and verify the correct row is returned.
    """
    engine = create_engine(POSTGRES_CONNECTION)
    Session = sessionmaker(bind=engine)
    session = Session()

    months = [
        (2, 2020, 10, 1000),
        (3, 2020, 20, 2000),
        (4, 2020, 30, 3000),
    ]

    for m, y, count, total in months:
        session.add(
            StatsRecord(
                total_record_count=count,
                total_record_bytes=total,
                month=m,
                year=y,
            )
        )
    session.commit()
    session.close()
    engine.dispose()

    res = client.get("/_stats/?month=3&year=2020")
    assert res.status_code == 200
    data = res.json
    assert data["fileCount"] == 20
    assert data["totalFileSize"] == 2000


def test_historical_gap_query(client, combined_default_and_single_table_settings):
    """
    Insert stats rows for January and March 2020 (skip February).
    Query for February 2020, should return January's row.
    """
    engine = create_engine(POSTGRES_CONNECTION)
    Session = sessionmaker(bind=engine)
    session = Session()

    gap_months = [
        (1, 2020, 50, 5000),
        (3, 2020, 80, 8000),
    ]

    for m, y, count, total in gap_months:
        session.add(
            StatsRecord(
                total_record_count=count,
                total_record_bytes=total,
                month=m,
                year=y,
            )
        )
    session.commit()
    session.close()
    engine.dispose()

    res = client.get("/_stats/?month=2&year=2020")
    assert res.status_code == 200
    data = res.json
    assert data["fileCount"] == 50
    assert data["totalFileSize"] == 5000


def test_query_requires_both_month_and_year(
    client, combined_default_and_single_table_settings
):
    """
    Verify the API returns an error when only month or only year is provided.
    """
    res = client.get("/_stats/?month=6")
    assert res.status_code == 400

    res = client.get("/_stats/?year=2025")
    assert res.status_code == 400

    res = client.get("/_stats/?month=6&year=2025")
    assert res.status_code == 200


def test_stats_empty_table(client, combined_default_and_single_table_settings):
    """
    When the stats table is empty, get_stats() should return (0, 0)
    """
    engine = create_engine(POSTGRES_CONNECTION)
    Session = sessionmaker(bind=engine)

    # Delete all stats rows
    session = Session()
    session.execute(text("DELETE FROM stats"))
    session.commit()
    session.close()

    res = client.get("/_stats/")
    assert res.status_code == 200
    data = res.json

    assert data["fileCount"] == 0
    assert data["totalFileSize"] == 0

    engine.dispose()


def _add_index_record(session, size):
    """Insert a minimal IndexRecord for reconciliation tests."""
    bid = str(uuid.uuid4())
    session.add(BaseVersion(baseid=bid))
    session.flush()
    session.add(
        IndexRecord(
            did=str(uuid.uuid4()),
            baseid=bid,
            rev=str(uuid.uuid4())[:8],
            form="object",
            size=size,
        )
    )


def test_seed_stats_empty_table(combined_default_and_single_table_settings):
    """
    seed_stats on an empty index_record table should create a stats row
    with count=0 and bytes=0.
    """
    engine = create_engine(POSTGRES_CONNECTION)
    Session = sessionmaker(bind=engine)
    session = Session()

    count, total_bytes = seed_stats(session)
    session.commit()

    assert count == 0
    assert total_bytes == 0

    now = datetime.datetime.now()
    row = (
        session.query(StatsRecord)
        .filter(StatsRecord.month == now.month, StatsRecord.year == now.year)
        .first()
    )
    assert row is not None, "seed_stats should create a row even when counts are 0"
    assert row.total_record_count == 0
    assert row.total_record_bytes == 0

    session.close()
    engine.dispose()


def test_seed_stats_accurate_counts(combined_default_and_single_table_settings):
    """
    seed_stats should report the correct count and total bytes matching
    the index_record table.
    """
    engine = create_engine(POSTGRES_CONNECTION)
    Session = sessionmaker(bind=engine)
    session = Session()

    sizes = [100, 250, 650]
    for s in sizes:
        _add_index_record(session, s)
    session.commit()

    count, total_bytes = seed_stats(session)
    session.commit()

    assert count == 3
    assert total_bytes == 1000

    now = datetime.datetime.now()
    row = (
        session.query(StatsRecord)
        .filter(StatsRecord.month == now.month, StatsRecord.year == now.year)
        .first()
    )
    assert row.total_record_count == 3
    assert row.total_record_bytes == 1000

    session.close()
    engine.dispose()


def test_seed_stats_corrects_drifted_values(
    combined_default_and_single_table_settings,
):
    """
    If the stats table has drifted, seed_stats should overwrite with the correct values.
    """
    engine = create_engine(POSTGRES_CONNECTION)
    Session = sessionmaker(bind=engine)
    session = Session()

    _add_index_record(session, 200)
    _add_index_record(session, 300)
    session.commit()

    # insert wrong stats row
    now = datetime.datetime.now()
    session.add(
        StatsRecord(
            total_record_count=999,
            total_record_bytes=999999,
            month=now.month,
            year=now.year,
        )
    )
    session.commit()

    # should overwrite the wrong values
    count, total_bytes = seed_stats(session)
    session.commit()

    assert count == 2
    assert total_bytes == 500

    row = (
        session.query(StatsRecord)
        .filter(StatsRecord.month == now.month, StatsRecord.year == now.year)
        .first()
    )
    assert row.total_record_count == 2
    assert row.total_record_bytes == 500

    session.close()
    engine.dispose()


def test_seed_stats_after_deletions(combined_default_and_single_table_settings):
    """
    If records are deleted but the stats weren't decremented,
    seed_stats should produce correct values.
    """
    engine = create_engine(POSTGRES_CONNECTION)
    Session = sessionmaker(bind=engine)
    session = Session()

    _add_index_record(session, 100)
    _add_index_record(session, 200)
    _add_index_record(session, 300)
    session.commit()

    # initial seed
    seed_stats(session)
    session.commit()

    now = datetime.datetime.now()
    row = (
        session.query(StatsRecord)
        .filter(StatsRecord.month == now.month, StatsRecord.year == now.year)
        .first()
    )
    assert row.total_record_count == 3
    assert row.total_record_bytes == 600

    # delete all records directly
    session.execute(text("DELETE FROM index_record"))
    session.execute(text("DELETE FROM base_version"))
    session.commit()

    # Reconcile should now show 0
    count, total_bytes = seed_stats(session)
    session.commit()

    assert count == 0
    assert total_bytes == 0

    row = (
        session.query(StatsRecord)
        .filter(StatsRecord.month == now.month, StatsRecord.year == now.year)
        .first()
    )
    assert row.total_record_count == 0
    assert row.total_record_bytes == 0

    session.close()
    engine.dispose()


def test_seed_stats_from_connection_accurate(
    combined_default_and_single_table_settings,
):
    """
    seed_stats_from_connection should produce
    the same results as the ORM path.
    """
    engine = create_engine(POSTGRES_CONNECTION)
    Session = sessionmaker(bind=engine)
    session = Session()

    _add_index_record(session, 150)
    _add_index_record(session, 350)
    session.commit()
    session.close()

    with engine.connect() as conn:
        seed_stats_from_connection(conn)

    session = Session()
    now = datetime.datetime.now()
    row = (
        session.query(StatsRecord)
        .filter(StatsRecord.month == now.month, StatsRecord.year == now.year)
        .first()
    )
    assert row is not None, "seed_stats_from_connection should create a stats row"
    assert row.total_record_count == 2
    assert row.total_record_bytes == 500

    session.close()
    engine.dispose()


def test_seed_stats_from_connection_empty_table(
    combined_default_and_single_table_settings,
):
    """
    seed_stats_from_connection on an empty index_record table should still
    create a row.
    """
    engine = create_engine(POSTGRES_CONNECTION)
    Session = sessionmaker(bind=engine)
    session = Session()

    with engine.connect() as conn:
        seed_stats_from_connection(conn)

    session = Session()
    now = datetime.datetime.now()
    row = (
        session.query(StatsRecord)
        .filter(StatsRecord.month == now.month, StatsRecord.year == now.year)
        .first()
    )
    assert (
        row is not None
    ), "seed_stats_from_connection should insert a row when counts are 0"
    assert row.total_record_count == 0
    assert row.total_record_bytes == 0

    session.close()
    engine.dispose()


def test_index_stats(client, user, combined_default_and_single_table_settings):
    """
    create records, verify counts, query future month.
    """
    _create_record(client, user, size=123)
    _create_record(client, user, size=77)
    _create_record(client, user, size=300)
    expected_size = 123 + 77 + 300

    count, size = _get_stats(client)
    assert count == 3
    assert size == expected_size

    # Querying a future month should return current stats
    now = datetime.datetime.now()
    future_month = now.month + 1 if now.month < 12 else 1
    future_year = now.year if now.month < 12 else now.year + 1
    res = client.get(f"/_stats/?month={future_month}&year={future_year}")
    assert res.status_code == 200
    future_data = res.json
    assert future_data["fileCount"] == 3
    assert future_data["totalFileSize"] == expected_size


def test_stats_decrease_on_delete(
    client, user, combined_default_and_single_table_settings
):
    """
    Verify that deleting records correctly decrements stats.
    """
    rec1 = _create_record(client, user, size=100)
    rec2 = _create_record(client, user, size=200)
    rec3 = _create_record(client, user, size=300)

    count, size = _get_stats(client)
    assert count == 3
    assert size == 600

    _delete_record(client, user, rec1["did"], rec1["rev"])
    count, size = _get_stats(client)
    assert count == 2
    assert size == 500

    _delete_record(client, user, rec2["did"], rec2["rev"])
    _delete_record(client, user, rec3["did"], rec3["rev"])
    count, size = _get_stats(client)
    assert count == 0
    assert size == 0
