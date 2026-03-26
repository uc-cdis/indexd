"""
Tests for indexd stats table feature.
"""

import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from unittest.mock import MagicMock

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from indexd.index.drivers.alchemy import (
    StatsRecord,
    update_stats,
)
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
    count = data["fileCount"] or 0
    size = data["totalFileSize"] or 0
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
    baseline_count, baseline_size = _get_stats(client)

    num_records = 10
    record_size = 50
    created = []

    for _ in range(num_records):
        rec = _create_record(client, user, size=record_size)
        created.append(rec)

    final_count, final_size = _get_stats(client)
    assert final_count == baseline_count + num_records
    assert final_size == baseline_size + (num_records * record_size)

    # Clean up
    for rec in created:
        _delete_record(client, user, rec["did"], rec["rev"])

    cleanup_count, cleanup_size = _get_stats(client)
    assert cleanup_count == baseline_count
    assert cleanup_size == baseline_size


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
            f.result()  # re-raise any exceptions

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


def test_size_update(client, user, combined_default_and_single_table_settings):
    """
    Create a blank record, size=None, then fill it with a size with
    PUT /index/blank/{did}, and verify that stats reflect correctly.
    """
    baseline_count, baseline_size = _get_stats(client)

    blank_data = {"uploader": "testuser", "file_name": "test_size_change.txt"}
    res = client.post("/index/blank/", json=blank_data, headers=user)
    assert res.status_code == 201
    blank_rec = res.json

    after_blank_count, after_blank_size = _get_stats(client)
    assert after_blank_count == baseline_count + 1
    assert after_blank_size == baseline_size

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
    filled_rec = res.json

    after_fill_count, after_fill_size = _get_stats(client)

    assert after_fill_count == baseline_count + 1
    assert after_fill_size == baseline_size + 250

    _delete_record(client, user, filled_rec["did"], filled_rec["rev"])


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

    # first remove any existing row for that month
    session.execute(
        text("DELETE FROM stats WHERE month = :m AND year = :y"),
        {"m": past_month, "y": past_year},
    )
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

    res = client.get(f"/_stats/?month={past_month}&year={past_year}")
    assert res.status_code == 200
    data = res.json
    assert data["fileCount"] == 42
    assert data["totalFileSize"] == 999999

    session = Session()
    session.execute(
        text("DELETE FROM stats WHERE month = :m AND year = :y"),
        {"m": past_month, "y": past_year},
    )
    session.commit()
    session.close()
    engine.dispose()


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
    When the stats table is empty, get_stats() should return (None, None)
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

    assert data["fileCount"] is None
    assert data["totalFileSize"] is None

    engine.dispose()
