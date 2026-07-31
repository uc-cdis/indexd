"""
Stats-seeding and reconciliation utilities for the indexd stats table.

- migration 9a2169051163_createstatstable uses seed_stats_from_connection with db connection.
- reconcile_stats uses seed_stats with sqlalchemy.
"""

from datetime import datetime

import sqlalchemy as sa
from cdislogging import get_logger
from sqlalchemy import and_

from indexd.index.drivers.alchemy import StatsRecord

logger = get_logger(__name__)


def _get_table_totals(bind, table_name):
    """Return (count, total_bytes) for the given table name."""
    count = bind.execute(sa.text(f"SELECT COUNT(*) FROM {table_name}")).scalar() or 0
    total = (
        bind.execute(
            sa.text(f"SELECT COALESCE(SUM(size), 0) FROM {table_name}")
        ).scalar()
        or 0
    )
    return int(count), int(total)


def _resolve_stats_source_table(bind):
    """
    Resolve which table should be used for stats reconciliation.

    Currently just resolve by returning the table with the most records.
    """
    inspector = sa.inspect(bind)
    candidates = []

    if inspector.has_table("index_record"):
        count, total = _get_table_totals(bind, "index_record")
        candidates.append(("index_record", count, total))

    if inspector.has_table("record"):
        count, total = _get_table_totals(bind, "record")
        candidates.append(("record", count, total))

    if not candidates:
        raise RuntimeError(
            "Unable to reconcile stats, neither index_record or record table exists"
        )

    # resolve using the highest record count
    source_table, count, total = max(candidates, key=lambda item: item[1])

    return source_table, count, total


def seed_stats_from_connection(bind):
    """Seed the stats table, given a db connection."""
    now = datetime.now()
    source_table, count, total = _resolve_stats_source_table(bind)
    bind.execute(
        sa.text(
            "INSERT INTO stats (total_record_count, total_record_bytes, month, year) "
            "VALUES (:count, :total, :month, :year)"
        ),
        {"count": count, "total": total, "month": now.month, "year": now.year},
    )
    logger.info(
        "seed_stats: source_table=%s month=%d year=%d count=%d bytes=%d",
        source_table,
        now.month,
        now.year,
        count,
        total,
    )


def seed_stats(session):
    """
    Compute current stats from the active record table and upsert into stats.

    Args:
        session: SQLAlchemy ORM session.

    Returns:
        Tuple of (record_count, total_bytes) that were written.
    """
    now = datetime.now()
    source_table, count, total_bytes = _resolve_stats_source_table(session.bind)

    existing = (
        session.query(StatsRecord)
        .filter(and_(StatsRecord.month == now.month, StatsRecord.year == now.year))
        .with_for_update()
        .first()
    )

    if existing:
        logger.info(
            "reconcile_stats: source_table=%s month=%d year=%d old_count=%d new_count=%d "
            "old_bytes=%d new_bytes=%d",
            source_table,
            now.month,
            now.year,
            existing.total_record_count,
            count,
            existing.total_record_bytes,
            total_bytes,
        )
        existing.total_record_count = count
        existing.total_record_bytes = total_bytes
    else:
        logger.info(
            "seed_stats: source_table=%s month=%d year=%d count=%d bytes=%d",
            source_table,
            now.month,
            now.year,
            count,
            total_bytes,
        )
        session.add(
            StatsRecord(
                total_record_count=count,
                total_record_bytes=total_bytes,
                month=now.month,
                year=now.year,
            )
        )

    return (count, total_bytes)
