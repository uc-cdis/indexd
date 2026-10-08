"""
Stats-seeding and reconciliation utilities for the indexd stats table.

- migration 9a2169051163_createstatstable uses seed_stats_from_connection with db connection.
- reconcile_stats uses seed_stats with sqlalchemy.
"""

from datetime import datetime

from sqlalchemy import select
import sqlalchemy as sa
from cdislogging import get_logger
from sqlalchemy import and_

from indexd.index.drivers.alchemy import StatsRecord

logger = get_logger(__name__)


async def _get_table_totals(conn, table_name):
    """Return (count, total_bytes) for the given table name using an active connection."""
    count_result = await conn.execute(sa.text(f"SELECT COUNT(*) FROM {table_name}"))
    count = count_result.scalar() or 0

    total_result = await conn.execute(
        sa.text(f"SELECT COALESCE(SUM(size), 0) FROM {table_name}")
    )
    total = total_result.scalar() or 0

    return int(count), int(total)


def _get_inspector(conn):
    inspector = sa.inspect(conn)
    return inspector.has_table("index_record"), inspector.has_table("record")


async def _resolve_stats_source_table(session):
    """
    Resolve which table should be used for stats reconciliation.
    """
    async with session.bind.begin() as conn:
        has_index_record, has_record = await conn.run_sync(_get_inspector)
        candidates = []

        if has_index_record:
            count, total = await _get_table_totals(conn, "index_record")
            candidates.append(("index_record", count, total))

        if has_record:
            count, total = await _get_table_totals(conn, "record")
            candidates.append(("record", count, total))

        if not candidates:
            raise RuntimeError(
                "Unable to reconcile stats, neither index_record or record table exists"
            )

        # resolve using the highest record count
        source_title, count, total = max(candidates, key=lambda item: item[1])

        return source_title, count, total


async def seed_stats_from_connection(session):
    """Seed the stats table, given a db connection or engine."""
    now = datetime.now()
    source_table, count, total = await _resolve_stats_source_table(session)

    async with session.bind.begin() as conn:
        await conn.execute(
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


async def seed_stats(session):
    """
    Compute current stats from the active record table and upsert into stats.

    Args:
        session: SQLAlchemy AsyncSession.

    Returns:
        Tuple of (record_count, total_bytes) that were written.
    """
    now = datetime.now()
    source_table, count, total_bytes = await _resolve_stats_source_table(session)

    stmt = (
        select(StatsRecord)
        .filter(and_(StatsRecord.month == now.month, StatsRecord.year == now.year))
        .with_for_update()
    )
    result = await session.execute(stmt)
    existing = result.scalars().first()

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
