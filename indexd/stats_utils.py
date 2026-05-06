"""
Stats-seeding and reconciliation utilities for the indexd stats table.

- migration 9a2169051163_createstatstable uses seed_stats_from_connection with db connection.
- reconcile_stats uses seed_stats with sqlalchemy.
"""

from datetime import datetime

import sqlalchemy as sa
from cdislogging import get_logger
from sqlalchemy import and_, func

from indexd.index.drivers.alchemy import IndexRecord, StatsRecord

logger = get_logger(__name__)


def seed_stats_from_connection(bind):
    """Seed the stats table, given a db connection."""
    now = datetime.now()
    count = bind.execute(sa.text("SELECT COUNT(*) FROM index_record")).scalar()
    total = bind.execute(
        sa.text("SELECT COALESCE(SUM(size), 0) FROM index_record")
    ).scalar()
    bind.execute(
        sa.text(
            "INSERT INTO stats (total_record_count, total_record_bytes, month, year) "
            "VALUES (:count, :total, :month, :year)"
        ),
        {"count": count, "total": total, "month": now.month, "year": now.year},
    )
    logger.info(
        "seed_stats: month=%d year=%d count=%d bytes=%d",
        now.month,
        now.year,
        count,
        total,
    )


def seed_stats(session):
    """
    Compute current stats from index_record and upsert into the stats table.

    Args:
        session: SQLAlchemy ORM session.

    Returns:
        Tuple of (record_count, total_bytes) that were written.
    """
    now = datetime.now()
    count = session.query(func.count()).select_from(IndexRecord).scalar()
    total_bytes = session.query(func.coalesce(func.sum(IndexRecord.size), 0)).scalar()

    existing = (
        session.query(StatsRecord)
        .filter(and_(StatsRecord.month == now.month, StatsRecord.year == now.year))
        .with_for_update()
        .first()
    )

    if existing:
        logger.info(
            "reconcile_stats: month=%d year=%d old_count=%d new_count=%d "
            "old_bytes=%d new_bytes=%d",
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
            "seed_stats: month=%d year=%d count=%d bytes=%d",
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
