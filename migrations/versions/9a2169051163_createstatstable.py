"""CreateStatsTable

Revision ID: 9a2169051163
Revises: b61e2d4aa93d
Create Date: 2024-05-15 23:06:19.088629

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "9a2169051163"
down_revision = "b61e2d4aa93d"  # pragma: allowlist secret
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "stats",
        sa.Column("sid", sa.BIGINT(), autoincrement=True, primary_key=True),
        sa.Column(
            "total_record_count", sa.BIGINT(), autoincrement=False, nullable=False
        ),
        sa.Column(
            "total_record_bytes", sa.BIGINT(), autoincrement=False, nullable=False
        ),
        sa.Column("month", sa.INTEGER(), autoincrement=False, nullable=False),
        sa.Column("year", sa.INTEGER(), autoincrement=False, nullable=False),
    )
    op.create_index("ix_stats_year_month", "stats", ["year", "month"])
    _seed_stats_table()


def _seed_stats_table():
    from datetime import datetime

    bind = op.get_bind()
    now = datetime.now()
    count = bind.execute(sa.text("SELECT COUNT(*) FROM index_record")).scalar() or 0
    total = (
        bind.execute(
            sa.text("SELECT COALESCE(SUM(size), 0) FROM index_record")
        ).scalar()
        or 0
    )
    if count > 0 or total > 0:
        bind.execute(
            sa.text(
                "INSERT INTO stats (total_record_count, total_record_bytes, month, year) "
                "VALUES (:count, :total, :month, :year)"
            ),
            {"count": count, "total": total, "month": now.month, "year": now.year},
        )


def downgrade() -> None:
    op.drop_index("ix_stats_year_month", table_name="stats")
    op.drop_table("stats")
