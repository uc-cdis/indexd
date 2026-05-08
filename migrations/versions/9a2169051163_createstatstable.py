"""CreateStatsTable

Revision ID: 9a2169051163
Revises: b61e2d4aa93d
Create Date: 2024-05-15 23:06:19.088629

"""
from alembic import op
import sqlalchemy as sa

from indexd.stats_utils import seed_stats_from_connection


# revision identifiers, used by Alembic.
revision = "9a2169051163"
down_revision = "b61e2d4aa93d"  # pragma: allowlist secret
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "stats",
        sa.Column(
            "total_record_count", sa.BIGINT(), autoincrement=False, nullable=False
        ),
        sa.Column(
            "total_record_bytes", sa.BIGINT(), autoincrement=False, nullable=False
        ),
        sa.Column("month", sa.INTEGER(), autoincrement=False, nullable=False),
        sa.Column("year", sa.INTEGER(), autoincrement=False, nullable=False),
        sa.PrimaryKeyConstraint("month", "year"),
    )

    seed_stats_from_connection(op.get_bind())


def downgrade() -> None:
    op.drop_table("stats")
