"""CreateStatsTable

Revision ID: 9a2169051163
Revises: bb3d7586a096
Create Date: 2024-05-15 23:06:19.088629

"""
from alembic import op
import sqlalchemy as sa
import os


# revision identifiers, used by Alembic.
revision = '9a2169051163'
down_revision = 'bb3d7586a096'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "stats",
        sa.Column("sid", sa.BIGINT(), autoincrement=True, primary_key=True),
        sa.Column("total_record_count", sa.BIGINT(),
                  autoincrement=False, nullable=False),
        sa.Column("total_record_bytes", sa.BIGINT(),
                  autoincrement=False, nullable=False),
        sa.Column("month", sa.INTEGER(),
                  autoincrement=False, nullable=False),
        sa.Column("year", sa.INTEGER(),
                  autoincrement=False, nullable=False),
    )
    os.system("python3 ../../indexd/stats_table_migration.py")


def downgrade() -> None:
    op.drop_table("stats")
