"""create_bucket_region_cache_table

Revision ID: d5a1e9847169
Revises: a72f117515c5
Create Date: 2023-06-21 13:03:15.477447

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "d5a1e9847169"  # pragma: allowlist secret
down_revision = "a72f117515c5"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "bucket_region_mapping_cache",
        sa.Column("bucket_name", sa.VARCHAR(), nullable=False),
        sa.Column("bucket_region", sa.VARCHAR(), nullable=False),
        sa.Column("storage_type", sa.VARCHAR(), nullable=False),
    )


def downgrade() -> None:
    op.drop_table("bucket_region_mapping_cache")
