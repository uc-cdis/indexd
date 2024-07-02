"""CreateSingleTable

Revision ID: bb3d7586a096
Revises: a72f117515c5
Create Date: 2023-10-24 14:46:03.868952

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB


# revision identifiers, used by Alembic.
revision = "bb3d7586a096"  # pragma: allowlist secret
down_revision = "a72f117515c5"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "record",
        sa.Column("guid", sa.VARCHAR(), primary_key=True),
        sa.Column("baseid", sa.VARCHAR(), index=True),
        sa.Column("rev", sa.VARCHAR()),
        sa.Column("form", sa.VARCHAR()),
        sa.Column("size", sa.BIGINT()),
        sa.Column("created_date", sa.DateTime, nullable=True),
        sa.Column("updated_date", sa.DateTime, nullable=True),
        sa.Column("file_name", sa.VARCHAR()),
        sa.Column("version", sa.VARCHAR()),
        sa.Column("uploader", sa.VARCHAR()),
        sa.Column("description", sa.VARCHAR()),
        sa.Column("content_created_date", sa.DateTime),
        sa.Column("content_updated_date", sa.DateTime),
        sa.Column("hashes", JSONB()),
        sa.Column("acl", sa.ARRAY(sa.VARCHAR())),
        sa.Column("authz", sa.ARRAY(sa.VARCHAR())),
        sa.Column("urls", sa.ARRAY(sa.VARCHAR())),
        sa.Column("record_metadata", JSONB()),
        sa.Column("url_metadata", JSONB()),
        sa.Column("alias", sa.ARRAY(sa.VARCHAR())),
    )


def downgrade() -> None:
    op.drop_table("record")
