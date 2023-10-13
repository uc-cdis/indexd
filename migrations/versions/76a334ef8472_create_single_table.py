"""create_single_table


Revision ID: 76a334ef8472
Revises: a72f117515c5
Create Date: 2023-10-11 16:01:13.463855

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "76a334ef8472"  # pragma: allowlist secret
down_revision = "a72f117515c5"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "Record",
        sa.Column("guid", String, primary_key=True),
        sa.Column("baseid", String, index=True),
        sa.Column("rev", String),
        sa.Column("form", String),
        sa.Column("size", BigInteger, index=True),
        sa.Column("created_date", DateTime, default=datetime.datetime.utcnow),
        sa.Column("updated_date", DateTime, default=datetime.datetime.utcnow),
        sa.Column("file_name", String, index=True),
        sa.Column("version", String, index=True),
        sa.Column("uploader", String, index=True),
        sa.Column("description", String),
        sa.Column("content_created_date", DateTime),
        sa.Column("content_updated_date", DateTime),
        sa.Column("hashes", JSONB),
        sa.Column("acl", ARRAY(String)),
        sa.Column("authz", ARRAY(String)),
        sa.Column("urls", ARRAY(String)),
        sa.Column("record_metadata", JSONB),
        sa.Column("url_metadata", JSONB),
        sa.Column("alias", ARRAY(String)),
    )


def downgrade() -> None:
    op.drop_table("Record")
