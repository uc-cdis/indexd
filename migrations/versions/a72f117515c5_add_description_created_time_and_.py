"""Add description, content_created_date and content_updated_date columns to IndexRecord

Revision ID: a72f117515c5
Revises: 15f2e9345ade
Create Date: 2023-04-11 10:00:59.250768

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "a72f117515c5"
down_revision = "15f2e9345ade"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "index_record", sa.Column("content_created_date", sa.DateTime, nullable=True)
    )
    op.add_column(
        "index_record", sa.Column("content_updated_date", sa.DateTime, nullable=True)
    )
    op.add_column("index_record", sa.Column("description", sa.VARCHAR(), nullable=True))


def downgrade() -> None:
    op.drop_column("index_record", "content_created_date")
    op.drop_column("index_record", "content_updated_date")
    op.drop_column("index_record", "description")
