"""Add_Single_Table_Indexes

Revision ID: b61e2d4aa93d
Revises: bb3d7586a096
Create Date: 2025-02-05 09:11:10.078262

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "b61e2d4aa93d"  # pragma: allowlist secret
down_revision = "bb3d7586a096"  # pragma: allowlist secret
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add indexes to columns
    op.create_index(op.f("ix_record_size"), "record", ["size"])
    op.create_index(op.f("ix_record_hashes"), "record", ["hashes"])


def downgrade() -> None:
    # Remove indexes from columns
    op.drop_index(op.f("ix_record_size"), table_name="record")
    op.drop_index(op.f("ix_record_hashes"), table_name="record")
