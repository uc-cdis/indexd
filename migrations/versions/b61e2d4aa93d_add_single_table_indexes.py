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
    op.create_index(op.f("ix_record_file_name"), "record", ["file_name"])
    op.create_index(op.f("ix_record_version"), "record", ["version"])
    op.create_index(op.f("ix_record_uploader"), "record", ["uploader"])
    op.create_index(op.f("ix_record_hashes"), "record", ["hashes"])
    op.create_index(op.f("ix_record_acl"), "record", ["acl"])
    op.create_index(op.f("ix_record_authz"), "record", ["authz"])
    op.create_index(op.f("ix_record_urls"), "record", ["urls"])
    op.create_index(op.f("ix_record_record_metadata"), "record", ["record_metadata"])
    op.create_index(op.f("ix_record_url_metadata"), "record", ["url_metadata"])
    op.create_index(op.f("ix_record_alias"), "record", ["alias"])


def downgrade() -> None:
    # Remove indexes from columns
    op.drop_index(op.f("ix_record_size"), table_name="record")
    op.drop_index(op.f("ix_record_file_name"), table_name="record")
    op.drop_index(op.f("ix_record_version"), table_name="record")
    op.drop_index(op.f("ix_record_uploader"), table_name="record")
    op.drop_index(op.f("ix_record_hashes"), table_name="record")
    op.drop_index(op.f("ix_record_acl"), table_name="record")
    op.drop_index(op.f("ix_record_authz"), table_name="record")
    op.drop_index(op.f("ix_record_urls"), table_name="record")
    op.drop_index(op.f("ix_record_record_metadata"), table_name="record")
    op.drop_index(op.f("ix_record_url_metadata"), table_name="record")
    op.drop_index(op.f("ix_record_alias"), table_name="record")
