"""Create tables

Revision ID: 15f2e9345ade
Revises:
Create Date: 2023-03-02 22:13:41.817477

"""
from alembic import op
import logging
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "15f2e9345ade"
down_revision = None
branch_labels = None
depends_on = None


logger = logging.getLogger("indexd.alembic")


def upgrade() -> None:
    # If this is a new instance of Indexd, we can run this initial Alembic
    # migration to create the tables. If not, the DB might have been partially
    # migrated by the old migration logic and we need to make sure all the
    # old migrations are run, then Alembic can pick up from there and run more
    # recent migrations.
    # The state of the DB after the old migration logic runs is the same as
    # after this initial Alembic migration.
    conn = op.get_bind()
    inspector = sa.engine.reflection.Inspector.from_engine(conn)
    tables = inspector.get_table_names()
    if len(tables) > 0 and tables != ["alembic_version"]:
        logger.info(
            "Found existing tables: this is not a new instance of Indexd. Running the old migration logic... Note that future migrations will be run using Alembic."
        )
        try:
            from local_settings import settings
        except ImportError:
            logger.info("Can't import local_settings, import from default")
            from indexd.default_settings import settings
        index_driver = settings["config"]["INDEX"]["driver"]
        alias_driver = settings["config"]["ALIAS"]["driver"]
        index_driver.migrate_index_database()
        alias_driver.migrate_alias_database()
        return
    else:
        logger.info("No existing tables: running initial migration")

    # index driver
    op.create_table(
        "base_version",
        sa.Column("baseid", sa.VARCHAR(), nullable=False),
        sa.PrimaryKeyConstraint("baseid"),
    )
    op.create_table(
        "index_record",
        sa.Column("did", sa.VARCHAR(), nullable=False),
        sa.Column("baseid", sa.VARCHAR(), nullable=True),
        sa.Column("rev", sa.VARCHAR(), nullable=True),
        sa.Column("form", sa.VARCHAR(), nullable=True),
        sa.Column("size", sa.BIGINT(), nullable=True),
        sa.Column("created_date", sa.DateTime, nullable=True),
        sa.Column("updated_date", sa.DateTime, nullable=True),
        sa.Column("file_name", sa.VARCHAR(), nullable=True),
        sa.Column("version", sa.VARCHAR(), nullable=True),
        sa.Column("uploader", sa.VARCHAR(), nullable=True),
        sa.ForeignKeyConstraint(
            ["baseid"],
            ["base_version.baseid"],
        ),
        sa.PrimaryKeyConstraint("did"),
    )
    op.create_index(
        "ix_index_record_version", "index_record", ["version"], unique=False
    )
    op.create_index(
        "ix_index_record_uploader", "index_record", ["uploader"], unique=False
    )
    op.create_index("ix_index_record_size", "index_record", ["size"], unique=False)
    op.create_index(
        "ix_index_record_file_name", "index_record", ["file_name"], unique=False
    )
    op.create_index("ix_index_record_baseid", "index_record", ["baseid"], unique=False)
    op.create_table(
        "drs_bundle_record",
        sa.Column("bundle_id", sa.VARCHAR(), nullable=False),
        sa.Column("name", sa.VARCHAR(), nullable=True),
        sa.Column("created_time", sa.DateTime, nullable=True),
        sa.Column("updated_time", sa.DateTime, nullable=True),
        sa.Column("checksum", sa.VARCHAR(), nullable=True),
        sa.Column("size", sa.BIGINT(), nullable=True),
        sa.Column("bundle_data", sa.TEXT(), nullable=True),
        sa.Column("description", sa.TEXT(), nullable=True),
        sa.Column("version", sa.VARCHAR(), nullable=True),
        sa.Column("aliases", sa.VARCHAR(), nullable=True),
        sa.PrimaryKeyConstraint("bundle_id"),
    )
    op.create_table(
        "index_record_url",
        sa.Column("did", sa.VARCHAR(), nullable=False),
        sa.Column("url", sa.VARCHAR(), nullable=False),
        sa.ForeignKeyConstraint(
            ["did"],
            ["index_record.did"],
        ),
        sa.PrimaryKeyConstraint("did", "url"),
    )
    op.create_index("index_record_url_idx", "index_record_url", ["did"], unique=False)
    op.create_table(
        "index_record_url_metadata",
        sa.Column("key", sa.VARCHAR(), nullable=False),
        sa.Column("url", sa.VARCHAR(), nullable=False),
        sa.Column("did", sa.VARCHAR(), nullable=False),
        sa.Column("value", sa.VARCHAR(), nullable=True),
        sa.ForeignKeyConstraint(
            ["did", "url"],
            ["index_record_url.did", "index_record_url.url"],
        ),
        sa.PrimaryKeyConstraint("key", "url", "did"),
    )
    op.create_index(
        "ix_index_record_url_metadata_did",
        "index_record_url_metadata",
        ["did"],
        unique=False,
    )
    op.create_index(
        "index_record_url_metadata_idx",
        "index_record_url_metadata",
        ["did"],
        unique=False,
    )
    op.create_table(
        "index_record_authz",
        sa.Column("did", sa.VARCHAR(), nullable=False),
        sa.Column("resource", sa.VARCHAR(), nullable=False),
        sa.ForeignKeyConstraint(
            ["did"],
            ["index_record.did"],
        ),
        sa.PrimaryKeyConstraint("did", "resource"),
    )
    op.create_index(
        "index_record_authz_idx", "index_record_authz", ["did"], unique=False
    )
    op.create_table(
        "index_record_alias",
        sa.Column("did", sa.VARCHAR(), nullable=False),
        sa.Column("name", sa.VARCHAR(), nullable=False),
        sa.ForeignKeyConstraint(
            ["did"],
            ["index_record.did"],
        ),
        sa.PrimaryKeyConstraint("did", "name"),
        sa.UniqueConstraint("name"),
    )
    op.create_index(
        "index_record_alias_name", "index_record_alias", ["name"], unique=False
    )
    op.create_index(
        "index_record_alias_idx", "index_record_alias", ["did"], unique=False
    )
    op.create_table(
        "index_record_hash",
        sa.Column("did", sa.VARCHAR(), nullable=False),
        sa.Column("hash_type", sa.VARCHAR(), nullable=False),
        sa.Column("hash_value", sa.VARCHAR(), nullable=True),
        sa.ForeignKeyConstraint(
            ["did"],
            ["index_record.did"],
        ),
        sa.PrimaryKeyConstraint("did", "hash_type"),
    )
    op.create_index(
        "index_record_hash_type_value_idx",
        "index_record_hash",
        ["hash_value", "hash_type"],
        unique=False,
    )
    op.create_index("index_record_hash_idx", "index_record_hash", ["did"], unique=False)
    op.create_table(
        "index_schema_version",
        sa.Column("version", sa.INTEGER(), nullable=False),
        sa.PrimaryKeyConstraint("version"),
    )
    op.create_table(
        "index_record_metadata",
        sa.Column("key", sa.VARCHAR(), nullable=False),
        sa.Column("did", sa.VARCHAR(), nullable=False),
        sa.Column("value", sa.VARCHAR(), nullable=True),
        sa.ForeignKeyConstraint(
            ["did"],
            ["index_record.did"],
        ),
        sa.PrimaryKeyConstraint("key", "did"),
    )
    op.create_index(
        "index_record_metadata_idx", "index_record_metadata", ["did"], unique=False
    )
    op.create_table(
        "index_record_ace",
        sa.Column("did", sa.VARCHAR(), nullable=False),
        sa.Column("ace", sa.VARCHAR(), nullable=False),
        sa.ForeignKeyConstraint(
            ["did"],
            ["index_record.did"],
        ),
        sa.PrimaryKeyConstraint("did", "ace"),
    )
    op.create_index("index_record_ace_idx", "index_record_ace", ["did"], unique=False)

    # alias driver
    op.create_table(
        "alias_schema_version",
        sa.Column("version", sa.INTEGER(), autoincrement=True, nullable=False),
        sa.PrimaryKeyConstraint("version", name="alias_schema_version_pkey"),
    )
    op.create_table(
        "alias_record",
        sa.Column("name", sa.VARCHAR(), autoincrement=False, nullable=False),
        sa.Column("rev", sa.VARCHAR(), autoincrement=False, nullable=True),
        sa.Column("size", sa.BIGINT(), autoincrement=False, nullable=True),
        sa.Column("release", sa.VARCHAR(), autoincrement=False, nullable=True),
        sa.Column("metastring", sa.VARCHAR(), autoincrement=False, nullable=True),
        sa.Column("keeper_authority", sa.VARCHAR(), autoincrement=False, nullable=True),
        sa.PrimaryKeyConstraint("name", name="alias_record_pkey"),
        postgresql_ignore_search_path=False,
    )
    op.create_table(
        "alias_record_hash",
        sa.Column("name", sa.VARCHAR(), autoincrement=False, nullable=False),
        sa.Column("hash_type", sa.VARCHAR(), autoincrement=False, nullable=False),
        sa.Column("hash_value", sa.VARCHAR(), autoincrement=False, nullable=True),
        sa.ForeignKeyConstraint(
            ["name"], ["alias_record.name"], name="alias_record_hash_name_fkey"
        ),
        sa.PrimaryKeyConstraint("name", "hash_type", name="alias_record_hash_pkey"),
    )
    op.create_table(
        "alias_record_host_authority",
        sa.Column("name", sa.VARCHAR(), autoincrement=False, nullable=False),
        sa.Column("host", sa.VARCHAR(), autoincrement=False, nullable=False),
        sa.ForeignKeyConstraint(
            ["name"],
            ["alias_record.name"],
            name="alias_record_host_authority_name_fkey",
        ),
        sa.PrimaryKeyConstraint(
            "name", "host", name="alias_record_host_authority_pkey"
        ),
    )

    # auth driver
    op.create_table(
        "auth_record",
        sa.Column("username", sa.VARCHAR(), autoincrement=False, nullable=False),
        sa.Column("password", sa.VARCHAR(), autoincrement=False, nullable=True),
        sa.PrimaryKeyConstraint("username", name="auth_record_pkey"),
    )


def downgrade() -> None:
    # auth driver
    op.drop_table("auth_record")

    # alias driver
    op.drop_table("alias_record_host_authority")
    op.drop_table("alias_record_hash")
    op.drop_table("alias_record")
    op.drop_table("alias_schema_version")

    # index driver
    op.drop_index("index_record_ace_idx", table_name="index_record_ace")
    op.drop_table("index_record_ace")
    op.drop_index("index_record_metadata_idx", table_name="index_record_metadata")
    op.drop_table("index_record_metadata")
    op.drop_table("index_schema_version")
    op.drop_index("index_record_hash_idx", table_name="index_record_hash")
    op.drop_index("index_record_hash_type_value_idx", table_name="index_record_hash")
    op.drop_table("index_record_hash")
    op.drop_index("index_record_alias_idx", table_name="index_record_alias")
    op.drop_index("index_record_alias_name", table_name="index_record_alias")
    op.drop_table("index_record_alias")
    op.drop_index("index_record_authz_idx", table_name="index_record_authz")
    op.drop_table("index_record_authz")
    op.drop_index(
        "index_record_url_metadata_idx", table_name="index_record_url_metadata"
    )
    op.drop_index(
        "ix_index_record_url_metadata_did", table_name="index_record_url_metadata"
    )
    op.drop_table("index_record_url_metadata")
    op.drop_index("index_record_url_idx", table_name="index_record_url")
    op.drop_table("index_record_url")
    op.drop_table("drs_bundle_record")
    op.drop_index("ix_index_record_baseid", table_name="index_record")
    op.drop_index("ix_index_record_file_name", table_name="index_record")
    op.drop_index("ix_index_record_size", table_name="index_record")
    op.drop_index("ix_index_record_uploader", table_name="index_record")
    op.drop_index("ix_index_record_version", table_name="index_record")
    op.drop_table("index_record")
    op.drop_table("base_version")
