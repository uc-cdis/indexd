import os

from indexd.index.drivers.single_table_alchemy import SingleTableSQLAlchemyIndexDriver
from .alias.drivers.alchemy import SQLAlchemyAliasDriver
from .auth.drivers.alchemy import SQLAlchemyAuthDriver

# - DEFAULT_PREFIX: prefix to be prepended.
# - PREPEND_PREFIX: the prefix is preprended to the generated GUID when a
#   new record is created WITHOUT a provided GUID.
# - ADD_PREFIX_ALIAS: aliases are created for new records - "<PREFIX><GUID>".
# Do NOT set both ADD_PREFIX_ALIAS and PREPEND_PREFIX to True, or aliases
# will be created as "<PREFIX><PREFIX><GUID>".

CONFIG = {}
CONFIG["DIST"] = [
    {
        "name": "testStage",
        "host": "https://fictitious-commons.io/index/",
        "hints": [".*dg\\.4503.*"],
        "type": "indexd",
    },
]

CONFIG["DRS_SERVICE_INFO"] = {
    "name": "DRS System",
    "type": {
        "group": "org.ga4gh",
        "artifact": "drs",
        "version": "1.0.3",
    },
    "version": "1.0.3",
    "organization": {
        "name": "CTDS",
        "url": "https://fictitious-commons.io",
    },
}

os.environ["PRESIGNED_FENCE_URL"] = "https://fictitious-commons.io/"
os.environ["HOSTNAME"] = "fictitious-commons.io"

# Set PSQL Port, see https://www.postgresql.org/docs/12/app-psql.html
# PSQL default port is 5432, but in some setups, can be 5433.
psql_port = os.environ["PGPORT"] if os.environ.get("PGPORT") else "5432"


CONFIG["INDEX"] = {
    "driver": SingleTableSQLAlchemyIndexDriver(
        "postgresql://postgres:postgres@localhost:5432/indexd_tests",  # pragma: allowlist secret
        echo=True,
        index_config={
            "DEFAULT_PREFIX": "testprefix:",
            "PREPEND_PREFIX": True,
            "ADD_PREFIX_ALIAS": False,
        },
    )
}

CONFIG["ALIAS"] = {
    "driver": SQLAlchemyAliasDriver(
        "postgresql://postgres:postgres@localhost:5432/indexd_tests",  # pragma: allowlist secret
        echo=True,  # pragma: allowlist secret
    )
}

AUTH = SQLAlchemyAuthDriver(
    "postgresql://postgres:postgres@localhost:5432/indexd_tests"  # pragma: allowlist secret
)  # pragma: allowlist secret

settings = {"config": CONFIG, "auth": AUTH}
