from .index.drivers.alchemy import SQLAlchemyIndexDriver
from .alias.drivers.alchemy import SQLAlchemyAliasDriver
from .auth.drivers.alchemy import SQLAlchemyAuthDriver
from .index.drivers.single_table_alchemy import SingleTableSQLAlchemyIndexDriver


CONFIG = {}

CONFIG["JSONIFY_PRETTYPRINT_REGULAR"] = False
AUTO_MIGRATE = True
# Key to lock the database during migrations
CONFIG["DB_MIGRATION_POSTGRES_LOCK_KEY"] = 100

USE_SINGLE_TABLE = False

# - DEFAULT_PREFIX: prefix to be prepended.
# - PREPEND_PREFIX: the prefix is preprended to the generated GUID when a
#   new record is created WITHOUT a provided GUID.
# - ADD_PREFIX_ALIAS: aliases are created for new records - "<PREFIX><GUID>".
# Do NOT set both ADD_PREFIX_ALIAS and PREPEND_PREFIX to True, or aliases
# will be created as "<PREFIX><PREFIX><GUID>".
if USE_SINGLE_TABLE is True:
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
else:
    CONFIG["INDEX"] = {
        "driver": SQLAlchemyIndexDriver(
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
        echo=True,
    )
}


CONFIG["DIST"] = [
    {
        "name": "Other IndexD",
        "host": "https://indexd.example.io/index/",
        "hints": [".*ROCKS.*"],
        "type": "indexd",
    },
    {"name": "DX DOI", "host": "https://doi.org/", "hints": ["10\..*"], "type": "doi"},
    {
        "name": "DOS System",
        "host": "https://example.com/api/ga4gh/dos/v1/",
        "hints": [],
        "type": "dos",
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
    "id": "com.example",
    "organization": {
        "name": "CTDS",
        "url": "http://example.com/",
    },
}

CONFIG["ARE_RECORDS_DISCOVERABLE"] = True  # Whether any records are discoverable at all. RBAC is not enabled by default

# Override per-record authz for GET/read
# Only applies to record discovery (not file access)
# If None, use per-record `authz`
CONFIG["GLOBAL_DISCOVERY_AUTHZ"] = []

AUTH = SQLAlchemyAuthDriver(
    "postgresql://postgres:postgres@localhost:5432/indexd_tests"  # pragma: allowlist secret
)

settings = {"config": CONFIG, "auth": AUTH, "use_single_table": USE_SINGLE_TABLE}
