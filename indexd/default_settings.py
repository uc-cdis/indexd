from .index.drivers.alchemy import SQLAlchemyIndexDriver
from .alias.drivers.alchemy import SQLAlchemyAliasDriver
from .auth.drivers.alchemy import SQLAlchemyAuthDriver

CONFIG = {}

CONFIG["JSONIFY_PRETTYPRINT_REGULAR"] = False
AUTO_MIGRATE = True

# - DEFAULT_PREFIX: prefix to be prepended.
# - PREPEND_PREFIX: the prefix is preprended to the generated GUID when a
#   new record is created WITHOUT a provided GUID.
# - ADD_PREFIX_ALIAS: aliases are created for new records - "<PREFIX><GUID>".
# Do NOT set both ADD_PREFIX_ALIAS and PREPEND_PREFIX to True, or aliases
# will be created as "<PREFIX><PREFIX><GUID>".
CONFIG["INDEX"] = {
    "driver": SQLAlchemyIndexDriver(
        "sqlite:///index.sq3",
        auto_migrate=AUTO_MIGRATE,
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
        "sqlite:///alias.sq3", auto_migrate=AUTO_MIGRATE, echo=True
    )
}

CONFIG["FENCE_URL"] = "http://fence-service/"

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
    {
        "name": "DRS System",
        "host": "https://example.com/api/ga4gh/drs/v1/",
        "hints": [],
        "type": "drs",
    },
]

AUTH = SQLAlchemyAuthDriver("sqlite:///auth.sq3")

settings = {"config": CONFIG, "auth": AUTH}
