import logging
import os

from .alias.drivers.alchemy import SQLAlchemyAliasDriver
from .auth.drivers.alchemy import SQLAlchemyAuthDriver
from .index.drivers.alchemy import SQLAlchemyIndexDriver

logger = logging.getLogger(__name__)
CONFIG = {"JSONIFY_PRETTYPRINT_REGULAR": False}

AUTO_MIGRATE = True
SQLALCHEMY_VERBOSE = os.getenv("INDEXD_VERBOSE", "").lower() in ["1", "yes", "true"]
PG_HOST = os.getenv("PG_INDEXD_HOST", "localhost")
PG_USER = os.getenv("PG_INDEXD_USER", "test")
PG_PASS = os.getenv("PG_INDEXD_PASS", "test")
PG_DBNAME = os.getenv("PG_INDEXD_DBNAME", "indexd_test")
PG_URL = f"postgresql://{PG_USER}:{PG_PASS}@{PG_HOST}/{PG_DBNAME}"
MAX_POOL_SIZE = int(os.getenv("PG_INDEXD_MAX_POOL_SIZE", "5"))
MAX_POOL_SIZE_OVERFLOW = int(os.getenv("PG_INDEXD_MAX_POOL_SIZE_OVERFLOW", "10"))

CONFIG["INDEX"] = {
    "driver": SQLAlchemyIndexDriver(
        PG_URL,
        auto_migrate=AUTO_MIGRATE,
        echo=SQLALCHEMY_VERBOSE,
        index_config={
            "DEFAULT_PREFIX": "testprefix:",
            "ADD_PREFIX_ALIAS": True,
            "PREPEND_PREFIX": True,
        },
        pool_size=MAX_POOL_SIZE,
        max_overflow=MAX_POOL_SIZE_OVERFLOW,
    ),
}

CONFIG["ALIAS"] = {
    "driver": SQLAlchemyAliasDriver(
        PG_URL,
        auto_migrate=AUTO_MIGRATE,
        echo=SQLALCHEMY_VERBOSE,
        pool_size=MAX_POOL_SIZE,
        max_overflow=MAX_POOL_SIZE_OVERFLOW,
    ),
}

CONFIG["DIST"] = [
    {
        "name": "Other IndexD",
        "host": "https://indexd.example.io/index/",
        "hints": [".*ROCKS.*"],
        "type": "indexd",
    },
]

AUTH = SQLAlchemyAuthDriver(
    PG_URL, pool_size=MAX_POOL_SIZE, max_overflow=MAX_POOL_SIZE_OVERFLOW
)

settings = {"config": CONFIG, "auth": AUTH}
try:
    INDEXD_USER = os.environ["INDEXD_USER"]
    INDEXD_PASS = os.environ["INDEXD_USER"]
    AUTH.add(INDEXD_USER, INDEXD_PASS)
except Exception as e:
    logger.warning("Unable to create indexd user/pass", exc_info=e)
