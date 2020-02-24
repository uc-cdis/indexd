from indexd.default_settings import *
from indexd.index.drivers.alchemy import SQLAlchemyIndexDriver

# override the default settings for INDEX because we want to test
# both PREPEND_PREFIX and ADD_PREFIX_ALIAS, which should not both
# be set to True in production environments
CONFIG["INDEX"] = {
    "driver": SQLAlchemyIndexDriver(
        "sqlite:///index.sq3",
        auto_migrate=True,
        echo=True,
        index_config={
            "DEFAULT_PREFIX": "testprefix:",
            "PREPEND_PREFIX": True,
            "ADD_PREFIX_ALIAS": True,
        },
    )
}

CONFIG["PRESIGNED_URL_ENDPT"] = "https://fictitious-commons.io/"

settings = {"config": CONFIG, "auth": AUTH}

settings["config"]["TEST_DB"] = "postgres://postgres@localhost/test_migration_db"