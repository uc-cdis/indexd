import os

from indexd.default_settings import *
from indexd.index.drivers.alchemy import SQLAlchemyIndexDriver

CONFIG["DIST"] = [
    {
        "name": "testStage",
        "host": "https://fictitious-commons.io/index/",
        "hints": [".*dg\\.4503.*"],
        "type": "indexd",
    }
]

os.environ["PRESIGNED_FENCE_URL"] = "https://fictitious-commons.io/"
os.environ["HOSTNAME"] = "fictitious-commons.io"
settings = {"config": CONFIG, "auth": AUTH}

# Set PSQL Port, see https://www.postgresql.org/docs/12/app-psql.html
# PSQL default port is 5432, but in some setups, can be 5433.
psql_port = os.environ["PGPORT"] if os.environ.get("PGPORT") else "5432"

# database used by the `/tests/postgres` tests
settings["config"][
    "TEST_DB"
] = "postgres://postgres:postgres@localhost:{0}/indexd_tests".format(psql_port)
