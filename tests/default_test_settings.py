import os

from indexd.utils import drs_service_info_id_url_reversal
from indexd.default_settings import *
from indexd.index.drivers.alchemy import SQLAlchemyIndexDriver

CONFIG["DIST"] = [
    {
        "name": "testStage",
        "host": "https://fictitious-commons.io/index/",
        "hints": [".*dg\\.4503.*"],
        "type": "indexd",
    },
    {
        "name": "DRS System",
        "type": "drs",
        "host": "https://fictitious-commons.io/",
        "artifact": "drs",
        "id": drs_service_info_id_url_reversal("fictitious-commons.io"),
        "version": "1.3.0",
        "organization": {
            "name": "Gen3",
            "url": "https://fictitious-commons.io/",
        },
    },
]

os.environ["PRESIGNED_FENCE_URL"] = "https://fictitious-commons.io/"
os.environ["HOSTNAME"] = "fictitious-commons.io"
settings = {"config": CONFIG, "auth": AUTH}

# Set PSQL Port, see https://www.postgresql.org/docs/12/app-psql.html
# PSQL default port is 5432, but in some setups, can be 5433.
psql_port = os.environ["PGPORT"] if os.environ.get("PGPORT") else "5432"
settings["config"][
    "TEST_DB"
] = "postgres://postgres:postgres@localhost:{0}/test_migration_db".format(  # pragma: allowlist secret
    psql_port
)
