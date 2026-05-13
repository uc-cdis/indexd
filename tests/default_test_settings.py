import os

from indexd.default_settings import *

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
        "version": "1.5.0",
    },
    "version": "1.5.0",
    "organization": {
        "name": "CTDS",
        "url": "https://fictitious-commons.io",
    },
}

CONFIG["DRS_AUTHORIZATION_METADATA"] = {
    "/gen3/programs/a/projects/b": {
        "supported_types": ["BearerAuth", "PassportAuth"],
        "passport_auth_issuers": ["https://ras/foo/bar"],
        "bearer_auth_issuers": ["https://gen3.datacommons.io"],
    }
}
CONFIG["DEFAULT_BEARER_ISSUER"] = "test_default"

os.environ["PRESIGNED_FENCE_URL"] = "https://fictitious-commons.io/"
os.environ["HOSTNAME"] = "fictitious-commons.io"
settings = {"config": CONFIG, "auth": AUTH}

# Set PSQL Port, see https://www.postgresql.org/docs/12/app-psql.html
# PSQL default port is 5432, but in some setups, can be 5433.
psql_port = os.environ["PGPORT"] if os.environ.get("PGPORT") else "5432"

# database used by the `/tests/postgres` tests
settings["config"]["TEST_DB"] = (
    "postgresql://postgres:postgres@localhost:{0}/indexd_tests".format(psql_port)
)
