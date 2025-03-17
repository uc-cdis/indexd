#!/usr/bin/env python

"""
The index sqlalchemy driver __init__ function runs all the necessary migration
functions. This will run every migration from the current version as noted in
the database to the latest migration function in the list.
"""

import os

from indexd.index.drivers.alchemy import SQLAlchemyIndexDriver


def parse_args():
    SQLAlchemyIndexDriver(
        "postgresql://{username}:{password}@{hostname}:{port}/{database}".format(
            username=os.environ.get("PG_USER"),
            password=os.environ.get("PG_PASS"),
            hostname=os.environ.get("PG_HOST"),
            port=os.environ.get("PG_PORT"),
            database=os.environ.get("PG_DATABASE"),
        )
    )


if __name__ == "__main__":
    parse_args()
