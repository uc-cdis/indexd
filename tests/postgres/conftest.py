import pytest

from alembic.config import main as alembic_main

from indexd.index.drivers.alchemy import SQLAlchemyIndexDriver
from indexd.alias.drivers.alchemy import SQLAlchemyAliasDriver
from indexd.auth.drivers.alchemy import SQLAlchemyAuthDriver
from tests.default_test_settings import settings


@pytest.fixture(scope="function", autouse=True)
def postgres_driver(app):
    index_driver = SQLAlchemyIndexDriver(settings["config"]["TEST_DB"])
    alias_driver = SQLAlchemyAliasDriver(settings["config"]["TEST_DB"])
    auth_driver = SQLAlchemyAuthDriver(settings["config"]["TEST_DB"])

    # update the settings so that alembic picks up the right DB url
    index_driver_bk = settings["config"]["INDEX"]["driver"]
    settings["config"]["INDEX"]["driver"] = index_driver

    for blueprint in app.blueprints.values():
        blueprint.index_driver = index_driver
        blueprint.alias_driver = alias_driver
        blueprint.auth_driver = auth_driver

    yield index_driver

    settings["config"]["INDEX"]["driver"] = index_driver_bk


@pytest.fixture(autouse=True)
def reset_db():
    alembic_main(["--raiseerr", "downgrade", "base"])
    alembic_main(["--raiseerr", "upgrade", "head"])

    yield
