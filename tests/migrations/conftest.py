import pytest

from alembic.config import main as alembic_main

from indexd.index.drivers.alchemy import SQLAlchemyIndexDriver
from tests.default_test_settings import settings


@pytest.fixture(scope="function", autouse=True)
def postgres_driver():
    driver_bk = settings["config"]["INDEX"]["driver"]
    driver = SQLAlchemyIndexDriver(settings["config"]["TEST_DB"])
    settings["config"]["INDEX"]["driver"] = driver

    yield driver

    settings["config"]["INDEX"]["driver"] = driver_bk


@pytest.fixture(autouse=True)
def reset_db():
    alembic_main(["--raiseerr", "downgrade", "base"])
    alembic_main(["--raiseerr", "upgrade", "head"])

    yield
