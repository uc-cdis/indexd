from alembic.config import main as alembic_main


def test_upgrade(postgres_driver):
    conn = postgres_driver.engine.connect()
