import argparse
import sys
import asyncio

from alembic.config import main as alembic_main
from cdislogging import get_logger

from indexd.index.drivers.alchemy import Base as IndexBase
from indexd.alias.drivers.alchemy import Base as AliasBase
from indexd.auth.drivers.alchemy import Base as AuthBase

logger = get_logger(__name__, log_level="info")


async def main(path, action=None, username=None, password=None):
    sys.path.append(path)
    try:
        from local_settings import settings
    except ImportError:
        logger.info("Can't import local_settings, import from default")
        from indexd.default_settings import settings

    driver = settings["auth"]

    try:
        if action == "create":
            try:
                await driver.add(username, password)
                logger.info("User {} created".format(username))
            except Exception as e:
                logger.error(e)

        elif action == "delete":
            try:
                await driver.delete(username)
                logger.info("User {} deleted".format(username))
            except Exception as e:
                logger.error(e)

        elif action == "migrate_database":
            try:
                index_driver = settings["config"]["INDEX"]["driver"]
                alias_driver = settings["config"]["ALIAS"]["driver"]

                engine_name = index_driver.engine.dialect.name
                logger.info(f"Start database migration. Engine name: {engine_name}")

                if engine_name == "sqlite":
                    # SQLAlchemy requires metadata.create_all to be run synchronously
                    # via run_sync inside an async connection
                    async with index_driver.engine.begin() as conn:
                        await conn.run_sync(IndexBase.metadata.create_all)

                    async with alias_driver.engine.begin() as conn:
                        await conn.run_sync(AliasBase.metadata.create_all)
                        await conn.run_sync(AuthBase.metadata.create_all)

                    await index_driver.migrate_index_database()
                    await alias_driver.migrate_alias_database()
                else:
                    # Alembic is strictly synchronous, so we run it in a separate thread
                    # to prevent it from blocking the async event loop
                    await asyncio.to_thread(
                        alembic_main, ["--raiseerr", "upgrade", "head"]
                    )

            except Exception as e:
                logger.error(e)

    finally:
        # Gracefully dispose of async engines to prevent unclosed connection warnings
        if hasattr(settings["config"]["INDEX"]["driver"], "engine"):
            await settings["config"]["INDEX"]["driver"].engine.dispose()
        if hasattr(settings["config"]["ALIAS"]["driver"], "engine"):
            await settings["config"]["ALIAS"]["driver"].engine.dispose()
        if hasattr(settings.get("auth"), "engine"):
            await settings["auth"].engine.dispose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--path", default="/var/www/indexd/", help="path to find local_settings.py"
    )
    subparsers = parser.add_subparsers(title="action", dest="action")
    create = subparsers.add_parser("create")
    delete = subparsers.add_parser("delete")
    migrate = subparsers.add_parser("migrate_database")

    create.add_argument("--username", required=True)
    create.add_argument("--password", required=True)
    delete.add_argument("--username", required=True)

    args = parser.parse_args()

    # Use asyncio.run to execute the async main function
    asyncio.run(main(**args.__dict__))
