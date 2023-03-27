import argparse
import sys

from alembic.config import main as alembic_main
from cdislogging import get_logger

from indexd.index.drivers.alchemy import Base as IndexBase
from indexd.alias.drivers.alchemy import Base as AliasBase
from indexd.auth.drivers.alchemy import Base as AuthBase

logger = get_logger(__name__, log_level="info")


def main(path, action=None, username=None, password=None):
    sys.path.append(path)
    try:
        from local_settings import settings
    except ImportError:
        logger.info("Can't import local_settings, import from default")
        from indexd.default_settings import settings
    driver = settings["auth"]
    if action == "create":
        try:
            driver.add(username, password)
            logger.info("User {} created".format(username))
        except Exception as e:
            logger.error(e)

    elif action == "delete":
        try:
            driver.delete(username)
            logger.info("User {} deleted".format(username))
        except Exception as e:
            logger.error(e)

    elif action == "migrate_database":
        try:
            engine_name = settings["config"]["INDEX"]["driver"].engine.dialect.name
            logger.info(f"Start database migration. Engine name: {engine_name}")
            if engine_name == "sqlite":
                IndexBase.metadata.create_all()
                AliasBase.metadata.create_all()
                AuthBase.metadata.create_all()
                settings["config"]["INDEX"]["driver"].migrate_index_database()
                settings["config"]["ALIAS"]["driver"].migrate_alias_database()
            else:
                alembic_main(["--raiseerr", "upgrade", "head"])

        except Exception as e:
            logger.error(e)


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
    main(**args.__dict__)
