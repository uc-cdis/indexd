import argparse
import logging
import sys

logger = logging.getLogger(__name__)


def main(path, action=None, username=None, password=None):
    sys.path.append(path)
    try:
        from local_settings import settings
    except ImportError:
        logger.info("Can't import local_settings, import from default")
        from indexd.default_settings import settings
    driver = settings["auth"]
    index_driver = settings["config"]["INDEX"]["driver"]
    alias_driver = settings["config"]["ALIAS"]["driver"]
    if action == "create":
        try:
            driver.add(username, password)
            logger.info(f"User {username} created")
        except Exception as e:
            logger.error(e)

    elif action == "delete":
        try:
            driver.delete(username)
            logger.info(f"User {username} deleted")
        except Exception as e:
            logger.error(e)

    elif action == "migrate_database":
        try:
            logger.info("Start database migration")
            alias_driver.migrate_alias_database()
            index_driver.migrate_index_database()
        except Exception as e:
            logger.error(e)


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--path",
        default="/var/www/indexd/",
        help="path to find local_settings.py",
    )
    subparsers = parser.add_subparsers(title="action", dest="action")
    create = subparsers.add_parser("create")
    delete = subparsers.add_parser("delete")
    subparsers.add_parser("migrate_database")
    create.add_argument("--username", required=True)
    create.add_argument("--password", required=True)
    delete.add_argument("--username", required=True)
    args = parser.parse_args()
    main(**args.__dict__)


if __name__ == "__main__":
    parse_args()
