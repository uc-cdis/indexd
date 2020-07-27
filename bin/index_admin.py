#!/usr/bin/env python

import argparse
import sys
from cdislogging import get_logger

logger = get_logger('index_admin')

def main(path, action=None, username=None, password=None):
    sys.path.append(path)
    try:
        from local_settings import settings
    except ImportError:
        logger.info("Can't import local_settings, import from default")
        from indexd.default_settings import settings
    driver = settings['auth']
    index_driver = settings['config']['INDEX']['driver']
    alias_driver = settings['config']['ALIAS']['driver']
    if action == 'create':
        try:
            driver.add(username, password)
            logger.info('User {} created'.format(username))
        except Exception as e:
            logger.error(e.message)

    elif action == 'delete':
        try:
            driver.delete(username)
            logger.info('User {} deleted'.format(username))
        except Exception as e:
            logger.error(e.message)

    elif action == 'migrate_database':
        try:
            logger.info('Start database migration')
            alias_driver.migrate_alias_database()
            index_driver.migrate_index_database()
        except Exception as e:
            logger.error(e.message)
if __name__ == '__main__':

    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--path',
        default='/var/www/indexd/',
        help='path to find local_settings.py',
    )
    subparsers = parser.add_subparsers(title='action', dest='action')
    create = subparsers.add_parser('create')
    delete = subparsers.add_parser('delete')
    migrate = subparsers.add_parser('migrate_database')
    create.add_argument('--username', required=True)
    create.add_argument('--password', required=True)
    delete.add_argument('--username', required=True)
    args = parser.parse_args()
    main(**args.__dict__)
