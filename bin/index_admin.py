import argparse
import sys


def main(path, action=None, username=None, password=None):
    sys.path.append(path)
    try:
        from local_settings import settings
    except ImportError:
        print "Can't import local_settings, import from default"
        from indexd.default_settings import settings
    driver = settings['auth']
    if action == 'create':
        try:
            driver.add(username, password)
            print 'User {} created'.format(username)
        except Exception as e:
            print e.message

    elif action == 'delete':
        try:
            driver.delete(username)
            print 'User {} deleted'.format(username)
        except Exception as e:
            print e.message


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
    create.add_argument('--username', required=True)
    create.add_argument('--password', required=True)
    delete.add_argument('--username', required=True)
    args = parser.parse_args()

    main(**args.__dict__)
