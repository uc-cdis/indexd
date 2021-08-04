import logging
import re

from sqlalchemy import create_engine
from sqlalchemy.engine.reflection import Inspector

import sqlalchemy_utils


logger = logging.getLogger(__name__)


def hint_match(record, hints):
    for hint in hints:
        if re.match(hint, record):
            return True
    return False


def try_drop_test_data(
        database='indexd_test', root_user='postgres', host='localhost'):

    # Using an engine that connects to the `postgres` database allows us to
    # create a new database.
    engine = create_engine("postgresql://{user}@{host}/{name}".format(
        user=root_user, host=host, name=database))

    if sqlalchemy_utils.database_exists(engine.url):
        sqlalchemy_utils.drop_database(engine.url)

    engine.dispose()


def setup_database(
        user='test', password='test', database='indexd_test',
        root_user='postgres', host='localhost', no_drop=False, no_user=False):
    """Setup the user and database"""

    if not no_drop:
        try_drop_test_data(database)

    # Create an engine connecting to the `postgres` database allows us to
    # create a new database from there.
    engine = create_engine("postgresql://{user}@{host}/{name}".format(
        user=root_user, host=host, name=database))
    if not sqlalchemy_utils.database_exists(engine.url):
        sqlalchemy_utils.create_database(engine.url)

    conn = engine.connect()

    if not no_user:
        try:
            user_stmt = "CREATE USER {user} WITH PASSWORD '{password}'".format(
                user=user, password=password)
            conn.execute(user_stmt)

            perm_stmt = 'GRANT ALL PRIVILEGES ON DATABASE {database} to {password}'\
                        ''.format(database=database, password=password)
            conn.execute(perm_stmt)
            conn.execute("commit")
        except Exception as e:
            logger.warning("Unable to add user: %s", e)
    conn.close()
    engine.dispose()


def check_engine_for_migrate(engine):
    """
    check if a db engine support database migration

    Args:
        engine (sqlalchemy.engine.base.Engine): a sqlalchemy engine

    Return:
        bool: whether the engine support migration
    """
    return engine.dialect.supports_alter


def init_schema_version(driver, model, current_version):
    """
    initialize schema table with a initialized singleton of version

    Args:
        driver (object): an alias or index driver instance
        model (sqlalchemy.ext.declarative.api.Base): the version table model
        current_version (int): current schema version
    Return:
        version (int): current version number in database
    """
    with driver.session as s:
        schema_version = s.query(model).first()
        if not schema_version:
            schema_version = model(version=current_version)
            s.add(schema_version)
        current_version = schema_version.version
    return current_version


def migrate_database(driver, migrate_functions, current_schema_version, model):
    """
    migrate current database to match the schema version provided in
    current schema

    Args:
        driver (object): an alias or index driver instance
        migrate_functions (list): a list of migration functions
        current_schema_version (int): version of current schema in code
        model (sqlalchemy.ext.declarative.api.Base): the version table model

    Return:
        None
    """
    db_schema_version = init_schema_version(driver, model, 0)

    need_migrate = (current_schema_version - db_schema_version) > 0

    if not check_engine_for_migrate(driver.engine) and need_migrate:
        logger.error(
            'Engine {} does not support alter, skip migration'.format(
                driver.engine.dialect.name))
        return
    for f in migrate_functions[
            db_schema_version:current_schema_version]:
        with driver.session as s:
            schema_version = s.query(model).first()
            schema_version.version += 1
            logger.debug('migrating {} schema to {}'.format(
                driver.__class__.__name__,
                schema_version.version))

            f(engine=driver.engine, session=s)
            s.merge(schema_version)
            logger.debug('finished migration for version {}'.format(
                schema_version.version))


def is_empty_database(driver):
    """
    check if the database is empty or not
    Args:
        driver (object): an alias or index driver instance

    Returns:
        Boolean
    """
    table_list = Inspector.from_engine(driver.engine).get_table_names()

    return len(table_list) == 0
