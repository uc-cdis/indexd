from sqlalchemy import create_engine
import logging

def try_drop_test_data(user, database, root_user='postgres', host=''):

    engine = create_engine("postgres://{user}@{host}/postgres".format(
        user=root_user, host=host))

    conn = engine.connect()
    conn.execute("commit")

    try:
        create_stmt = 'DROP DATABASE "{database}"'.format(database=database)
        conn.execute(create_stmt)
    except Exception:
        logging.warn("Unable to drop test data:")

    conn.close()

def setup_database(user, password, database, root_user='postgres',
                   host='', no_drop=False, no_user=False):
    """
    setup the user and database
    """

    if not no_drop:
        try_drop_test_data(user, database)

    engine = create_engine("postgres://{user}@{host}/postgres".format(
        user=root_user, host=host))
    conn = engine.connect()
    conn.execute("commit")

    create_stmt = 'CREATE DATABASE "{database}"'.format(database=database)
    try:
        conn.execute(create_stmt)
    except Exception:
        logging.warn('Unable to create database')

    if not no_user:
        try:
            user_stmt = "CREATE USER {user} WITH PASSWORD '{password}'".format(
                user=user, password=password)
            conn.execute(user_stmt)

            perm_stmt = 'GRANT ALL PRIVILEGES ON DATABASE {database} to {password}'\
                        ''.format(database=database, password=password)
            conn.execute(perm_stmt)
            conn.execute("commit")
        except Exception:
            logging.warn("Unable to add user:")
    conn.close()


def create_tables(host, user, password, database):
    """
    create a table
    """
    engine = create_engine("postgres://{user}:{pwd}@{host}/{db}".format(
        user=user, host=host, pwd=password, db=database))
    conn = engine.connect()

    create_index_record_stm = "CREATE TABLE index_record (\
        did VARCHAR NOT NULL, rev VARCHAR, form VARCHAR, size BIGINT, PRIMARY KEY (did) )"
    create_record_hash_stm = "CREATE TABLE index_record_hash (\
        did VARCHAR NOT NULL, hash_type VARCHAR NOT NULL, hash_value VARCHAR, \
        PRIMARY KEY (did, hash_type), FOREIGN KEY(did) REFERENCES index_record (did))"
    create_record_url_stm = "CREATE TABLE index_record_url( \
        did VARCHAR NOT NULL, url VARCHAR NOT NULL, PRIMARY KEY (did, url),\
        FOREIGN KEY(did) REFERENCES index_record (did) )"
    try:
        conn.execute(create_index_record_stm)
        conn.execute(create_record_hash_stm)
        conn.execute(create_record_url_stm)
    except Exception:
        logging.warn('Unable to create table')
    conn.close()

def check_engine_for_migrate(engine):
    '''
    check if a db engine support database migration

    Args:
        engine (sqlalchemy.engine.base.Engine): a sqlalchemy engine

    Return:
        bool: whether the engine support migration
    '''
    return engine.dialect.supports_alter


def init_schema_version(driver, model):
    '''
    initialize schema table with a singleton of version 0

    Args:
        driver (object): an alias or index driver instance
        model (sqlalchemy.ext.declarative.api.Base): the version table model

    Return:
        version (int): current version number in database
    '''
    with driver.session as s:
        schema_version = s.query(model).first()
        if not schema_version:
            schema_version = model(version=0)
            s.add(schema_version)
        version = schema_version.version
    return version


def migrate_database(driver, migrate_functions, current_schema_version, model):
    '''
    migrate current database to match the schema version provded in
    current schema

    Args:
        driver (object): an alias or index driver instance
        migrate_functions (list): a list of migration functions
        curent_schema_version (int): version of current schema in code
        model (sqlalchemy.ext.declarative.api.Base): the version table model

    Return:
        None
    '''
    db_schema_version = init_schema_version(driver, model)
    need_migrate = (current_schema_version - db_schema_version) > 0

    if not check_engine_for_migrate(driver.engine) and need_migrate:
        driver.logger.error(
            'Engine {} does not support alter, skip migration'.format(
                driver.engine.dialect.name))
        return
    for f in migrate_functions[
            db_schema_version:current_schema_version]:
        with driver.session as s:
            schema_version = s.query(model).first()
            schema_version.version += 1
            driver.logger.info('migrating {} schema to {}'.format(
                driver.__class__.__name__,
                schema_version.version))

            f(engine=driver.engine, session=s)
            s.add(schema_version)
