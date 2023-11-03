import logging
import re
from urllib.parse import urlparse


def hint_match(record, hints):
    for hint in hints:
        if re.match(hint, record):
            return True
    return False


from sqlalchemy import create_engine
from sqlalchemy.engine.reflection import Inspector


def try_drop_test_data(
    user, database, root_user="postgres", host=""
):  # pragma: no cover
    engine = create_engine(
        "postgres://{user}@{host}/postgres".format(user=root_user, host=host)
    )

    conn = engine.connect()
    conn.execute("commit")

    try:
        create_stmt = 'DROP DATABASE "{database}"'.format(database=database)
        conn.execute(create_stmt)
    except Exception:
        logging.warning("Unable to drop test data:")

    conn.close()


def setup_database(
    user,
    password,
    database,
    root_user="postgres",
    host="",
    no_drop=False,
    no_user=False,
):  # pragma: no cover
    """
    setup the user and database
    """

    if not no_drop:
        try_drop_test_data(user, database)

    engine = create_engine(
        "postgres://{user}@{host}/postgres".format(user=root_user, host=host)
    )
    conn = engine.connect()
    conn.execute("commit")

    create_stmt = 'CREATE DATABASE "{database}"'.format(database=database)
    try:
        conn.execute(create_stmt)
    except Exception:
        logging.warning("Unable to create database")

    if not no_user:
        try:
            user_stmt = "CREATE USER {user} WITH PASSWORD '{password}'".format(
                user=user, password=password
            )
            conn.execute(user_stmt)

            perm_stmt = (
                "GRANT ALL PRIVILEGES ON DATABASE {database} to {password}"
                "".format(database=database, password=password)
            )
            conn.execute(perm_stmt)
            conn.execute("commit")
        except Exception:
            logging.warning("Unable to add user")
    conn.close()


def create_tables(host, user, password, database):  # pragma: no cover
    """
    create tables
    """
    engine = create_engine(
        "postgres://{user}:{pwd}@{host}/{db}".format(
            user=user, host=host, pwd=password, db=database
        )
    )
    conn = engine.connect()

    create_index_record_stm = "CREATE TABLE index_record (\
        did VARCHAR NOT NULL, rev VARCHAR, form VARCHAR, size BIGINT, PRIMARY KEY (did) )"
    create_record_hash_stm = "CREATE TABLE index_record_hash (\
        did VARCHAR NOT NULL, hash_type VARCHAR NOT NULL, hash_value VARCHAR, \
        PRIMARY KEY (did, hash_type), FOREIGN KEY(did) REFERENCES index_record (did))"
    create_record_url_stm = "CREATE TABLE index_record_url( \
        did VARCHAR NOT NULL, url VARCHAR NOT NULL, PRIMARY KEY (did, url),\
        FOREIGN KEY(did) REFERENCES index_record (did) )"
    create_index_schema_version_stm = "CREATE TABLE index_schema_version (\
        version INT)"
    create_drs_bundle_record = "CREATE TABLE drs_bundle_record (\
        bundle_id VARCHAR NOT NULL, name VARCHAR, created_time DATETIME, updated_time DATETIME,\
        checksum VARCHAR, size BIGINT, bundle_data TEXT, description TEXT, version VARCHAR, aliases VARCHAR, PRIMARY KEY(bundle_id)"
    try:
        conn.execute(create_index_record_stm)
        conn.execute(create_record_hash_stm)
        conn.execute(create_record_url_stm)
        conn.execute(create_index_schema_version_stm)
        conn.execute(create_drs_bundle_record)
    except Exception:
        logging.warning("Unable to create table")
        raise
    finally:
        conn.close()


def check_engine_for_migrate(engine):
    """
    check if a db engine support database migration

    Args:
        engine (sqlalchemy.engine.base.Engine): a sqlalchemy engine

    Return:
        bool: whether the engine support migration
    """
    return engine.dialect.supports_alter


def init_schema_version(driver, model, version):
    """
    initialize schema table with a initialized singleton of version

    Args:
        driver (object): an alias or index driver instance
        model (sqlalchemy.ext.declarative.api.Base): the version table model

    Return:
        version (int): current version number in database
    """
    with driver.session as s:
        schema_version = s.query(model).first()
        if not schema_version:
            schema_version = model(version=version)
            s.add(schema_version)
        version = schema_version.version
    return version


def migrate_database(driver, migrate_functions, current_schema_version, model):
    """
    This migration logic is DEPRECATED. It is still supported for backwards compatibility,
    but any new migration should be added using Alembic.

    migrate current database to match the schema version provided in
    current schema

    Args:
        driver (object): an alias or index driver instance
        migrate_functions (list): a list of migration functions
        curent_schema_version (int): version of current schema in code
        model (sqlalchemy.ext.declarative.api.Base): the version table model

    Return:
        None
    """
    db_schema_version = init_schema_version(driver, model, 0)

    need_migrate = (current_schema_version - db_schema_version) > 0

    if not check_engine_for_migrate(driver.engine) and need_migrate:
        driver.logger.error(
            "Engine {} does not support alter, skip migration".format(
                driver.engine.dialect.name
            )
        )
        return

    for f in migrate_functions[db_schema_version:current_schema_version]:
        with driver.session as s:
            schema_version = s.query(model).first()
            driver.logger.info(
                "migrating {} schema to {}".format(
                    driver.__class__.__name__, schema_version.version
                )
            )

            f(engine=driver.engine, session=s)
            schema_version.version += 1
            s.add(schema_version)


def reverse_url(url):
    """
    Reverse the domain name for drs service-info IDs
    Args:
        url (str): url of the domain
        example: drs.example.org

    returns:
        id (str): DRS service-info ID
        example: org.example.drs
    """
    parsed_url = urlparse(url)
    if parsed_url.scheme in ["http", "https"]:
        url = parsed_url.hostname
    segments = url.split(".")
    reversed_segments = reversed(segments)
    res = ".".join(reversed_segments)
    return res
