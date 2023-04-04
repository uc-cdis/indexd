import logging
import os
import re
from typing import Optional

import sqlalchemy_utils
from sqlalchemy import create_engine
from sqlalchemy.engine.reflection import Inspector

logger = logging.getLogger(__name__)


def handle_error(resp):
    if 400 <= resp.status_code < 600:
        try:
            json = resp.json()
            resp.reason = json.get("error")
        except KeyError:
            pass
        finally:
            resp.raise_for_status()


def hint_match(record, hints):
    for hint in hints:
        if re.match(hint, record):
            return True
    return False


ROOT_USER = os.getenv("PG_INDEXD_ROOT_USER", "postgres")
ROOT_PASS = os.getenv("PG_INDEXD_ROOT_PASS")


def __root_user_auth(user: str, password: Optional[str] = None) -> Optional[str]:
    if not user:
        return
    return user if not password else f"{user}:{password}"


IndexdConfig = dict(
    user=os.getenv("PG_INDEXD_USER", "test"),
    password=os.getenv("PG_INDEXD_PASS", "test"),
    host=os.getenv("PG_INDEXD_HOST", "localhost"),
    database=os.getenv("PG_INDEXD_DBNAME", "indexd_test"),
    root_user=ROOT_USER,
    root_password=ROOT_PASS,
    drop_database=os.getenv("PG_INDEXD_DROP_DB", "true") == "true",
    root_auth=__root_user_auth(ROOT_USER, ROOT_PASS),
)


def try_drop_test_data(
    database: Optional[str] = None,
    root_user: Optional[str] = None,
    host: Optional[str] = None,
    root_pass: Optional[str] = None,
    drop_db: Optional[bool] = None,
) -> None:
    """Attempts dropping the indexd database, useful only for testing."""

    host = host or IndexdConfig["host"]
    database = database or IndexdConfig["database"]
    root_auth = __root_user_auth(root_user, root_pass) or IndexdConfig["root_auth"]
    drop_db = drop_db or IndexdConfig["drop_database"]

    if not drop_db:
        return

    engine = create_engine(
        "postgresql://{user}@{host}/{name}".format(user=root_auth, host=host, name=database)
    )

    if sqlalchemy_utils.database_exists(engine.url):
        sqlalchemy_utils.drop_database(engine.url)

    engine.dispose()


def setup_database(
    user=None,
    password=None,
    database=None,
    root_user=None,
    host=None,
    no_drop=None,
    no_user=None,
    root_pass=None,
) -> None:
    """Set up the user and database"""

    user = user or IndexdConfig["user"]
    password = password or IndexdConfig["password"]
    host = host or IndexdConfig["host"]
    database = database or IndexdConfig["database"]
    root_user = root_user or IndexdConfig["root_user"]
    root_pass = root_pass or IndexdConfig["root_password"]
    auth = __root_user_auth(root_user, root_pass) or IndexdConfig["root_auth"]
    drop_db = no_drop or IndexdConfig["drop_database"]

    try_drop_test_data(
        database=database,
        root_user=root_user,
        host=host,
        root_pass=root_pass,
        drop_db=drop_db,
    )

    # Create an engine connecting to the `postgres` database allows us to
    # create a new database from there.
    engine = create_engine(
        "postgresql://{user}@{host}/{name}".format(user=auth, host=host, name=database)
    )
    if not sqlalchemy_utils.database_exists(engine.url):
        sqlalchemy_utils.create_database(engine.url)

    conn = engine.connect()

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
            "Engine {} does not support alter, skip migration".format(
                driver.engine.dialect.name
            )
        )
        return
    for f in migrate_functions[db_schema_version:current_schema_version]:
        with driver.session as s:
            schema_version = s.query(model).first()
            schema_version.version += 1
            logger.debug(
                "migrating {} schema to {}".format(
                    driver.__class__.__name__, schema_version.version
                )
            )

            f(engine=driver.engine, session=s)
            s.merge(schema_version)
            logger.debug(
                "finished migration for version {}".format(schema_version.version)
            )


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
