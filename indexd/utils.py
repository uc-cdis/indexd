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
            'This engine does not support alter, skip migration')
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
