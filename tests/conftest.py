import base64

import flask
import pytest
from sqlalchemy import create_engine

import swagger_client
from indexd import app_init, get_app
from indexd.alias.drivers.alchemy import (
    Base as alias_base,
    SQLAlchemyAliasDriver,
)
from indexd.auth.drivers.alchemy import Base as auth_base, SQLAlchemyAuthDriver
from indexd.index.drivers.alchemy import (
    Base as index_base,
    SQLAlchemyIndexDriver,
)
from indexd_fixtures import (
    alias_driver,
    alias_driver_no_migrate,
    auth_driver,
    create_indexd_tables,
    create_indexd_tables_no_migrate,
    index_driver,
    index_driver_no_migrate,
    indexd_server,
    setup_indexd_test_database,
)

PG_URL = 'postgres://test:test@localhost/indexd_test'


@pytest.fixture
def app(index_driver, alias_driver, auth_driver):
    """
    We have to give all the settings here because when a driver is initiated
    it goes through an entire migration process that creates all the tables.
    The tables are already created from the fixtures in this module.
    """
    app = flask.Flask('indexd')
    settings = {
        'config': {
            'INDEX': {
                'driver': index_driver,
            },
            'ALIAS': {
                'driver': alias_driver,
            },
        },
        'auth': auth_driver,
    }
    app_init(app, settings=settings)
    return app


@pytest.fixture
def user(auth_driver):
    auth_driver.add('test', 'test')
    return {
        'Authorization': (
            'Basic ' +
            base64.b64encode(b'test:test').decode('ascii')),
        'Content-Type': 'application/json'
    }


@pytest.fixture
def swg_config(indexd_server, create_indexd_tables):
    config = swagger_client.Configuration()
    config.host = 'http://localhost:8001'
    config.username = 'admin'
    config.password = 'admin'
    return config


@pytest.fixture
def swg_config_no_migrate(indexd_server_no_migrate, create_indexd_tables_no_migrate):
    config = swagger_client.Configuration()
    config.host = 'http://localhost:8001'
    config.username = 'admin'
    config.password = 'admin'
    return config


@pytest.fixture
def swg_index_client(swg_config):
    return swagger_client.IndexApi(swagger_client.ApiClient(swg_config))


@pytest.fixture
def swg_index_client_no_migrate(swg_config_no_migrate):
    return swagger_client.IndexApi(swagger_client.ApiClient(swg_config_no_migrate))


@pytest.fixture
def swg_global_client(swg_config):
    return swagger_client.GlobalApi(swagger_client.ApiClient(swg_config))


@pytest.fixture
def swg_global_client_no_migrate(swg_config_no_migrate):
    return swagger_client.GlobalApi(swagger_client.ApiClient(swg_config_no_migrate))


@pytest.fixture
def swg_alias_client(swg_config):
    return swagger_client.AliasApi(swagger_client.ApiClient(swg_config))


@pytest.fixture
def swg_alias_client_no_migrate(swg_config_no_migrate):
    return swagger_client.AliasApi(swagger_client.ApiClient(swg_config_no_migrate))


@pytest.fixture
def swg_dos_client(swg_config):
    return swagger_client.DOSApi(swagger_client.ApiClient(swg_config))


@pytest.fixture
def swg_dos_client_no_migrate(swg_config_no_migrate):
    return swagger_client.DOSApi(swagger_client.ApiClient(swg_config_no_migrate))


@pytest.fixture
def swg_query_client(swg_config):
    return swagger_client.QueryApi(swagger_client.ApiClient(swg_config))


@pytest.fixture
def swg_query_client_no_migrate(swg_config_no_migrate):
    return swagger_client.QueryApi(swagger_client.ApiClient(swg_config_no_migrate))


@pytest.fixture
def swg_bulk_client(swg_config):
    return swagger_client.BulkApi(swagger_client.ApiClient(swg_config))


@pytest.fixture
def swg_bulk_client_no_migrate(swg_config_no_migrate):
    return swagger_client.BulkApi(swagger_client.ApiClient(swg_config_no_migrate))


@pytest.fixture
def database_engine():
    engine = create_engine(PG_URL)
    yield engine
    engine.dispose()


@pytest.fixture
def database_conn(database_engine):
    conn = database_engine.connect()
    yield conn
    conn.close()
