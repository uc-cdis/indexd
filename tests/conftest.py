from indexd import get_app
import base64
import pytest
# indexd_server and indexd_client is needed as fixtures
from cdisutilstest.code.conftest import indexd_server, indexd_client # noqa
from cdisutilstest.code.indexd_fixture import clear_database
import swagger_client

from indexd import auth
import importlib

try:
    reload  # Python 2.7
except NameError:
    try:
        from importlib import reload  # Python 3.4+
    except ImportError:
        from imp import reload  # Python 3.0 - 3.3<Paste>


@pytest.fixture
def app():
    # this is to make sure sqlite is initialized
    # for every unittest
    from indexd import default_settings
    importlib.reload(default_settings)
    yield get_app()
    try:
        clear_database()

    except:
        pass


@pytest.fixture
def user(app):
    app.auth.add('test', 'test')
    yield {
        'Authorization': (
            'Basic ' +
            base64.b64encode(b'test:test').decode('ascii')),
        'Content-Type': 'application/json'
    }
    app.auth.delete('test')


@pytest.fixture
def swg_config(indexd_client): # noqa
    config = swagger_client.Configuration()
    config.host = indexd_client.url
    config.username = indexd_client.auth[0]
    config.password = indexd_client.auth[1]
    yield config


@pytest.fixture
def swg_index_client(swg_config):
    api = swagger_client.IndexApi(swagger_client.ApiClient(swg_config))
    yield api


@pytest.fixture
def swg_global_client(swg_config):
    api = swagger_client.GlobalApi(swagger_client.ApiClient(swg_config))
    yield api


@pytest.fixture
def swg_alias_client(swg_config):
    api = swagger_client.AliasApi(swagger_client.ApiClient(swg_config))
    yield api


@pytest.fixture
def swg_dos_client(swg_config):
    api = swagger_client.DOSApi(swagger_client.ApiClient(swg_config))
    yield api


@pytest.fixture
def swg_query_client(swg_config):
    api = swagger_client.QueryApi(swagger_client.ApiClient(swg_config))
    yield api

@pytest.fixture 
def swg_bulk_client(swg_config): 
    api = swagger_client.BulkApi(swagger_client.ApiClient(swg_config))
    yield api


@pytest.fixture
def skip_authz():
    orig = auth.authorize
    auth.authorize = lambda *x: x
    yield
    auth.authorize = orig
