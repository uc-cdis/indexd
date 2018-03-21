from indexd import get_app
import os
import base64
import pytest
from cdisutilstest.code.conftest import indexd_server, indexd_client
from cdisutilstest.code.indexd_fixture import clear_database
import swagger_client


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
    reload(default_settings)
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
def swg_client(indexd_client): # noqa
    config = swagger_client.Configuration()
    config.host = indexd_client.url
    config.username = indexd_client.auth[0]
    config.password = indexd_client.auth[1]
    api = swagger_client.IndexApi(swagger_client.ApiClient(config))
    yield api
