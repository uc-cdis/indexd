from indexd import get_app
import base64
import pytest

# indexd_server and indexd_client is needed as fixtures
from cdisutilstest.code.conftest import indexd_server, indexd_client  # noqa
from cdisutilstest.code.indexd_fixture import clear_database


from indexd import auth
import importlib


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
    app.auth.add("test", "test")
    yield {
        "Authorization": ("Basic " + base64.b64encode(b"test:test").decode("ascii")),
        "Content-Type": "application/json",
    }
    app.auth.delete("test")


@pytest.fixture
def skip_authz():
    orig = auth.authorize
    auth.authorize = lambda *x: x
    yield
    auth.authorize = orig
