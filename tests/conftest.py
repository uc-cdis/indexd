from indexd import get_app
import os
import base64
import pytest
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
        os.remove('auth.sq3')
        os.remove('index.sq3')
        os.remove('alias.sq3')
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
