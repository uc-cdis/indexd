import flask
import pytest

import indexd
from indexd import errors
from indexd.index.sqlite import SQLiteIndexDriver
from indexd.alias.sqlite import SQLiteAliasDriver


INDEX_CONFIG = {
    'driver': SQLiteIndexDriver,
    'SQLITE3': {
        'host': 'index.sq3',
    }
}

ALIAS_CONFIG = {
    'driver': SQLiteAliasDriver,
    'SQLITE3': {
        'host': 'alias.sq3',
    }
}


def test_flask_blueprint():
    '''
    Tests standing up the server using flask.
    '''
    app = flask.Flask(__name__)

    app.config['INDEX'] = INDEX_CONFIG
    app.config['ALIAS'] = ALIAS_CONFIG

    app.register_blueprint(indexd.blueprint)

def test_flask_blueprint_missing_index_config():
    '''
    Tests standing up the server using flask without an index config.
    '''
    app = flask.Flask(__name__)

    app.config['ALIAS'] = ALIAS_CONFIG

    with pytest.raises(errors.ConfigurationError):
        app.register_blueprint(indexd.blueprint)

def test_flask_blueprint_invalid_index_config():
    '''
    Tests standing up the server using flask without an index config.
    '''
    app = flask.Flask(__name__)

    app.config['INDEX'] = None
    app.config['ALIAS'] = ALIAS_CONFIG

    with pytest.raises(errors.ConfigurationError):
        app.register_blueprint(indexd.blueprint)

def test_flask_blueprint_missing_alias_config():
    '''
    Tests standing up the server using flask without an alias config.
    '''
    app = flask.Flask(__name__)

    app.config['INDEX'] = INDEX_CONFIG

    with pytest.raises(errors.ConfigurationError):
        app.register_blueprint(indexd.blueprint)

def test_flask_blueprint_invalid_alias_config():
    '''
    Tests standing up the server using flask without an alias config.
    '''
    app = flask.Flask(__name__)

    app.config['INDEX'] = INDEX_CONFIG
    app.config['ALIAS'] = None

    with pytest.raises(errors.ConfigurationError):
        app.register_blueprint(indexd.blueprint)
