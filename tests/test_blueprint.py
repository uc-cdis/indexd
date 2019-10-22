import flask
import pytest
from indexd.alias.blueprint import blueprint as indexd_alias_blueprint
from indexd.bulk.blueprint import blueprint as indexd_bulk_blueprint
from indexd.index.blueprint import blueprint as indexd_index_blueprint


def test_flask_blueprint(index_driver, alias_driver):
    """
    Tests standing up the server using flask.
    """
    app = flask.Flask(__name__)

    app.config['INDEX'] = {'driver': index_driver}
    app.config['ALIAS'] = {'driver': alias_driver}
    app.config['DIST'] = []

    app.register_blueprint(indexd_bulk_blueprint)
    app.register_blueprint(indexd_index_blueprint)
    app.register_blueprint(indexd_alias_blueprint)


def test_flask_blueprint_missing_index_config(alias_driver):
    """
    Tests standing up the server using flask without an index config.
    """
    app = flask.Flask(__name__)

    app.config['ALIAS'] = {'driver': alias_driver}
    app.config['DIST'] = []

    with pytest.raises(Exception):
        app.register_blueprint(indexd_index_blueprint)

    app.register_blueprint(indexd_alias_blueprint)


def test_flask_blueprint_invalid_index_config(alias_driver):
    """
    Tests standing up the server using flask without an index config.
    """
    app = flask.Flask(__name__)

    app.config['INDEX'] = None
    app.config['ALIAS'] = {'driver': alias_driver}
    app.config['DIST'] = []

    with pytest.raises(Exception):
        app.register_blueprint(indexd_index_blueprint)

    app.register_blueprint(indexd_alias_blueprint)


def test_flask_blueprint_missing_alias_config(index_driver):
    """
    Tests standing up the server using flask without an alias config.
    """
    app = flask.Flask(__name__)

    app.config['INDEX'] = {'driver': index_driver}
    app.config['DIST'] = []

    with pytest.raises(Exception):
        app.register_blueprint(indexd_alias_blueprint)

    app.register_blueprint(indexd_index_blueprint)


def test_flask_blueprint_invalid_alias_config(index_driver):
    """
    Tests standing up the server using flask without an alias config.
    """
    app = flask.Flask(__name__)

    app.config['INDEX'] = {'driver': index_driver}
    app.config['ALIAS'] = None
    app.config['DIST'] = []

    with pytest.raises(Exception):
        app.register_blueprint(indexd_alias_blueprint)

    app.register_blueprint(indexd_index_blueprint)
