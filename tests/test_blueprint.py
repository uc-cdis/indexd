import flask
import pytest

import tests.util as util

from indexd.bulk.blueprint import blueprint as indexd_bulk_blueprint
from indexd.index.blueprint import blueprint as indexd_index_blueprint
from indexd.alias.blueprint import blueprint as indexd_alias_blueprint

from indexd.index.drivers.alchemy import SQLAlchemyIndexDriver
from indexd.alias.drivers.alchemy import SQLAlchemyAliasDriver


DIST_CONFIG = []

INDEX_CONFIG = {"driver": SQLAlchemyIndexDriver("sqlite:///index.sq3")}

ALIAS_CONFIG = {"driver": SQLAlchemyAliasDriver("sqlite:///alias.sq3")}


@util.removes("index.sq3")
@util.removes("alias.sq3")
def test_flask_blueprint():
    """
    Tests standing up the server using flask.
    """
    app = flask.Flask(__name__)

    app.config["INDEX"] = INDEX_CONFIG
    app.config["ALIAS"] = ALIAS_CONFIG
    app.config["DIST"] = []

    app.register_blueprint(indexd_bulk_blueprint)
    app.register_blueprint(indexd_index_blueprint)
    app.register_blueprint(indexd_alias_blueprint)


@util.removes("alias.sq3")
def test_flask_blueprint_missing_index_config():
    """
    Tests standing up the server using flask without an index config.
    """
    app = flask.Flask(__name__)

    app.config["ALIAS"] = ALIAS_CONFIG
    app.config["DIST"] = []

    with pytest.raises(Exception):
        app.register_blueprint(indexd_index_blueprint)

    app.register_blueprint(indexd_alias_blueprint)


@util.removes("alias.sq3")
def test_flask_blueprint_invalid_index_config():
    """
    Tests standing up the server using flask without an index config.
    """
    app = flask.Flask(__name__)

    app.config["INDEX"] = None
    app.config["ALIAS"] = ALIAS_CONFIG
    app.config["DIST"] = []

    with pytest.raises(Exception):
        app.register_blueprint(indexd_index_blueprint)

    app.register_blueprint(indexd_alias_blueprint)


@util.removes("index.sq3")
def test_flask_blueprint_missing_alias_config():
    """
    Tests standing up the server using flask without an alias config.
    """
    app = flask.Flask(__name__)

    app.config["INDEX"] = INDEX_CONFIG
    app.config["DIST"] = []

    with pytest.raises(Exception):
        app.register_blueprint(indexd_alias_blueprint)

    app.register_blueprint(indexd_index_blueprint)


@util.removes("index.sq3")
def test_flask_blueprint_invalid_alias_config():
    """
    Tests standing up the server using flask without an alias config.
    """
    app = flask.Flask(__name__)

    app.config["INDEX"] = INDEX_CONFIG
    app.config["ALIAS"] = None
    app.config["DIST"] = []

    with pytest.raises(Exception):
        app.register_blueprint(indexd_alias_blueprint)

    app.register_blueprint(indexd_index_blueprint)
