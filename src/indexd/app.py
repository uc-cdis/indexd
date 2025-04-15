import logging
import os
import sys
from typing import Any

import flask

from indexd.alias.blueprint import blueprint as indexd_alias_blueprint
from indexd.blueprint import blueprint as cross_blueprint
from indexd.bulk.blueprint import blueprint as indexd_bulk_blueprint
from indexd.index.blueprint import blueprint as indexd_index_blueprint
from indexd.urls.blueprint import blueprint as index_urls_blueprint

logger = logging.getLogger(__name__)


def app_init(app: flask.Flask, settings=None):
    if not settings:
        from .default_settings import settings
    app.config.update(settings["config"])
    app.auth = settings["auth"]
    app.register_blueprint(indexd_bulk_blueprint)
    app.register_blueprint(indexd_index_blueprint)
    app.register_blueprint(indexd_alias_blueprint)
    app.register_blueprint(cross_blueprint)
    app.register_blueprint(index_urls_blueprint, url_prefix="/_query/urls")


def get_app(_settings: dict[str, Any] | str | None = None):
    app = flask.Flask(__name__)

    if "INDEXD_SETTINGS" in os.environ:
        sys.path.append(os.environ["INDEXD_SETTINGS"])

    settings = _settings

    try:
        from local_settings import settings
    except ImportError:
        pass

    app_init(app, settings)
    logger.debug("IndexD initialized successfully.")
    return app
