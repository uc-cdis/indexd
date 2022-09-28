import flask
from .bulk.blueprint import blueprint as indexd_bulk_blueprint
from .index.blueprint import blueprint as indexd_index_blueprint
from .alias.blueprint import blueprint as indexd_alias_blueprint
from .dos.blueprint import blueprint as indexd_dos_blueprint
from .drs.blueprint import blueprint as indexd_guid_blueprint
from .guid.blueprint import blueprint as indexd_drs_blueprint
from .blueprint import blueprint as cross_blueprint

from indexd.urls.blueprint import blueprint as index_urls_blueprint

import os
import sys
import cdislogging


def app_init(app, settings=None):
    app.url_map.strict_slashes = False
    app.logger.addHandler(cdislogging.get_stream_handler())
    if not settings:
        from .default_settings import settings
    app.config.update(settings["config"])
    app.auth = settings["auth"]
    app.hostname = os.environ.get("HOSTNAME") or "http://example.io"
    app.register_blueprint(indexd_bulk_blueprint)
    app.register_blueprint(indexd_index_blueprint)
    app.register_blueprint(indexd_alias_blueprint)
    app.register_blueprint(indexd_dos_blueprint)
    app.register_blueprint(indexd_drs_blueprint)
    app.register_blueprint(indexd_guid_blueprint)
    app.register_blueprint(cross_blueprint)
    app.register_blueprint(index_urls_blueprint, url_prefix="/_query/urls")


def get_app(settings=None):
    app = flask.Flask("indexd")

    if "INDEXD_SETTINGS" in os.environ:
        sys.path.append(os.environ["INDEXD_SETTINGS"])

    if not settings:
        try:
            from local_settings import settings
        except ImportError:
            pass

    app_init(app, settings)

    return app
