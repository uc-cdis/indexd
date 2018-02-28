import flask
from .index.blueprint import blueprint as indexd_index_blueprint
from .alias.blueprint import blueprint as indexd_alias_blueprint
from .blueprint import blueprint as cross_blueprint
import os
import sys
import cdislogging


def app_init(app, settings=None):
    app.logger.addHandler(cdislogging.get_stream_handler())
    if not settings:
        from .default_settings import settings
    app.config.update(settings['config'])
    app.auth = settings['auth']
    app.register_blueprint(indexd_index_blueprint)
    app.register_blueprint(indexd_alias_blueprint)
    app.register_blueprint(cross_blueprint)


def get_app():
    app = flask.Flask('indexd')

    if 'INDEXD_SETTINGS' in os.environ:
        sys.path.append(os.environ['INDEXD_SETTINGS'])

    settings = None
    try:
        from local_settings import settings
    except ImportError:
        pass

    app_init(app, settings)

    return app
