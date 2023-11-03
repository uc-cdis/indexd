import os
import sys

from alembic.config import main as alembic_main
import cdislogging
import flask

from indexd.index.drivers.alchemy import Base as IndexBase
from indexd.alias.drivers.alchemy import Base as AliasBase
from indexd.auth.drivers.alchemy import Base as AuthBase
from .bulk.blueprint import blueprint as indexd_bulk_blueprint
from .index.blueprint import blueprint as indexd_index_blueprint
from .alias.blueprint import blueprint as indexd_alias_blueprint
from .dos.blueprint import blueprint as indexd_dos_blueprint
from .drs.blueprint import blueprint as indexd_guid_blueprint
from .guid.blueprint import blueprint as indexd_drs_blueprint
from .blueprint import blueprint as cross_blueprint
from indexd.urls.blueprint import blueprint as index_urls_blueprint

logger = cdislogging.get_logger(__name__)


def app_init(app, settings=None):
    app.url_map.strict_slashes = False
    if not settings:
        from .default_settings import settings
    app.config.update(settings["config"])

    if settings.get("AUTO_MIGRATE", True):
        engine_name = settings["config"]["INDEX"]["driver"].engine.dialect.name
        logger.info(f"Auto migrating. Engine name: {engine_name}")
        if engine_name == "sqlite":
            IndexBase.metadata.create_all()
            AliasBase.metadata.create_all()
            AuthBase.metadata.create_all()
            settings["config"]["INDEX"]["driver"].migrate_index_database()
            settings["config"]["ALIAS"]["driver"].migrate_alias_database()
        else:
            alembic_main(["--raiseerr", "upgrade", "head"])
    else:
        logger.info("Auto migrations are disabled")

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
