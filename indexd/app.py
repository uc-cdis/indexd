import os
import sys
import cdislogging

from alembic.config import main as alembic_main
from fastapi import FastAPI
from fastapi.responses import JSONResponse

from indexd.config_helper import validate_config
from indexd.index.drivers.alchemy import Base as IndexBase
from indexd.alias.drivers.alchemy import Base as AliasBase
from indexd.auth.drivers.alchemy import Base as AuthBase

from .blueprint import router as cross_router, set_cross_config
from .alias.blueprint import router as indexd_alias_router, set_alias_config
from .bulk.blueprint import router as indexd_bulk_router, set_bulk_config
from .dos.blueprint import router as indexd_dos_router, set_dos_config
from .drs.blueprint import router as indexd_drs_router, set_drs_config
from .guid.blueprint import router as indexd_guid_router, set_guid_config
from .index.blueprint import router as indexd_index_router, set_index_config
from .urls.blueprint import router as index_urls_router, set_urls_config

from indexd.errors import IndexdUnexpectedError, UserError
from indexd.alias.errors import (
    NoRecordFound as AliasNoRecordFound,
    MultipleRecordsFound as AliasMultipleRecordsFound,
    RevisionMismatch as AliasRevisionMismatch,
)
from indexd.auth.errors import AuthError, AuthzError
from indexd.index.errors import (
    UnhealthyCheck,
    MultipleRecordsFound as IndexMultipleRecordsFound,
    RevisionMismatch as IndexRevisionMismatch,
    NoRecordFound as IndexNoRecordFound,
)

logger = cdislogging.get_logger(__name__)


def app_init(app, settings=None):
    if not settings:
        from .default_settings import settings

    app.settings = settings

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

    validate_config(settings)

    app.auth = settings["auth"]
    app.hostname = os.environ.get("HOSTNAME")

    set_cross_config(app)
    set_alias_config(app)
    set_bulk_config(app)
    set_dos_config(app)
    set_drs_config(app)
    set_guid_config(app)
    set_index_config(app)
    set_urls_config(app)

    app.include_router(cross_router)
    app.include_router(indexd_alias_router)
    app.include_router(indexd_bulk_router)
    app.include_router(indexd_dos_router)
    app.include_router(indexd_drs_router)
    app.include_router(indexd_guid_router)
    app.include_router(indexd_index_router)
    app.include_router(index_urls_router, prefix="/_query/urls")


def get_app(settings=None):
    app = FastAPI(title="indexd")

    if "INDEXD_SETTINGS" in os.environ:
        sys.path.append(os.environ["INDEXD_SETTINGS"])

    if not settings:
        try:
            from local_settings import settings
        except ImportError:
            pass

    app_init(app, settings)

    @app.exception_handler(IndexdUnexpectedError)
    async def handle_indexd_unexpected_error(request, exc: IndexdUnexpectedError):
        return JSONResponse(status_code=exc.code, content={"error": exc.message})

    @app.exception_handler(UserError)
    async def handle_user_error(request, exc: UserError):
        return JSONResponse(status_code=400, content={"error": str(exc)})

    @app.exception_handler(AliasNoRecordFound)
    async def handle_alias_no_record_found(request, exc: AliasNoRecordFound):
        return JSONResponse(status_code=404, content={"error": str(exc)})

    @app.exception_handler(AliasMultipleRecordsFound)
    async def handle_alias_multiple_records_found(
        request, exc: AliasMultipleRecordsFound
    ):
        return JSONResponse(status_code=409, content={"error": str(exc)})

    @app.exception_handler(AliasRevisionMismatch)
    async def handle_alias_revision_mismatch(request, exc: AliasRevisionMismatch):
        return JSONResponse(status_code=409, content={"error": str(exc)})

    @app.exception_handler(AuthError)
    async def handle_auth_error(request, exc: AuthError):
        return JSONResponse(status_code=403, content={"error": str(exc)})

    @app.exception_handler(AuthzError)
    async def handle_authz_error(request, exc: AuthzError):
        return JSONResponse(status_code=401, content={"error": str(exc)})

    @app.exception_handler(UnhealthyCheck)
    async def handle_unhealthy_check(request, exc: UnhealthyCheck):
        return JSONResponse(status_code=500, content={"error": "Unhealthy"})

    @app.exception_handler(IndexNoRecordFound)
    async def handle_index_no_record(request, exc: IndexNoRecordFound):
        return JSONResponse(status_code=404, content={"error": str(exc)})

    @app.exception_handler(IndexMultipleRecordsFound)
    async def handle_index_multiple_records_found(request, exc):
        return JSONResponse(status_code=409, content={"error": str(exc)})

    @app.exception_handler(IndexRevisionMismatch)
    async def handle_index_revision_mismatch(request, exc):
        return JSONResponse(status_code=409, content={"error": str(exc)})

    return app
