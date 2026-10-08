import os
import sys
import logging as stdlib_logging
import cdislogging

import asyncio
from contextlib import asynccontextmanager
from alembic.config import main as alembic_main
from fastapi import FastAPI
from fastapi.responses import JSONResponse
import logging

from gen3authz.client.arborist.async_client import ArboristClient

from indexd.config_helper import validate_config
from indexd.middleware import MergeSlashesMiddleware
from indexd.index.drivers.alchemy import Base as IndexBase
from indexd.alias.drivers.alchemy import Base as AliasBase
from indexd.auth.drivers.alchemy import Base as AuthBase

from .router import router as cross_router, set_cross_config
from .alias.router import router as indexd_alias_router, set_alias_config
from .bulk.router import router as indexd_bulk_router, set_bulk_config
from .dos.router import router as indexd_dos_router, set_dos_config
from .drs.router import router as indexd_drs_router, set_drs_config
from .guid.router import router as indexd_guid_router, set_guid_config
from .index.router import router as indexd_index_router, set_index_config
from .urls.router import router as index_urls_router, set_urls_config

from indexd.errors import IndexdUnexpectedError, RequestTooLargeError, UserError
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

SERVER_LOGGER_NAMES = ("uvicorn", "uvicorn.error", "uvicorn.access")

logger = cdislogging.get_logger(__name__, log_level="debug")


def warn_about_logger():
    raise Exception("Use cdislogging.get_logger instead of app.logger")


def configure_logging() -> None:
    """
    Send the root and web server loggers through this service's logging setup.

    Uvicorn puts its own handlers and levels on its loggers before it imports this
    module, so those are dropped and the loggers are re-parented: server logs then use
    the cdislogging format and follow this service's DEBUG setting.
    """
    cdislogging.get_logger(None, log_level="debug")

    for logger_name in SERVER_LOGGER_NAMES:
        server_logger = stdlib_logging.getLogger(logger_name)
        server_logger.handlers.clear()
        server_logger.setLevel(stdlib_logging.NOTSET)
        server_logger.propagate = True
        server_logger.parent = logger


routers = [
    (indexd_alias_router, {}),
    (indexd_bulk_router, {}),
    (indexd_dos_router, {}),
    (indexd_drs_router, {}),
    (indexd_guid_router, {}),
    (indexd_index_router, {}),
    (index_urls_router, {"prefix": "/_query/urls"}),
    (cross_router, {}),  # must go at bottom since catch all router
]


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    FastAPI Lifespan event. Runs before the server starts accepting requests.
    Perfect for asynchronous database migrations and setups.
    """
    settings = app.settings
    if settings.get("AUTO_MIGRATE", True):
        index_driver = settings["config"]["INDEX"]["driver"]
        alias_driver = settings["config"]["ALIAS"]["driver"]

        engine_name = index_driver.engine.dialect.name
        logger.info(f"Auto migrating. Engine name: {engine_name}")

        if engine_name == "sqlite":
            async with index_driver.engine.begin() as conn:
                await conn.run_sync(IndexBase.metadata.create_all)

            async with alias_driver.engine.begin() as conn:
                await conn.run_sync(AliasBase.metadata.create_all)
                await conn.run_sync(AuthBase.metadata.create_all)

            await index_driver.migrate_index_database()
            await alias_driver.migrate_alias_database()
        else:
            await asyncio.to_thread(alembic_main, ["--raiseerr", "upgrade", "head"])

        # Alembic's fileConfig() may disable existing loggers. Re-apply this
        # service's logging setup so the app and web server keep logging after
        # migrations, the way app_init() set it up.
        configure_logging()
        enable_indexd_loggers()
        logger.info("indexd logging re-initialized after migrations")
    else:
        logger.info("Auto migrations are disabled")

    yield

    if hasattr(settings["config"]["INDEX"]["driver"], "engine"):
        await settings["config"]["INDEX"]["driver"].engine.dispose()
    if hasattr(settings["config"]["ALIAS"]["driver"], "engine"):
        await settings["config"]["ALIAS"]["driver"].engine.dispose()


def app_init(app, settings=None):
    app.__dict__["logger"] = warn_about_logger
    if not settings:
        from .default_settings import settings

    app.settings = settings
    validate_config(settings)

    logger.info("Initializing Arborist client")
    if os.environ.get("ARBORIST_URL"):
        app.arborist_client = ArboristClient(
            arborist_base_url=os.environ["ARBORIST_URL"],
            logger=logger,
        )
    else:
        app.arborist_client = ArboristClient(logger=logger)

    app.auth = settings["auth"]
    app.hostname = os.environ.get("HOSTNAME") or "http://example.io"
    set_cross_config(app)
    set_alias_config(app)
    set_bulk_config(app)
    set_dos_config(app)
    set_drs_config(app)
    set_guid_config(app)
    set_index_config(app)
    set_urls_config(app)

    for router, opts in routers:
        app.include_router(router, **opts)

    configure_logging()
    enable_indexd_loggers()

    logger.info("indexd logging initialized")


def enable_indexd_loggers():
    for name in logging.Logger.manager.loggerDict:
        logging.getLogger(name).disabled = False


# All DRS routes are registered under this prefix (see indexd/drs/router.py).
DRS_PATH_PREFIX = "/ga4gh/drs/v1"


def error_response(request, status_code: int, message: str) -> JSONResponse:
    """
    Build an error body matching the pre-FastAPI blueprint error handlers.

    Flask scoped error handlers per blueprint, so the shape depended on the
    route: the DRS blueprint returned {"msg": ..., "status_code": ...} while
    every other blueprint returned {"error": ...}. FastAPI exception handlers
    are app-wide, so branch on the request path to preserve both shapes.
    """
    if request.url.path.startswith(DRS_PATH_PREFIX):
        content = {"msg": message, "status_code": status_code}
    else:
        content = {"error": message}
    return JSONResponse(status_code=status_code, content=content)


def get_app(settings=None):

    app = FastAPI(title="indexd", redirect_slashes=True, debug=True, lifespan=lifespan)

    # Must wrap the router: the "/{record:path}" catch-all would otherwise match
    # the un-merged path first.
    app.add_middleware(MergeSlashesMiddleware)

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
        return error_response(request, exc.code, exc.message)

    @app.exception_handler(UserError)
    async def handle_user_error(request, exc: UserError):
        return error_response(request, 400, str(exc))

    @app.exception_handler(RequestTooLargeError)
    async def handle_request_too_large_error(request, exc: RequestTooLargeError):
        return error_response(request, exc.code, exc.message)

    @app.exception_handler(AliasNoRecordFound)
    async def handle_alias_no_record_found(request, exc: AliasNoRecordFound):
        return error_response(request, 404, str(exc))

    @app.exception_handler(AliasMultipleRecordsFound)
    async def handle_alias_multiple_records_found(
        request, exc: AliasMultipleRecordsFound
    ):
        return error_response(request, 409, str(exc))

    @app.exception_handler(AliasRevisionMismatch)
    async def handle_alias_revision_mismatch(request, exc: AliasRevisionMismatch):
        return error_response(request, 409, str(exc))

    @app.exception_handler(AuthError)
    async def handle_auth_error(request, exc: AuthError):
        return error_response(request, 403, str(exc))

    @app.exception_handler(AuthzError)
    async def handle_authz_error(request, exc: AuthzError):
        return error_response(request, 401, str(exc))

    @app.exception_handler(UnhealthyCheck)
    async def handle_unhealthy_check(request, exc: UnhealthyCheck):
        return error_response(request, 500, "Unhealthy")

    @app.exception_handler(IndexNoRecordFound)
    async def handle_index_no_record(request, exc: IndexNoRecordFound):
        return error_response(request, 404, str(exc))

    @app.exception_handler(IndexMultipleRecordsFound)
    async def handle_index_multiple_records_found(request, exc):
        return error_response(request, 409, str(exc))

    @app.exception_handler(IndexRevisionMismatch)
    async def handle_index_revision_mismatch(request, exc):
        return error_response(request, 409, str(exc))

    logger.info("Returning app.....")
    return app
