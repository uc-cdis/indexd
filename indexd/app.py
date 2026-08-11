import os
import sys
import cdislogging

import asyncio
from contextlib import asynccontextmanager
from alembic.config import main as alembic_main
from fastapi import FastAPI
from fastapi.responses import JSONResponse
import httpx

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
            # 1. Asynchronous Table Creation
            async with index_driver.engine.begin() as conn:
                await conn.run_sync(IndexBase.metadata.create_all)

            # Note: Assuming AliasBase and AuthBase are also on the same async engine.
            # If they have their own engines, wrap these in their respective engine.begin() blocks.
            async with alias_driver.engine.begin() as conn:
                await conn.run_sync(AliasBase.metadata.create_all)
                await conn.run_sync(AuthBase.metadata.create_all)

            # 2. Await the async migration methods
            await index_driver.migrate_index_database()
            await alias_driver.migrate_alias_database()
        else:
            # 3. Alembic is inherently synchronous. Run it in a background thread
            # to prevent it from blocking the async event loop during startup.
            await asyncio.to_thread(alembic_main, ["--raiseerr", "upgrade", "head"])
    else:
        logger.info("Auto migrations are disabled")

    yield  # The application serves requests here

    # Graceful shutdown (optional but recommended)
    if hasattr(settings["config"]["INDEX"]["driver"], "engine"):
        await settings["config"]["INDEX"]["driver"].engine.dispose()
    if hasattr(settings["config"]["ALIAS"]["driver"], "engine"):
        await settings["config"]["ALIAS"]["driver"].engine.dispose()


def app_init(app, settings=None):
    if not settings:
        from .default_settings import settings

    app.settings = settings
    validate_config(settings)

    app.auth = settings["auth"]
    app.hostname = os.environ.get("HOSTNAME") or "http://example.io"
    logger.info("Setting config for APIs.........")
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

    logger.info("Done setting configs for APIs.........")


def get_app(settings=None):
    logger.info("Starting get_app............")

    # 4. Attach the lifespan context manager to the FastAPI instance
    app = FastAPI(title="indexd", redirect_slashes=True, lifespan=lifespan)

    if "INDEXD_SETTINGS" in os.environ:
        sys.path.append(os.environ["INDEXD_SETTINGS"])

    if not settings:
        try:
            from local_settings import settings
        except ImportError:
            pass

    logger.info("Starting app_init.............")
    app_init(app, settings)
    logger.info("Done with app_init............")

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

    logger.info("Returning app.....")
    return app
