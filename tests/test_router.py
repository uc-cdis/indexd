import pytest
from fastapi import FastAPI

from indexd.bulk.router import router as indexd_bulk_router, set_bulk_config
from indexd.index.router import router as indexd_index_router, set_index_config
from indexd.alias.router import router as indexd_alias_router, set_alias_config

from indexd.index.drivers.alchemy import SQLAlchemyIndexDriver
from indexd.alias.drivers.alchemy import SQLAlchemyAliasDriver

from starlette.testclient import TestClient

from indexd.middleware import MergeSlashesMiddleware

DIST_CONFIG = []

INDEX_CONFIG = {
    "driver": SQLAlchemyIndexDriver(
        "postgresql+asyncpg://postgres:postgres@localhost:5432/indexd_tests"  # pragma: allowlist secret
    )
}

ALIAS_CONFIG = {
    "driver": SQLAlchemyAliasDriver(
        "postgresql+asyncpg://postgres:postgres@localhost:5432/indexd_tests"  # pragma: allowlist secret
    )
}


def create_app(index_config=None, alias_config=None, dist_config=None):
    app = FastAPI(title="indexd", redirect_slashes=True)
    app.settings = {"config": {"DIST": dist_config or []}}
    if index_config:
        app.settings["config"]["INDEX"] = index_config
        set_index_config(app)
        app.include_router(indexd_index_router)
    if alias_config:
        app.settings["config"]["ALIAS"] = alias_config
        set_alias_config(app)
        app.include_router(indexd_alias_router)
    set_bulk_config(app)
    app.include_router(indexd_bulk_router)
    return app


def test_fastapi_router_registration():
    """
    Tests standing up the server using FastAPI.
    """
    app = create_app(
        index_config=INDEX_CONFIG, alias_config=ALIAS_CONFIG, dist_config=[]
    )
    client = TestClient(app)
    response = client.get("/index/")
    assert response.status_code == 200


def test_fastapi_missing_index_config():
    """
    Tests standing up the server using FastAPI without an index config.
    """
    app = create_app(alias_config=ALIAS_CONFIG, dist_config=[])
    # If index config is missing, /index/ routes should not be registered
    client = TestClient(app)
    response = client.get("/index/")
    assert response.status_code == 404


def test_fastapi_invalid_index_config():
    """
    Tests standing up the server using FastAPI without an index config.
    """
    app = create_app(index_config=None, alias_config=ALIAS_CONFIG, dist_config=[])
    client = TestClient(app)
    response = client.get("/index/")
    assert response.status_code == 404


def test_fastapi_missing_alias_config():
    """
    Tests standing up the server using FastAPI without an alias config.
    """
    app = create_app(index_config=INDEX_CONFIG, dist_config=[])
    # If alias config is missing, alias routes should not be registered
    client = TestClient(app)
    response = client.get("/alias/")
    assert response.status_code == 404


def test_fastapi_invalid_alias_config():
    """
    Tests standing up the server using FastAPI without an alias config.
    """
    app = create_app(index_config=INDEX_CONFIG, alias_config=None, dist_config=[])
    client = TestClient(app)
    response = client.get("/alias/")
    assert response.status_code == 404


def create_merge_slashes_app():
    """
    Minimal stand-in for the real app's route shape: a prefixed route plus the
    "/{record:path}" catch-all that indexd/router.py registers last.
    """
    app = FastAPI(title="indexd", redirect_slashes=True)
    app.add_middleware(MergeSlashesMiddleware)

    @app.get("/index/{record:path}")
    async def get_index(record: str):
        return {"hit": "index", "record": record}

    @app.put("/index/{record:path}")
    async def put_index(record: str):
        return {"hit": "index", "record": record}

    @app.get("/{record:path}")
    async def catch_all(record: str):
        return {"hit": "catch-all", "record": record}

    return TestClient(app)


@pytest.mark.parametrize(
    "path",
    [
        "/index/dg.4503/abc-123",
        "//index/dg.4503/abc-123",
        "///index/dg.4503/abc-123",
        "//index//dg.4503/abc-123",
    ],
)
def test_repeated_slashes_are_merged(path):
    """
    Werkzeug merged repeated slashes for us under Flask, so clients calling
    "//index/<guid>" kept working. Starlette does not, and the un-merged path
    would otherwise be swallowed by the "/{record:path}" catch-all.
    """
    client = create_merge_slashes_app()
    response = client.request("GET", "http://testserver" + path)
    assert response.status_code == 200
    assert response.json() == {"hit": "index", "record": "dg.4503/abc-123"}


def test_repeated_slashes_merged_for_non_get_verbs():
    """The catch-all only serves GET, so without merging a PUT 405s."""
    client = create_merge_slashes_app()
    response = client.request("PUT", "http://testserver//index/dg.4503/abc-123")
    assert response.status_code == 200
    assert response.json() == {"hit": "index", "record": "dg.4503/abc-123"}


def test_unmatched_path_still_reaches_catch_all():
    """Merging must not divert paths the catch-all is supposed to resolve."""
    client = create_merge_slashes_app()
    response = client.request("GET", "http://testserver//some-guid")
    assert response.status_code == 200
    assert response.json() == {"hit": "catch-all", "record": "some-guid"}
