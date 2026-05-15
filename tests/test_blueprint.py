import pytest
from fastapi import FastAPI

from indexd.bulk.blueprint import router as indexd_bulk_router, set_bulk_config
from indexd.index.blueprint import router as indexd_index_router, set_index_config
from indexd.alias.blueprint import router as indexd_alias_router, set_alias_config

from indexd.index.drivers.alchemy import SQLAlchemyIndexDriver
from indexd.alias.drivers.alchemy import SQLAlchemyAliasDriver

from starlette.testclient import TestClient

DIST_CONFIG = []

INDEX_CONFIG = {
    "driver": SQLAlchemyIndexDriver(
        "postgresql://postgres:postgres@localhost:5432/indexd_tests"  # pragma: allowlist secret
    )
}

ALIAS_CONFIG = {
    "driver": SQLAlchemyAliasDriver(
        "postgresql://postgres:postgres@localhost:5432/indexd_tests"  # pragma: allowlist secret
    )
}


def create_app(index_config=None, alias_config=None, dist_config=None):
    app = FastAPI()
    app.settings = {
        "INDEX": index_config,
        "ALIAS": alias_config,
        "DIST": dist_config or [],
    }
    if index_config:
        set_index_config(app)
        app.include_router(indexd_index_router)
    if alias_config:
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
