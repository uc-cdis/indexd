import pytest
import requests
from urllib.parse import urlparse
from fastapi.testclient import TestClient

from indexd.app import get_app
import indexclient.indexclient.client as indexclient_module
from indexclient.indexclient.client import IndexClient
from tests.conftest import clear_database


@pytest.fixture(scope="function")
def test_client():
    app = get_app()
    with TestClient(app) as client:
        yield client


@pytest.fixture(scope="function")
def indexd_client(test_client, monkeypatch, user):
    baseurl = "http://indexd"

    def request(method, url, **kwargs):
        parsed = urlparse(url)
        kwargs.pop("timeout", None)

        if "params" in kwargs:
            kwargs["query_string"] = kwargs.pop("params")

        auth = kwargs.pop("auth", None)
        if auth:
            headers = kwargs.setdefault("headers", {})
            headers.setdefault("Content-Type", "application/json")
            headers["Authorization"] = user["Authorization"]

        test_resp = test_client.request(
            method=method,
            url=parsed.path,
            **kwargs,
        )

        resp = requests.Response()
        resp.status_code = test_resp.status_code
        resp._content = test_resp.content
        resp.headers = test_resp.headers
        resp.url = url
        resp.reason = getattr(test_resp, "reason", "")

        return resp

    monkeypatch.setattr(
        indexclient_module.requests, "get", lambda url, **kw: request("GET", url, **kw)
    )
    monkeypatch.setattr(
        indexclient_module.requests,
        "post",
        lambda url, **kw: request("POST", url, **kw),
    )
    monkeypatch.setattr(
        indexclient_module.requests, "put", lambda url, **kw: request("PUT", url, **kw)
    )
    monkeypatch.setattr(
        indexclient_module.requests,
        "delete",
        lambda url, **kw: request("DELETE", url, **kw),
    )
    monkeypatch.setattr(
        indexclient_module.requests,
        "options",
        lambda url, **kw: request("OPTIONS", url, **kw),
    )

    yield IndexClient(baseurl=baseurl, auth=("test", "test"))
    clear_database()


@pytest.fixture(scope="function")
def index_client(indexd_client):
    return indexd_client
