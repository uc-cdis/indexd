import json
import requests
from urllib.parse import urlparse

import pytest
import indexclient.indexclient.client as indexclient_module
from indexclient.indexclient.client import IndexClient

from tests.conftest import clear_database


@pytest.fixture(scope="function")
def indexd_client(app, monkeypatch, user):
    flask_client = app.test_client()
    baseurl = "http://indexd"

    def request(method, url, **kwargs):
        parsed = urlparse(url)

        #
        kwargs.pop("timeout", None)

        if "params" in kwargs:
            kwargs["query_string"] = kwargs.pop("params")

        auth = kwargs.pop("auth", None)
        if auth:
            headers = kwargs.setdefault("headers", {})
            headers.setdefault("Content-Type", "application/json")
            headers["Authorization"] = user["Authorization"]
        flask_resp = flask_client.open(
            path=parsed.path,
            method=method,
            **kwargs,
        )

        resp = requests.Response()
        resp.status_code = flask_resp.status_code
        resp._content = flask_resp.get_data()
        resp.headers = flask_resp.headers
        resp.url = url
        resp.reason = flask_resp.status
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
