import random
import pytest
from tests.test_client import get_doc


@pytest.fixture(scope="function")
def test_data(client, user):
    system_random = random.SystemRandom()
    url_x_count = system_random.randint(2, 5)

    url_x_type = url_x_count
    url_x = "s3://awesome-x/bucket/key"

    versioned_count = system_random.randint(5, 10)
    for _ in range(versioned_count):
        doc = get_doc(has_urls_metadata=True, has_version=True)
        if url_x_type > 0:
            doc["urls"].append(url_x)
            doc["urls_metadata"][url_x] = {"state": "uploaded"}
            url_x_type -= 1
        print(doc)
        res = client.post("/index/", json=doc, headers=user)
        assert res.status_code == 200
    rec = client.get("/index/", json=doc, headers=user)
    assert rec.status_code == 200

    url_x_type = url_x_count
    unversioned_count = system_random.randint(6, 10)
    for _ in range(unversioned_count):
        doc = get_doc(has_urls_metadata=True)
        if url_x_type > 0:
            doc["urls"].append(url_x)
            doc["urls_metadata"][url_x] = {"state": "uploaded"}
            url_x_type -= 1
        print(doc)
        res = client.post("/index/", json=doc, headers=user)
        assert res.status_code == 200
    rec = client.get("/index/", json=doc, headers=user)
    assert rec.status_code == 200
    return url_x_count, versioned_count, unversioned_count


def test_query_urls(client, test_data):
    """
    Args:
        client (test fixture)
        test_data (tuple[int, int, int]:
    """
    url_x_count, versioned_count, unversioned_count = test_data

    # test get all
    res = client.get("/_query/urls/q")
    assert res.status_code == 200
    urls_list = res.json
    print(urls_list)
    assert len(urls_list) == versioned_count + unversioned_count

    # test list versioned urls
    res = client.get("/_query/urls/q?versioned=true")
    assert res.status_code == 200
    urls_list = res.json
    print(urls_list)
    assert len(urls_list) == versioned_count

    # test list un versioned
    res = client.get("/_query/urls/q?versioned=false")
    assert res.status_code == 200
    urls_list = res.json
    print(urls_list)
    assert len(urls_list) == unversioned_count

    # test exclude url
    res = client.get("/_query/urls/q?exclude=awesome-x")
    assert res.status_code == 200
    urls_list = res.json
    print(urls_list)
    assert len(urls_list) == versioned_count + unversioned_count - 2 * url_x_count

    # test include
    res = client.get("/_query/urls/q?include=awesome-x")
    assert res.status_code == 200
    urls_list = res.json
    print(urls_list)
    assert len(urls_list) == 2 * url_x_count

    # test include and exclude
    res = client.get("/_query/urls/q?include=endpointurl&exclude=awesome-x")
    assert res.status_code == 200
    urls_list = res.json
    print(urls_list)
    assert len(urls_list) == versioned_count + unversioned_count - 2 * url_x_count


def test_query_urls_metadata(client, test_data):
    """
    Args:
        client (test fixture)
        test_data (tuple[int, int, int]:
    """
    url_x_count, _, unversioned_count = test_data

    # test get all
    res = client.get("_query/urls/metadata/q?key=state&value=uploaded&url=awesome-x")
    assert res.status_code == 200
    urls_list = res.json
    assert len(urls_list) == 2 * url_x_count

    # test list versioned urls
    res = client.get(
        "_query/urls/metadata/q?key=state&value=uploaded&url=awesome-x&versioned=True"
    )
    assert res.status_code == 200
    urls_list = res.json
    assert len(urls_list) == url_x_count

    # test list un versioned
    res = client.get(
        "_query/urls/metadata/q?key=state&value=uploaded&url=endpointurl&versioned=False"
    )
    assert res.status_code == 200
    urls_list = res.json
    assert len(urls_list) == unversioned_count

    # test unknown state
    res = client.get("_query/urls/metadata/q?key=state&value=uploadedx&url=awesome-x")
    assert res.status_code == 200
    urls_list = res.json
    assert len(urls_list) == 0
