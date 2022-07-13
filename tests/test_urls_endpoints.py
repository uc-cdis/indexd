import random

import pytest

from tests.test_client import get_doc


@pytest.fixture(scope="function")
def test_data(swg_index_client):
    system_random = random.SystemRandom()
    url_x_count = system_random.randint(2, 5)

    url_x_type = url_x_count
    url_x = "s3://awesome-x/bucket/key"

    versioned_count = system_random.randint(5, 10)
    for _ in range(versioned_count):
        doc = get_doc(version="1")
        if url_x_type > 0:
            doc["urls"].append(url_x)
            doc["urls_metadata"][url_x] = {"state": "uploaded"}
            url_x_type -= 1
        swg_index_client.add_index_entry(doc)

    url_x_type = url_x_count
    unversioned_count = system_random.randint(6, 10)
    for _ in range(unversioned_count):
        doc = get_doc()
        if url_x_type > 0:
            doc["urls"].append(url_x)
            doc["urls_metadata"][url_x] = {"state": "uploaded"}
            url_x_type -= 1
        swg_index_client.add_index_entry(doc)

    url_x_type = url_x_count
    deleted_count = system_random.randint(6, 10)
    for _ in range(deleted_count):
        doc = get_doc(has_metadata=False)
        doc["metadata"] = {"deleted": "True"}
        if url_x_type > 0:
            doc["urls"].append(url_x)
            doc["urls_metadata"][url_x] = {"state": "uploaded"}
            url_x_type -= 1
        swg_index_client.add_index_entry(doc)

    return url_x_count, versioned_count, unversioned_count, deleted_count


def test_query_urls(swg_index_client, swg_query_client, test_data):
    """
    Tests query urls endpoint
    Args:
        swg_index_client (swagger_client.api.indexurls_api.IndexApi): index api client
        swg_query_client (swagger_client.api.indexurls_api.IndexurlsApi): urls api client
        test_data (tuple[int, int, int, int]): test data counts
    """
    url_x_count, versioned_count, unversioned_count, deleted_count = test_data
    # test get all
    urls_list = swg_query_client.query_urls()
    print(urls_list)
    assert len(urls_list) == versioned_count + unversioned_count + deleted_count

    # test exclude deleted
    urls_list = swg_query_client.query_urls(exclude_deleted=True)
    assert len(urls_list) == versioned_count + unversioned_count

    # test list versioned urls
    urls_list = swg_query_client.query_urls(versioned=True, exclude_deleted=True)
    assert len(urls_list) == versioned_count

    # test un-versioned
    urls_list = swg_query_client.query_urls(versioned=False, exclude_deleted=True)
    assert len(urls_list) == unversioned_count

    # test exclude url
    urls_list = swg_query_client.query_urls(exclude="awesome-x")
    assert len(urls_list) == versioned_count + unversioned_count + deleted_count - 3 * url_x_count

    # test include
    urls_list = swg_query_client.query_urls(include="awesome-x")
    assert len(urls_list) == 3 * url_x_count


def test_query_urls_metadata(swg_index_client, swg_query_client, test_data):
    """
    Test query urls metadata endpoint
    Args:
        swg_index_client (swagger_client.api.indexurls_api.IndexApi): index api client
        swg_query_client (swagger_client.api.indexurls_api.IndexurlsApi): urls api client
        test_data (tuple[int, int, int, int]: test data counts
    """
    url_x_count, versioned_count, unversioned_count, deleted_count = test_data

    # test get all
    urls_list = swg_query_client.query_urls_metadata(key="state", value="uploaded", url="endpointurl")
    assert len(urls_list) == versioned_count + unversioned_count + deleted_count
    urls_list = swg_query_client.query_urls_metadata(key="state", value="uploaded", url="awesome-x")
    assert len(urls_list) == 3 * url_x_count

    # test exclude deleted with url known to exist in all documents
    urls_list = swg_query_client.query_urls_metadata(
        key="state", value="uploaded", url="endpointurl", exclude_deleted=True
    )
    assert len(urls_list) == versioned_count + unversioned_count

    # test exclude deleted with url known to exist only in a subset of documents
    urls_list = swg_query_client.query_urls_metadata(
        key="state", value="uploaded", url="awesome-x", exclude_deleted=True
    )
    assert len(urls_list) == 2 * url_x_count

    # test un-versioned with url known to exist in all documents
    urls_list = swg_query_client.query_urls_metadata(
        key="state", value="uploaded", url="endpointurl", versioned=False, exclude_deleted=True)
    assert len(urls_list) == unversioned_count

    # test un-versioned with url known to exist only in a subset of documents
    urls_list = swg_query_client.query_urls_metadata(
        key="state", value="uploaded", url="awesome-x", versioned=False, exclude_deleted=True)
    assert len(urls_list) == url_x_count

    # test versioned with url known to exist in all documents
    urls_list = swg_query_client.query_urls_metadata(
        key="state", value="uploaded", url="endpointurl", versioned=True, exclude_deleted=True)
    assert len(urls_list) == versioned_count

    # test versioned with url known to exist only in a subset of documents
    urls_list = swg_query_client.query_urls_metadata(
        key="state", value="uploaded", url="awesome-x", versioned=True, exclude_deleted=True)
    assert len(urls_list) == url_x_count

    # test unknown state
    urls_list = swg_query_client.query_urls_metadata(key="state", value="uploadedx", url="awesome-x")
    assert len(urls_list) == 0


def test_status(client):
    resp = client.get("/_status")
    assert resp.status_code == 200
    assert resp.data == b"Healthy"
