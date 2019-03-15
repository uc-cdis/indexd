import random

import pytest

from tests.test_client import get_doc


@pytest.fixture(scope="function")
def test_data(swg_index_client, create_indexd_tables):
    system_random = random.SystemRandom()
    url_x_count = system_random.randint(2, 5)

    url_x_type = url_x_count
    url_x = "s3://awesome-x/bucket/key"

    versioned_count = system_random.randint(5, 10)
    for _ in range(versioned_count):
        doc = get_doc(has_version=True)
        if url_x_type > 0:
            doc["urls"].append(url_x)
            doc["urls_metadata"][url_x] = {"state": "uploaded"}
            url_x_type -= 1
        swg_index_client.add_entry(doc)

    url_x_type = url_x_count
    unversioned_count = system_random.randint(6, 10)
    for _ in range(unversioned_count):
        doc = get_doc()
        if url_x_type > 0:
            doc["urls"].append(url_x)
            doc["urls_metadata"][url_x] = {"state": "uploaded"}
            url_x_type -= 1
        swg_index_client.add_entry(doc)
    return url_x_count, versioned_count, unversioned_count


def test_query_urls(swg_index_client, swg_query_client, test_data, create_indexd_tables):
    """
    Args:
        swg_index_client (swagger_client.api.indexurls_api.IndexApi):
        swg_query_client (swagger_client.api.indexurls_api.IndexurlsApi): urls api client
        test_data (tuple[int, int, int]:
    """
    url_x_count, versioned_count, unversioned_count = test_data
    # test get all
    urls_list = swg_query_client.query_urls()
    print(urls_list)
    assert len(urls_list) == versioned_count + unversioned_count

    # test list versioned urls
    urls_list = swg_query_client.query_urls(versioned=True)
    assert len(urls_list) == versioned_count

    # test list un versioned
    urls_list = swg_query_client.query_urls(versioned=False)
    assert len(urls_list) == unversioned_count

    # test exclude url
    urls_list = swg_query_client.query_urls(exclude="awesome-x")
    assert len(urls_list) == versioned_count + unversioned_count - 2 * url_x_count

    # test include
    urls_list = swg_query_client.query_urls(include="awesome-x")
    assert len(urls_list) == 2 * url_x_count


def test_query_urls_metadata(swg_index_client, swg_query_client, test_data):
    """
    Args:
        swg_index_client (swagger_client.api.indexurls_api.IndexApi):
        swg_query_client (swagger_client.api.indexurls_api.IndexurlsApi): urls api client
        test_data (tuple[int, int, int]:
    """
    url_x_count, _, unversioned_count = test_data
    # test get all
    urls_list = swg_query_client.query_urls_metadata(key="state", value="uploaded", url="awesome-x")
    assert len(urls_list) == 2 * url_x_count

    # test list versioned urls
    urls_list = swg_query_client.query_urls_metadata(key="state", value="uploaded",
                                                     url="awesome-x", versioned=True)
    assert len(urls_list) == url_x_count

    # test list un versioned
    urls_list = swg_query_client.query_urls_metadata(key="state", value="uploaded", url="endpointurl", versioned=False)
    assert len(urls_list) == unversioned_count

    # test unknown state
    urls_list = swg_query_client.query_urls_metadata(key="state", value="uploadedx", url="awesome-x")
    assert len(urls_list) == 0
