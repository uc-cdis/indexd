import pytest


def _get(client, url, expected_status, user_, mock_arborist_requests) -> dict:
    mock_arborist_requests(resource_method_to_authorized=user_["permissions"])
    res = client.get(url, headers=user_["header"])
    assert (
        res.status_code == expected_status
    ), f"{url} Expected status {expected_status}, got {res.status_code} {res.text} for user {user_['header']}"
    return res.json


def ensure_drs(
    client, did, expected_count, expected_status, mock_arborist_requests, user_
):
    url = f"/ga4gh/drs/v1/objects"
    response = _get(client, url, expected_status, user_, mock_arborist_requests)
    if expected_status == 200:
        assert (
            "drs_objects" in response
        ), f"Expected 'drs_objects' in response, got {response} for user {user_['header']}"
        drs_objects = response["drs_objects"]
        assert isinstance(
            drs_objects, list
        ), f"Expected 'drs_objects' to be a list, got {type(drs_objects)} for user {user_['header']}"
        assert (
            len(drs_objects) == expected_count
        ), f"Expected non-empty 'drs_objects', got {drs_objects} for user {user_['header']}"
        if expected_count > 0:
            record = drs_objects[0]
            assert (
                "id" in record
            ), f"Expected 'id' in drs_object, got {record} for user {user_['header']}"
            assert (
                record["id"] == did
            ), f"Expected DID {did}, got {record['id']} for user {user_['header']}"
    url = f"/ga4gh/drs/v1/objects/{did}"
    if expected_count == 0:
        expected_status = 401 if expected_status == 200 else expected_status
    response = _get(client, url, expected_status, user_, mock_arborist_requests)
    if expected_count > 0:
        record = response
        assert (
            "id" in record
        ), f"Expected 'id' in drs_object, got {record} for user {user_['header']}"
        assert (
            record["id"] == did
        ), f"Expected DID {did}, got {record['id']} for user {user_['header']}"


def ensure_get_urls(
    client, did, expected_count, expected_status, mock_arborist_requests, user_
):
    url = "/urls"
    response = _get(client, url, expected_status, user_, mock_arborist_requests)
    if expected_status == 200:
        assert (
            "urls" in response
        ), f"Expected 'urls' in response, got {response} for user {user_['header']}"
        urls = response["urls"]
        assert isinstance(
            urls, list
        ), f"Expected 'urls' to be a list, got {type(urls)} for user {user_['header']}"
        assert (
            len(urls) == expected_count
        ), f"Expected non-empty 'urls', got {urls} for user {user_['header']}"
        if expected_count > 0:
            record = urls[0]
            assert (
                "metadata" in record
            ), f"Expected 'metadata' in url, got {record} for user {user_['header']}"
            assert (
                "url" in record
            ), f"Expected 'url' in url, got {record} for user {user_['header']}"


@pytest.mark.parametrize(
    "record_fixture, authz_fixture, user_fixture, expected_count",
    [
        ("public_record", "public_authz", "power_user", 1),
        ("public_record", "public_authz", "basic_user", 1),
        ("public_record", "public_authz", "null_user", 1),
        ("public_record", "public_authz", "controlled_user", 1),
        ("public_record", "public_authz", "discovery_user", 1),
        ("controlled_record", "controlled_authz", "power_user", 1),
        ("controlled_record", "controlled_authz", "basic_user", 0),
        ("controlled_record", "controlled_authz", "null_user", 0),
        ("controlled_record", "controlled_authz", "controlled_user", 1),
        ("controlled_record", "controlled_authz", "discovery_user", 1),
        ("private_record", "private_authz", "power_user", 1),
        ("private_record", "private_authz", "basic_user", 0),
        ("private_record", "private_authz", "null_user", 0),
        ("private_record", "private_authz", "controlled_user", 0),
        ("private_record", "private_authz", "discovery_user", 1),
    ],
)
def test_client_get_index_access(
    client,
    request,
    record_fixture,
    authz_fixture,
    user_fixture,
    expected_count,
    mock_arborist_requests,
):
    """
    Test access to /index/ for all record and user combinations.
    """
    user_ = request.getfixturevalue(user_fixture)
    authz = request.getfixturevalue(authz_fixture)
    _ = request.getfixturevalue(record_fixture)  # ensure record exists

    rec = _get(client, "/index/", 200, user_, mock_arborist_requests)

    assert (
        len(rec["records"]) == expected_count
    ), f"Response should contain {expected_count} record {rec} user: {user_}"
    if expected_count > 0:
        assert (
            authz in rec["records"][0]["authz"]
        ), f"Record should have the correct authz {authz}, got {rec}"


@pytest.mark.parametrize(
    "record_fixture, user_fixture, expected_status",
    [
        ("public_record", "power_user", 200),
        ("public_record", "controlled_user", 200),
        ("public_record", "basic_user", 200),
        ("public_record", "null_user", 200),
        ("public_record", "discovery_user", 200),
        ("controlled_record", "power_user", 200),
        ("controlled_record", "controlled_user", 200),
        ("controlled_record", "basic_user", 401),
        ("controlled_record", "null_user", 401),
        ("controlled_record", "discovery_user", 200),
        ("private_record", "power_user", 200),
        ("private_record", "controlled_user", 401),
        ("private_record", "basic_user", 401),
        ("private_record", "null_user", 401),
        ("private_record", "discovery_user", 200),
    ],
)
def test_get_index_record(
    client,
    request,
    record_fixture,
    user_fixture,
    expected_status,
    mock_arborist_requests,
):
    """
    Test GET /index/{did} for all record and user combinations.
    """
    record = request.getfixturevalue(record_fixture)
    did = record["did"]
    user_ = request.getfixturevalue(user_fixture)
    url = f"/index/{did}"
    mock_arborist_requests(resource_method_to_authorized=user_["permissions"])
    res = client.get(url, headers=user_["header"])
    assert (
        res.status_code == expected_status
    ), f"GET /index/{did} expected {expected_status}, got {res.status_code} for user {user_['header']}"
    if expected_status == 200:
        data = res.json
        assert (
            data["did"] == did
        ), f"Expected DID {did}, got {data['did']} for user {user_['header']}"


@pytest.mark.parametrize(
    "record_fixture, user_fixture, expected_status, expected_count",
    [
        ("public_record", "power_user", 200, 1),
        ("public_record", "controlled_user", 200, 1),
        ("public_record", "basic_user", 200, 1),
        ("public_record", "null_user", 200, 1),
        ("public_record", "discovery_user", 200, 1),
        ("controlled_record", "power_user", 200, 1),
        ("controlled_record", "controlled_user", 200, 1),
        ("controlled_record", "basic_user", 200, 0),
        ("controlled_record", "null_user", 200, 0),
        ("controlled_record", "discovery_user", 200, 1),
        ("private_record", "power_user", 200, 1),
        ("private_record", "controlled_user", 200, 0),
        ("private_record", "basic_user", 200, 0),
        ("private_record", "null_user", 200, 0),
        ("private_record", "discovery_user", 200, 1),
    ],
)
def test_get_drs_objects(
    client,
    request,
    record_fixture,
    user_fixture,
    expected_status,
    expected_count,
    mock_arborist_requests,
):
    """
    Test /ga4gh/drs/v1/objects for all record and user combinations.
    """
    record = request.getfixturevalue(record_fixture)
    did = record["did"]
    user_ = request.getfixturevalue(user_fixture)

    ensure_drs(
        client, did, expected_count, expected_status, mock_arborist_requests, user_
    )


@pytest.mark.parametrize(
    "record_fixture, user_fixture, expected_status, expected_count",
    [
        ("public_record", "power_user", 200, 1),
        ("public_record", "controlled_user", 200, 1),
        ("public_record", "basic_user", 200, 1),
        ("public_record", "null_user", 200, 1),
        ("public_record", "discovery_user", 200, 1),
        ("controlled_record", "power_user", 200, 1),
        ("controlled_record", "controlled_user", 200, 1),
        ("controlled_record", "basic_user", 200, 0),
        ("controlled_record", "null_user", 200, 0),
        ("controlled_record", "discovery_user", 200, 1),
        ("private_record", "power_user", 200, 1),
        ("private_record", "controlled_user", 200, 0),
        ("private_record", "basic_user", 200, 0),
        ("private_record", "null_user", 200, 0),
        ("private_record", "discovery_user", 200, 1),
    ],
)
def test_get_urls(
    client,
    request,
    record_fixture,
    user_fixture,
    expected_status,
    expected_count,
    mock_arborist_requests,
):
    """
    Test /urls for all record and user combinations.
    """
    record = request.getfixturevalue(record_fixture)
    did = record["did"]
    user_ = request.getfixturevalue(user_fixture)

    ensure_get_urls(
        client, did, expected_count, expected_status, mock_arborist_requests, user_
    )


@pytest.mark.parametrize(
    "record_fixture, user_fixture, expected_status",
    [
        ("public_record", "power_user", 200),
        ("public_record", "controlled_user", 200),
        ("public_record", "basic_user", 200),
        ("public_record", "null_user", 200),
        ("public_record", "discovery_user", 200),
        ("controlled_record", "power_user", 200),
        ("controlled_record", "controlled_user", 200),
        ("controlled_record", "basic_user", 401),
        ("controlled_record", "null_user", 401),
        ("controlled_record", "discovery_user", 200),
        ("private_record", "power_user", 200),
        ("private_record", "controlled_user", 401),
        ("private_record", "basic_user", 401),
        ("private_record", "null_user", 401),
        ("private_record", "discovery_user", 200),
    ],
)
def test_get_index_record_by_did(
    client,
    request,
    mock_arborist_requests,
    record_fixture,
    user_fixture,
    expected_status,
):
    """
    Test GET /{did} for each record and user type.
    """
    record = request.getfixturevalue(record_fixture)
    user_ = request.getfixturevalue(user_fixture)
    did = record["did"]
    mock_arborist_requests(resource_method_to_authorized=user_["permissions"])
    res = client.get(f"/index/{did}", headers=user_["header"])
    assert (
        res.status_code == expected_status
    ), f"GET /index/{did} expected {expected_status}, got {res.status_code} for user {user_['header']}"


@pytest.mark.parametrize(
    "record_fixture, user_fixture, expected_status, expected_count",
    [
        ("public_record", "power_user", 200, 1),
        ("public_record", "controlled_user", 200, 1),
        ("public_record", "basic_user", 200, 1),
        ("public_record", "null_user", 200, 1),
        ("public_record", "discovery_user", 200, 1),
        ("controlled_record", "power_user", 200, 1),
        ("controlled_record", "controlled_user", 200, 1),
        ("controlled_record", "basic_user", 401, 0),
        ("controlled_record", "null_user", 401, 0),
        ("controlled_record", "discovery_user", 200, 1),
        ("private_record", "power_user", 200, 1),
        ("private_record", "controlled_user", 401, 0),
        ("private_record", "basic_user", 401, 0),
        ("private_record", "null_user", 401, 0),
        ("private_record", "discovery_user", 200, 1),
    ],
)
def test_get_index_aliases(
    client,
    request,
    record_fixture,
    user_fixture,
    expected_status,
    expected_count,
    mock_arborist_requests,
):
    """
    Test GET /index/{did}/aliases for all record and user combinations.
    """
    record = request.getfixturevalue(record_fixture)
    did = record["did"]
    user_ = request.getfixturevalue(user_fixture)
    mock_arborist_requests(resource_method_to_authorized=user_["permissions"])
    url = f"/index/{did}/aliases"
    res = client.get(url, headers=user_["header"])
    assert (
        res.status_code == expected_status
    ), f"GET {url} expected {expected_status}, got {res.status_code} for user {user_['header']} {res.text}"
    if expected_status == 200:
        data = res.json
        assert isinstance(data, dict), f"Expected dict, got {type(data)}"
        assert "aliases" in data, f"Expected 'aliases' in response, got {data}"
        assert (
            len(data["aliases"]) == expected_count
        ), f"Expected {expected_count} aliases, got {len(data['aliases'])} for user {user_['header']}"


@pytest.mark.parametrize(
    "record_fixture, user_fixture, expected_status, expected_count",
    [
        ("public_record", "power_user", 200, 1),
        ("public_record", "controlled_user", 200, 1),
        ("public_record", "basic_user", 200, 1),
        ("public_record", "null_user", 200, 1),
        ("public_record", "discovery_user", 200, 1),
        ("controlled_record", "power_user", 200, 1),
        ("controlled_record", "controlled_user", 200, 1),
        ("controlled_record", "basic_user", 200, 0),
        ("controlled_record", "null_user", 200, 0),
        ("controlled_record", "discovery_user", 200, 1),
        ("private_record", "power_user", 200, 1),
        ("private_record", "controlled_user", 200, 0),
        ("private_record", "basic_user", 200, 0),
        ("private_record", "null_user", 200, 0),
        ("private_record", "discovery_user", 200, 1),
    ],
)
def test_get_dos_dataobjects(
    client,
    request,
    record_fixture,
    user_fixture,
    expected_status,
    expected_count,
    mock_arborist_requests,
):
    """
    Test GET /ga4gh/dos/v1/dataobjects for all record and user combinations.
    """
    record = request.getfixturevalue(record_fixture)
    did = record["did"]
    user_ = request.getfixturevalue(user_fixture)
    mock_arborist_requests(resource_method_to_authorized=user_["permissions"])
    url = f"/ga4gh/dos/v1/dataobjects?ids={did}"
    res = client.get(url, headers=user_["header"])
    assert (
        res.status_code == expected_status
    ), f"GET {url} expected {expected_status}, got {res.status_code} for user {user_['header']} {res.text}"
    if expected_status == 200:
        data = res.json
        assert isinstance(data, dict), f"Expected dict, got {type(data)}"
        assert (
            "data_objects" in data
        ), f"Expected 'data_objects' in response, got {data}"
        assert (
            len(data["data_objects"]) == expected_count
        ), f"Expected {expected_count} data_objects, got {len(data['data_objects'])} for user {user_['header']}"


@pytest.mark.parametrize(
    "record_fixture, user_fixture, expected_status, expected_count",
    [
        ("public_record", "power_user", 200, 1),
        ("public_record", "controlled_user", 200, 1),
        ("public_record", "basic_user", 200, 1),
        ("public_record", "null_user", 200, 1),
        ("public_record", "discovery_user", 200, 1),
        ("controlled_record", "power_user", 200, 1),
        ("controlled_record", "controlled_user", 200, 1),
        ("controlled_record", "basic_user", 200, 0),
        ("controlled_record", "null_user", 200, 0),
        ("controlled_record", "discovery_user", 200, 1),
        ("private_record", "power_user", 200, 1),
        ("private_record", "controlled_user", 200, 0),
        ("private_record", "basic_user", 200, 0),
        ("private_record", "null_user", 200, 0),
        ("private_record", "discovery_user", 200, 1),
    ],
)
def test_get_index_by_project_id_metadata(
    client,
    request,
    record_fixture,
    user_fixture,
    expected_status,
    expected_count,
    mock_arborist_requests,
):
    """
    Test GET /index/?metadata=project_id:{project_id} for all record and user combinations.
    """
    record = request.getfixturevalue(record_fixture)
    project_id = record["metadata"]["project_id"]
    user_ = request.getfixturevalue(user_fixture)
    mock_arborist_requests(resource_method_to_authorized=user_["permissions"])
    url = f"/index/?metadata=project_id:{project_id}"
    res = client.get(url, headers=user_["header"])
    assert (
        res.status_code == expected_status
    ), f"GET {url} expected {expected_status}, got {res.status_code} for user {user_['header']} {res.text}"
    if expected_status == 200:
        data = res.json
        assert isinstance(data, dict), f"Expected dict, got {type(data)}"
        assert "records" in data, f"Expected 'records' in response, got {data}"
        assert (
            len(data["records"]) == expected_count
        ), f"Expected {expected_count} records, got {len(data['records'])} for user {user_['header']}"
        if expected_count > 0:
            assert (
                data["records"][0]["metadata"]["project_id"] == project_id
            ), f"Expected project_id {project_id}, got {data['records'][0]['metadata']['project_id']}"


@pytest.mark.parametrize(
    "record_fixture, user_fixture, expected_status, expected_count",
    [
        ("public_record", "power_user", 200, 1),
        ("public_record", "controlled_user", 200, 1),
        ("public_record", "basic_user", 200, 1),
        ("public_record", "null_user", 200, 1),
        ("public_record", "discovery_user", 200, 1),
        ("controlled_record", "power_user", 200, 1),
        ("controlled_record", "controlled_user", 200, 1),
        ("controlled_record", "basic_user", 200, 0),
        ("controlled_record", "null_user", 200, 0),
        ("controlled_record", "discovery_user", 200, 1),
        ("private_record", "power_user", 200, 1),
        ("private_record", "controlled_user", 200, 0),
        ("private_record", "basic_user", 200, 0),
        ("private_record", "null_user", 200, 0),
        ("private_record", "discovery_user", 200, 1),
    ],
)
def test_query_urls_q(
    client,
    request,
    record_fixture,
    user_fixture,
    expected_status,
    expected_count,
    mock_arborist_requests,
):
    """
    Test GET /_query/urls/q for all record and user combinations.
    """

    # get the record fixture, ensures it was created
    request.getfixturevalue(record_fixture)

    user_ = request.getfixturevalue(user_fixture)
    url = f"/_query/urls/q"

    data = _get(client, url, expected_status, user_, mock_arborist_requests)

    if expected_status == 200:
        assert isinstance(data, list), f"Expected dict, got {type(data)}"
        assert (
            len(data) == expected_count
        ), f"Expected {expected_count} urls, got {len(data)} for user {user_['header']} {data}"
        if expected_count > 0:
            assert "urls" in data[0], f"Expected 'urls' in response, got {data}"
            assert (
                len(data[0]["urls"]) == expected_count
            ), f"Expected {expected_count} urls, got {len(data[0]['urls'])} for user {user_['header']}"


@pytest.mark.parametrize(
    "record_fixture, user_fixture, expected_status, expected_count",
    [
        ("public_record", "power_user", 200, 1),
        ("public_record", "controlled_user", 200, 1),
        ("public_record", "basic_user", 200, 1),
        ("public_record", "null_user", 200, 1),
        ("public_record", "discovery_user", 200, 1),
        ("controlled_record", "power_user", 200, 1),
        ("controlled_record", "controlled_user", 200, 1),
        ("controlled_record", "basic_user", 401, 0),
        ("controlled_record", "null_user", 401, 0),
        ("controlled_record", "discovery_user", 200, 1),
        ("private_record", "power_user", 200, 1),
        ("private_record", "controlled_user", 401, 0),
        ("private_record", "basic_user", 401, 0),
        ("private_record", "null_user", 401, 0),
        ("private_record", "discovery_user", 200, 1),
    ],
)
def test_get_index_versions(
    client,
    request,
    record_fixture,
    user_fixture,
    expected_status,
    expected_count,
    mock_arborist_requests,
):
    """
    Test GET /index/{did}/versions for all record and user combinations.
    """
    record = request.getfixturevalue(record_fixture)
    did = record["did"]
    user_ = request.getfixturevalue(user_fixture)
    mock_arborist_requests(resource_method_to_authorized=user_["permissions"])
    url = f"/index/{did}/versions"
    res = client.get(url, headers=user_["header"])
    assert (
        res.status_code == expected_status
    ), f"GET {url} expected {expected_status}, got {res.status_code} for user {user_['header']} {res.text}"
    if expected_status == 200:
        data = res.json
        assert isinstance(data, dict), f"Expected dict, got {type(data)}"
        # returns a dict indexed by a counter, so we need to check the keys
        assert (
            len(data.keys()) == expected_count
        ), f"Expected {expected_count} versions, got {len(data['versions'])} for user {user_['header']}"
        assert (
            str(expected_count - 1) in data
        ), f"Expected key '{expected_count - 1}' in response, got {data} for user {user_['header']}"
