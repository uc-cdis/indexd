import pytest
import copy


@pytest.mark.parametrize(
    "user_fixture, record_fixture, new_hash, new_url, expected_status",
    [
        (
            "power_user",
            "public_record",
            {"md5": "e99a18c428cb38d5f260853678922e03"},
            "s3://bucket/newkey1",
            200,
        ),
        (
            "controlled_user",
            "controlled_record",
            {"md5": "5d41402abc4b2a76b9719d911017c592"},
            "s3://bucket/newkey2",
            200,
        ),
        (
            "basic_user",
            "public_record",
            {"md5": "098f6bcd4621d373cade4e832627b4f6"},
            "s3://bucket/newkey3",
            401,
        ),
        (
            "null_user",
            "public_record",
            {"md5": "ad0234829205b9033196ba818f7a872b"},
            "s3://bucket/newkey4",
            401,
        ),
        (
            "discovery_user",
            "public_record",
            {"md5": "1a79a4d60de6718e8e5b326e338ae533"},
            "s3://bucket/newkey5",
            401,
        ),
    ],
)
def test_post_index(
    client,
    request,
    user_fixture,
    record_fixture,
    new_hash,
    new_url,
    expected_status,
    mock_arborist_requests,
):
    """
    Test POST /index using existing record fixtures as base content, but with modified hashes and urls.
    """
    base_record = copy.deepcopy(request.getfixturevalue(record_fixture))
    user_ = request.getfixturevalue(user_fixture)
    # the fixtures were updated with the results of a previous POST, so we need to remove these fields
    del base_record["did"]
    del base_record["aliases"]
    del base_record["rev"]

    mock_arborist_requests(resource_method_to_authorized=user_["permissions"])
    url = "/index/"
    res = client.post(url, json=base_record, headers=user_["header"])
    assert (
        res.status_code == expected_status
    ), f"POST {url} expected {expected_status}, got {res.status_code} for user {user_['header']} {res.text}"
    if expected_status == 200:
        data = res.json
        assert "did" in data


@pytest.mark.parametrize(
    "user_fixture, record_fixture, new_alias, expected_status",
    [
        ("power_user", "public_record", "new-alias-1", 200),
        ("controlled_user", "controlled_record", "new-alias-2", 200),
        # NOTE: Existing code base authorizes basic auth users
        ("basic_user", "public_record", "new-alias-3", 200),
        ("null_user", "public_record", "new-alias-4", 401),
        ("discovery_user", "public_record", "new-alias-4", 401),
    ],
)
def test_post_index_aliases(
    client,
    request,
    user_fixture,
    record_fixture,
    new_alias,
    expected_status,
    mock_arborist_requests,
):
    """
    Test POST /index/{did}/aliases to add a new alias to an existing record.
    """
    base_record = copy.deepcopy(request.getfixturevalue(record_fixture))
    user_ = request.getfixturevalue(user_fixture)
    did = base_record["did"]
    url = f"/index/{did}/aliases"
    body = {"aliases": [{"value": new_alias}]}
    mock_arborist_requests(resource_method_to_authorized=user_["permissions"])
    res = client.post(url, json=body, headers=user_["header"])
    assert (
        res.status_code == expected_status
    ), f"POST {url} expected {expected_status}, got {res.status_code} for user {user_['header']} {res.text}"
