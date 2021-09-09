import pytest
import string
import json
import urllib.parse


# Test fixtures and helper functions
# =============================================
def url_encode(str):
    return urllib.parse.quote(str, safe="")


def get_endpoint(guid):
    return "/index/{}/aliases".format(guid)


def to_payload(aliases):
    """
    Boxes a list of aliases into a JSON payload object expected
    by the server.
    """
    return {"aliases": [{"value": alias} for alias in aliases]}


def payload_to_list(alias_payload):
    """
    Unboxes a JSON payload object expected by the server into
    a list of alias names.
    """
    return [record["value"] for record in alias_payload["aliases"]]


def create_record(client, user):
    """
    Creates a record in indexd and returns that record's GUID.
    """
    document = {
        "form": "object",
        "size": 123,
        "urls": ["s3://endpointurl/bucket/key"],
        "hashes": {"md5": "8b9942cf415384b27cadf1f4d2d682e5"},
        "metadata": {"project_id": "bpa-UChicago"},
    }
    res = client.post("/index/", json=document, headers=user)
    assert res.status_code == 200
    # The GUID is the "did" (Document IDentifier) returned from a successful
    # POST request.
    guid = res.get_json()["did"]

    return guid


@pytest.fixture(scope="function")
def guid(client, user):
    """
    Creates a record in indexd and returns that record's GUID.
    """
    return create_record(client, user)


@pytest.fixture(scope="function")
def aliases(client, user, guid):
    """
    Associates aliases with a GUID in indexd and returns the new aliases.
    """
    aliases = [
        ".?=G}k@Up3LIlv+p96yaI06,t@?j=ejk[%+",
        'Fa"uW< A"/\'hELmTjH%r%6@Tp^HaB^',
        "{j'8D6d5fc]5#[*9n%|G9\"hZ?z3:wX",
        '"U7XsT+EXD|1?@$ywDV"ce<B}7v9t)',
    ]
    alias_payload = to_payload(aliases)

    url = get_endpoint(guid)
    res = client.put(url, json=alias_payload, headers=user)
    assert res.status_code == 200, f"Unable to PUT aliases: {alias_payload}"

    return aliases


@pytest.fixture(scope="function")
def unused_aliases():
    """
    Returns a pool of unused aliases, ie aliases that are guaranteed to not
    already be in the db / associated with a GUID.
    """
    return [
        "p96yaI06,t@?j=ejk[%+//.?=G}k@Up3LIlv+",
        'FajH%r%6@Tp^HaB^/"uW<A"/\'hELmT',
        "{j'8D6Z?z3:wXd5fc]5#[*9n%|G9\"h",
        '"B}7v9t)U7XsT+EXD|1?@$ywDV"ce<',
    ]


# =============================================


# GET /{alias}
# ------------------------
def test_global_endpoint_valid_alias(client, guid, aliases):
    """
    expect query for alias on global endpoint to return the record associated with alias
    """
    for alias in aliases:
        res = client.get("/" + url_encode(alias))
        assert res.status_code == 200, res.text
        record = res.get_json()
        assert record["did"] == guid, f"Did not retrieve correct record for alias"


def test_global_endpoint_nonexistant_alias(client, guid, aliases):
    """
    expect query for alias on global endpoint to return 404 for a nonexistant record
    """
    fake_alias = aliases[0] + "_but_fake"
    res = client.get("/" + url_encode(fake_alias))
    assert res.status_code == 404, f"Request for fake alias should have failed"


def test_global_endpoint_alias_guid_collision(client, guid, aliases, user):
    """
    if an alias has the same name as a GUID, the global endpoint should resolve
    to the GUID, not resolve the alias
    """
    # create a new guid
    guid_A = guid
    guid_B = create_record(client, user)
    # assign an alias with the value of guid_B to guid_A
    aliases_payload = to_payload([guid_B])
    res = client.post(get_endpoint(guid_A), json=aliases_payload, headers=user)
    assert res.status_code == 200, res.text

    # expect that a search for value of guid_B on the global endpoint should resolve
    # to guid_B's record, not to guid_A's record
    res = client.get("/" + url_encode(guid_B))
    assert res.status_code == 200, res.text
    record = res.get_json()
    assert record["did"] == guid_B
    assert record["did"] != guid_A


def test_global_endpoint_alias_endpoint_collision(client, guid, aliases, user):
    """
    when an alias shares a name with an endpoint on the root of
    the API, querying that endpoint should function as normal,
    not resolve the alias
    """
    # associate a new alias named "index/" with guid. "index/" is an endpoint
    # on the root of the api.
    aliases_payload = to_payload(["index"])
    res = client.post(get_endpoint(guid), json=aliases_payload, headers=user)
    assert res.status_code == 200, res.text

    # when we query "index", we expect to see the "index" endpoint, not
    # an alias.
    res = client.get("index")
    assert res.status_code == 200, res.text
    body = res.get_json()
    body_is_not_record = body.get("did") is None
    assert (
        body_is_not_record
    ), f"Expected not to retrieve record -- expected to retrieve index/ endpoint"


# GET /index/{GUID}/aliases
# -------------------------
def test_GET_aliases_invalid_GUID(client, guid, aliases):
    """
    expect to return 404 for nonexistant GUID
    """
    fake_guid = guid + "but_fake"
    alias_endpoint = get_endpoint(fake_guid)
    res = client.get(alias_endpoint)
    assert res.status_code == 404, res.text


def test_GET_aliases_valid_GUID(client, guid, aliases):
    """
    expect to return all aliases for a valid GUID
    """
    alias_endpoint = get_endpoint(guid)
    res = client.get(alias_endpoint)
    assert res.status_code == 200, res.text
    aliases_in_db = payload_to_list(res.get_json())
    expected_aliases = aliases
    assert set(aliases_in_db) == set(expected_aliases)


# POST /index/{GUID}/aliases
# -------------------------
def test_POST_aliases_valid_GUID_valid_aliases(
    client, user, guid, aliases, unused_aliases
):
    """
    normal operation: expect to append to aliases and return list of all aliases
    """
    new_aliases = unused_aliases
    new_alias_payload = to_payload(new_aliases)

    # expect POST to return list of all aliases
    res = client.post(get_endpoint(guid), json=new_alias_payload, headers=user)
    assert res.status_code == 200, res.text
    aliases_in_db = payload_to_list(res.get_json())

    # expect new aliases to be appended to old aliases
    expected_aliases = aliases + new_aliases
    assert set(aliases_in_db) == set(
        expected_aliases
    ), f"Expected to append {new_aliases} to {aliases}"


def test_POST_aliases_unauthenticated(client, user, guid, aliases, unused_aliases):
    """
    expect request to fail with 403 if user is unauthenticated
    """
    new_aliases = unused_aliases
    new_aliases_payload = to_payload(new_aliases)

    res = client.post(get_endpoint(guid), json=new_aliases_payload)
    assert res.status_code == 403, res.text

    bad_user = {
        "Authorization": "Basic badpassword",
        "Content-Type": "application/json",
    }
    res = client.post(get_endpoint(guid), json=new_aliases_payload, headers=bad_user)
    assert res.status_code == 403, res.text


def test_POST_aliases_invalid_GUID(client, user, guid, aliases, unused_aliases):
    """
    expect to return 404 and have no effect for nonexistant GUID
    """
    fake_guid = guid + "but_fake"
    alias_endpoint = get_endpoint(fake_guid)
    new_aliases = unused_aliases
    new_aliases_payload = to_payload(new_aliases)

    res = client.post(alias_endpoint, json=new_aliases_payload, headers=user)
    assert res.status_code == 404, res.text


def test_POST_aliases_nonunique_aliases(client, user, guid, aliases, unused_aliases):
    """
    expect to return 409 and have no effect if valid GUID but one or more aliases
    already associated with another GUID
    """
    guid_A = guid
    guid_B = create_record(client, user)

    # add unused_aliases to guid_B
    res = client.post(
        get_endpoint(guid_B), json=to_payload(unused_aliases), headers=user
    )
    assert res.status_code == 200, res.text

    # expect that an attempt to add unused_aliases to guid_A
    # will fail, as aliases are already assigned to guid_B
    res = client.post(
        get_endpoint(guid_A), json=to_payload(unused_aliases), headers=user
    )
    assert res.status_code == 409, res.json

    # expect aliases that were already associated with guid_A to be unchanged.
    res = client.get(get_endpoint(guid_A))
    assert res.status_code == 200, res.text
    aliases_in_db = payload_to_list(res.get_json())
    expected_aliases = aliases
    assert set(aliases_in_db) == set(
        expected_aliases
    ), f"Expected original aliases {aliases} to be in db"


def test_POST_aliases_GUID_already_has_alias(client, user, guid, aliases):
    """
    expect to return 409 and have no effect if valid GUID and one or more aliases
    already associated with this GUID
    """
    # pick a subset of the aliases already associated with this GUID
    subset_old_aliases = aliases[0:1]

    # expect a POST request with the new subset of aliases to fail with 409
    res = client.post(
        get_endpoint(guid), json=to_payload(subset_old_aliases), headers=user
    )
    assert res.status_code == 409, res.json


def test_POST_aliases_duplicate_aliases_in_request(
    client, user, guid, aliases, unused_aliases
):
    """
    expect to fail with 409 if valid GUID and one or more aliases duplicated
    in request
    """
    new_aliases = unused_aliases

    # duplicate some aliases: pick a subset of the new aliases and
    # append it to the new aliases
    subset_new_aliases = new_aliases[0:1]
    duplicated_new_aliases = new_aliases + subset_new_aliases

    # expect POST the duplicated aliases to fail
    res = client.post(
        get_endpoint(guid), json=to_payload(duplicated_new_aliases), headers=user
    )
    assert res.status_code == 409, res.json

    # expect aliases in db to be unchanged
    res = client.get(get_endpoint(guid))
    aliases_in_db = payload_to_list(res.get_json())
    assert res.status_code == 200, res.text
    expected_aliases = aliases
    assert set(aliases_in_db) == set(
        expected_aliases
    ), f"Expected original aliases {aliases} to be in db"


def test_POST_aliases_valid_GUID_empty_aliases(client, user, guid, aliases):
    """
    expect to succeed with no effect if passed an empty list of aliases
    """
    # POST an empty list of aliases
    empty_aliases = []
    res = client.post(get_endpoint(guid), json=to_payload(empty_aliases), headers=user)
    assert res.status_code == 200, res.text

    # expect aliases in db to be unchanged
    res = client.get(get_endpoint(guid))
    assert res.status_code == 200, res.text
    aliases_in_db = payload_to_list(res.get_json())
    expected_aliases = aliases
    assert set(aliases_in_db) == set(
        expected_aliases
    ), f"Expected original aliases {aliases} to be in db"


def test_POST_no_body(client, user, guid, aliases):
    """
    expect POST with no body in request to fail with 400
    """
    res = client.post(get_endpoint(guid), headers=user)
    assert res.status_code == 400, res.text


def test_POST_bad_content_type(client, user, guid, aliases):
    """
    expect POST with a non-JSON content type (e.g., `text/plain`) to fail with 400
    """
    res = client.post(get_endpoint(guid), headers=user, content_type="text/plain")
    assert res.status_code == 400, res.text


# PUT /index/{GUID}/aliases
# -------------------------
def test_PUT_aliases_valid_GUID_valid_aliases(
    client, user, guid, aliases, unused_aliases
):
    """
    normal operation: expect to replace aliases and return list of new aliases
    for this GUID
    """
    new_aliases = unused_aliases
    new_alias_payload = to_payload(new_aliases)
    # expect PUT to return list of all aliases
    res = client.put(get_endpoint(guid), json=new_alias_payload, headers=user)
    assert res.status_code == 200, res.text
    aliases_in_db = payload_to_list(res.get_json())
    new_aliases = unused_aliases

    # expect new aliases to have replaced old aliases
    res = client.get(get_endpoint(guid))
    assert res.status_code == 200, res.text
    aliases_in_db = payload_to_list(res.get_json())
    expected_aliases = new_aliases
    assert set(aliases_in_db) == set(
        expected_aliases
    ), f"Expect aliases in db to be {new_aliases}"


def test_PUT_aliases_unauthenticated(client, user, guid, aliases, unused_aliases):
    """
    expect request to fail with 403 if user is unauthenticated
    """
    new_aliases = unused_aliases
    new_aliases_payload = to_payload(new_aliases)

    res = client.put(get_endpoint(guid), json=new_aliases_payload)
    assert res.status_code == 403, res.text

    bad_user = {
        "Authorization": "Basic badpassword",
        "Content-Type": "application/json",
    }
    res = client.put(get_endpoint(guid), json=new_aliases_payload, headers=bad_user)
    assert res.status_code == 403, res.text


def test_PUT_aliases_invalid_GUID(client, user, guid, aliases, unused_aliases):
    """
    expect to return 404 and have no effect for nonexistant GUID
    """
    fake_guid = guid + "but_fake"
    alias_endpoint = get_endpoint(fake_guid)
    new_aliases = unused_aliases
    new_aliases_payload = to_payload(new_aliases)

    res = client.put(alias_endpoint, json=new_aliases_payload, headers=user)
    assert res.status_code == 404, res.text


def test_PUT_aliases_nonunique_aliases(client, user, guid, aliases, unused_aliases):
    """
    expect to return 409 and have no effect if valid GUID but one or more aliases
    already associated with another GUID
    """
    new_aliases = unused_aliases
    # add a subset of the generated aliases to a different GUID.
    other_guid_aliases = [new_aliases[0]]
    other_guid = create_record(client, user)
    res = client.put(
        get_endpoint(other_guid), json=to_payload(other_guid_aliases), headers=user
    )
    assert res.status_code == 200, res.text

    # expect that an attempt to add the original set of random aliases
    # will fail, as some of the aliases are already assigned to a different GUID.
    res = client.put(get_endpoint(guid), json=to_payload(new_aliases), headers=user)
    assert res.status_code == 409, res.json

    # expect aliases that were already associated with GUID to be unchanged.
    res = client.get(get_endpoint(guid))
    assert res.status_code == 200, res.text
    aliases_in_db = payload_to_list(res.get_json())
    expected_aliases = aliases
    assert set(aliases_in_db) == set(
        expected_aliases
    ), f"Expected original aliases {aliases} to be in db"


def test_PUT_aliases_previously_used_valid_alias(
    client, user, guid, aliases, unused_aliases
):
    """
    expect to replace aliases if valid GUID and one or more aliases were once
    associated with a different GUID, but are no longer associated wiht a different GUID.
    """
    new_aliases = unused_aliases

    # add a subset of the generated aliases to a different GUID
    other_guid_aliases = [new_aliases[0]]
    other_guid = create_record(client, user)
    res = client.put(
        get_endpoint(other_guid), json=to_payload(other_guid_aliases), headers=user
    )
    assert res.status_code == 200, res.text

    # delete the aliases from the different GUID
    res = client.delete(get_endpoint(other_guid), headers=user)
    assert res.status_code == 200, res.text

    # expect an attempt to add the original set of random aliases will succeed
    res = client.put(get_endpoint(guid), json=to_payload(new_aliases), headers=user)
    assert res.status_code == 200, res.text


def test_PUT_aliases_GUID_already_has_alias(client, user, guid, aliases):
    """
    expect to replace aliases if valid GUID and one or more aliases already
    associated with this GUID, but not already associated with another GUID
    """
    # pick a subset of the aliases already associated with this GUID
    subset_old_aliases = aliases[0:1]

    # expect a PUT request with the new subset of aliases to succeed
    res = client.put(
        get_endpoint(guid), json=to_payload(subset_old_aliases), headers=user
    )
    assert res.status_code == 200, res.text

    # expect the aliases of this GUID to be the new subset of the old aliases
    res = client.get(get_endpoint(guid))
    assert res.status_code == 200, res.text
    aliases_in_db = payload_to_list(res.get_json())
    expected_aliases = subset_old_aliases
    assert set(aliases_in_db) == set(
        expected_aliases
    ), f"Expected aliases {expected_aliases} to be in db"


def test_PUT_aliases_duplicate_aliases_in_request(
    client, user, guid, aliases, unused_aliases
):
    """
    expect to fail with 409 if valid GUID and one or more aliases duplicated
    in request
    """
    new_aliases = unused_aliases

    # duplicate some aliases: pick a subset of the new aliases and
    # append it to the new aliases
    subset_new_aliases = new_aliases[0:1]
    duplicated_new_aliases = new_aliases + subset_new_aliases

    # expect PUT the duplicated aliases to fail
    res = client.put(
        get_endpoint(guid), json=to_payload(duplicated_new_aliases), headers=user
    )
    assert res.status_code == 409, res.json


def test_PUT_aliases_valid_GUID_empty_aliases(client, user, guid, aliases):
    """
    expect succeed and remove all aliases if passed an empty list of aliases
    """
    empty_aliases = []
    res = client.put(get_endpoint(guid), json=to_payload(empty_aliases), headers=user)
    assert res.status_code == 200, res.text

    # expect aliases in db to be empty
    res = client.get(get_endpoint(guid))
    assert res.status_code == 200, res.text
    aliases_in_db = payload_to_list(res.get_json())
    expected_aliases = empty_aliases
    assert set(aliases_in_db) == set(
        expected_aliases
    ), "Expected no aliases to be in db"


def test_PUT_no_body(client, user, guid, aliases):
    """
    expect PUT with no body in request to fail with 400
    """
    res = client.put(get_endpoint(guid), json=None, headers=user)
    assert res.status_code == 400, res.text


def test_PUT_bad_content_type(client, user, guid, aliases):
    """
    expect PUT with a non-JSON content type (e.g., `text/plain`) to fail with 400
    """
    res = client.put(get_endpoint(guid), headers=user, content_type="text/plain")
    assert res.status_code == 400, res.text


# DELETE /index/{GUID}/aliases
# ----------------------------
def test_DELETE_all_aliases_valid_GUID(client, user, guid, aliases):
    """
    normal operation: expect to delete all aliases if valid GUID
    """
    res = client.delete(get_endpoint(guid), headers=user)
    assert res.status_code == 200, res.text

    # expect all aliases to be gone
    res = client.get(get_endpoint(guid))
    aliases_in_db = payload_to_list(res.get_json())
    expected_aliases = []
    assert aliases_in_db == expected_aliases, "Expected no aliases to be in db"


def test_DELETE_all_aliases_unauthenticated(client, user, guid, aliases):
    """
    expect request to fail with 403 if user is unauthenticated
    """
    res = client.delete(get_endpoint(guid))
    assert res.status_code == 403, res.text

    bad_user = {
        "Authorization": "Basic badpassword",
        "Content-Type": "application/json",
    }
    res = client.delete(get_endpoint(guid), headers=bad_user)
    assert res.status_code == 403, res.text


def test_DELETE_all_aliases_invalid_GUID(client, user, guid, aliases):
    """
    expect to return 404 and have no effect for nonexistant GUID
    """
    fake_guid = guid + "_but_fake"
    res = client.delete(get_endpoint(fake_guid), headers=user)
    assert res.status_code == 404, res.text


# DELETE /index/{GUID}/aliases/{ALIAS}
# ------------------------------------
def test_DELETE_one_alias_valid_GUID(client, user, guid, aliases):
    """
    normal operation: expect to delete listed alias if valid GUID and
    alias associated with this GUID
    """
    # pick one alias to delete
    alias_to_delete = aliases[0]
    endpoint = get_endpoint(guid) + "/" + url_encode(alias_to_delete)
    res = client.delete(endpoint, headers=user)
    assert res.status_code == 200, res.text

    # expect that alias to no longer be in this guid's aliases
    res = client.get(get_endpoint(guid))
    aliases_in_db = payload_to_list(res.get_json())
    expected_aliases = [a for a in aliases if a != alias_to_delete]
    assert set(aliases_in_db) == set(expected_aliases)


def test_DELETE_one_alias_unauthenticated(client, user, guid, aliases):
    """
    expect request to fail with 403 if user is unauthenticated
    """
    # pick one alias to delete
    alias_to_delete = aliases[0]
    endpoint = get_endpoint(guid) + "/" + url_encode(alias_to_delete)

    # expect an unauthenticated delete request to that alias to fail
    res = client.delete(endpoint)
    assert res.status_code == 403, res.text

    bad_user = {
        "Authorization": "Basic badpassword",
        "Content-Type": "application/json",
    }
    res = client.delete(endpoint, headers=bad_user)
    assert res.status_code == 403, res.text


def test_DELETE_one_alias_invalid_GUID(client, user, guid, aliases):
    """
    expect to return 404 and have no effect for nonexistant GUID
    """
    # pick a nonexistant guid
    fake_guid = guid + "_but_fake"

    # pick a nonexistant alias to delete
    alias_to_delete = "fake_alias"
    endpoint = get_endpoint(fake_guid) + "/" + url_encode(alias_to_delete)
    res = client.delete(endpoint, headers=user)
    assert res.status_code == 404, res.text


def test_DELETE_one_alias_GUID_does_not_have_alias(client, user, guid, aliases):
    """
    expect to return 404 and have no effect if alias not associated
    with this GUID
    """
    # pick a nonexistant alias to delete on an existing guid
    alias_to_delete = "fake_alias"
    endpoint = get_endpoint(guid) + "/" + url_encode(alias_to_delete)
    res = client.delete(endpoint, headers=user)
    assert res.status_code == 404, res.text
