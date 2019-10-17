import pytest
import random
import string
import json
import urllib.parse
from tests.test_client import get_doc

from indexd import get_app

# Test fixtures and helper functions
# =============================================
NUM_RANDOM_ALIASES = 12

def url_encode(str):
    return urllib.parse.quote(str, safe="")

def get_endpoint(guid):
    return "/index/{}/aliases".format(guid)

def create_random_alias():
    alias_length = 30 
    chars = string.ascii_letters + string.punctuation + string.digits
    return "".join(random.choice(chars) for _ in range(alias_length)) 

def create_random_aliases(num_aliases):
    return [create_random_alias() for _ in range(num_aliases)]

def to_payload(aliases):
    """
    Boxes a list of aliases into a JSON payload object expected
    by the server.
    """
    return [{"value": alias} for alias in aliases]

def payload_to_list(alias_payload):
    """
    Unboxes a JSON payload object expected by the server into
    a list of alias names.
    """
    return [record["value"] for record in alias_payload]

def create_random_record(client, user):
    """
    Creates a random record in indexd and returns that record's GUID.
    """
    document = get_doc()
    res = client.post("/index/", json=document, headers=user)
    assert res.status_code == 200
    # The GUID is the "did" (Document IDentifier) returned from a successful 
    # POST request.
    guid = res.get_json()["did"]

    return guid

@pytest.fixture(scope="function")
def guid(client, user):
    """
    Creates a random record in indexd and returns that record's GUID.
    """
    return create_random_record(client, user)

@pytest.fixture(scope="function")
def aliases(client, user, guid):
    """
    Associates between MIN_ALIASES and MAX_ALIASES random aliases with a GUID in indexd.
    Returns the new aliases.
    """
    MIN_ALIASES = 1
    MAX_ALIASES = 20
    num_aliases = random.randint(MIN_ALIASES, MAX_ALIASES+1)

    aliases = create_random_aliases(num_aliases)
    alias_payload = to_payload(aliases)

    url = get_endpoint(guid)
    res = client.put(url, json=alias_payload, headers=user)
    assert res.status_code == 200

    return aliases

@pytest.fixture(scope="function")
def root_endpoints():
    app = get_app()
    rules = [str(rule) for rule in app.url_map.iter_rules()]
    # rules are in format ['/_query/urls/metadata/q', '/ga4gh/dos/v1/dataobjects', ...]
    # We want only the names of the root endpoints - so keep only the string
    # between the first two forward slashes.
    root_endpoints = [rule.split("/")[1] for rule in rules]
    # remove duplicated values from root_endpoints
    root_endpoints = list(set(root_endpoints))
    # remove '<path:record>' from root_endpoints -- it represents the `/{GUID | ALIAS}`
    # endpoint.
    root_endpoints.remove("<path:record>")
    return root_endpoints

# =============================================


# GET /{alias}
# ------------------------
def test_global_endpoint_valid_alias(client, guid, aliases):
    """
    expect query for alias on global endpoint to return the record associated with alias
    """
    for alias in aliases:
        res = client.get("/" + url_encode(alias))
        assert res.status_code == 200
        record = res.get_json()
        assert record["did"] == guid

def test_global_endpoint_nonexistant_alias(client, guid, aliases):
    """
    expect query for alias on global endpoint to return 404 for a nonexistant record
    """
    fake_alias = aliases[0] + "_but_fake"
    res = client.get("/" + url_encode(fake_alias))
    assert res.status_code == 404

# GET /index/{GUID}/aliases
# -------------------------
def test_GET_aliases_invalid_GUID(client, guid, aliases):
    """
    expect to return 404 for nonexistant GUID
    """
    fake_guid = guid + "but_fake"
    alias_endpoint = get_endpoint(fake_guid)
    res = client.get(alias_endpoint)
    assert res.status_code == 404

def test_GET_aliases_valid_GUID(client, guid, aliases):
    """
    expect to return all aliases for a valid GUID
    """
    alias_endpoint = get_endpoint(guid)
    res = client.get(alias_endpoint)
    assert res.status_code == 200
    aliases_in_db = payload_to_list(res.get_json())
    expected_aliases = aliases
    assert set(aliases_in_db) == set(expected_aliases)

# POST /index/{GUID}/aliases
# -------------------------
def test_POST_aliases_valid_GUID_valid_aliases(client, user, guid, aliases):
    """
    normal operation: expect to append to aliases and return list of all aliases
    """
    new_aliases = create_random_aliases(NUM_RANDOM_ALIASES)
    new_alias_payload = to_payload(new_aliases)
    res = client.post(get_endpoint(guid), json=new_alias_payload, headers=user)
    assert res.status_code == 200

    # expect new aliases to be appended to old aliases
    res = client.get(get_endpoint(guid))
    assert res.status_code == 200
    aliases_in_db = payload_to_list(res.get_json())
    expected_aliases = aliases + new_aliases
    assert set(aliases_in_db) == set(expected_aliases)

def test_POST_aliases_unauthenticated(client, user, guid, aliases):
    """
    expect request to fail with 403 if user is unauthenticated
    """
    new_aliases = create_random_aliases(NUM_RANDOM_ALIASES)
    new_aliases_payload = to_payload(new_aliases)

    res = client.post(get_endpoint(guid), json=new_aliases_payload)
    assert res.status_code == 403

    bad_user = {'Authorization': 'Basic badpassword', 'Content-Type': 'application/json'}
    res = client.post(get_endpoint(guid), json=new_aliases_payload, headers=bad_user)
    assert res.status_code == 403

def test_POST_aliases_invalid_GUID(client, user, guid, aliases):
    """
    expect to return 404 and have no effect for nonexistant GUID
    """
    fake_guid = guid + "but_fake"
    alias_endpoint = get_endpoint(fake_guid)
    new_aliases = create_random_aliases(NUM_RANDOM_ALIASES)
    new_aliases_payload = to_payload(new_aliases)

    res = client.post(alias_endpoint, json=new_aliases_payload, headers=user)
    assert res.status_code == 404

def test_POST_aliases_nonunique_aliases(client, user, guid, aliases):
    """
    expect to return 400 and have no effect if valid GUID but one or more aliases 
    already associated with another GUID
    """
    # generate random aliases
    new_aliases = create_random_aliases(NUM_RANDOM_ALIASES)
    # add a subset of the generated aliases to a different GUID.
    other_guid_aliases = random.sample(new_aliases, 1)
    other_guid = create_random_record(client, user)
    res = client.post(get_endpoint(other_guid), json=to_payload(other_guid_aliases), headers=user)
    assert res.status_code == 200

    # expect that an attempt to add the original set of random aliases
    # will fail, as some of the aliases are already assigned to a different GUID.
    res = client.post(get_endpoint(guid), json=to_payload(new_aliases), headers=user)
    assert res.status_code == 400

    # expect aliases that were already associated with GUID to be unchanged.
    res = client.get(get_endpoint(guid))
    assert res.status_code == 200
    aliases_in_db = payload_to_list(res.get_json())
    expected_aliases = aliases
    assert set(aliases_in_db) == set(expected_aliases)

def test_POST_aliases_GUID_already_has_alias(client, user, guid, aliases):
    """
    expect to return 400 and have no effect if valid GUID and one or more aliases 
    already associated with this GUID
    """
    # pick a random subset of the aliases already associated with this GUID
    subset_old_aliases = random.sample(aliases, random.randint(1, len(aliases)))

    # expect a POST request with the new subset of aliases to fail with 400
    res = client.post(get_endpoint(guid), json=to_payload(subset_old_aliases), headers=user)
    assert res.status_code == 400

def test_POST_aliases_duplicate_aliases_in_request(client, user, guid, aliases):
    """
    expect to fail with 400 if valid GUID and one or more aliases duplicated
    in request
    """
    # generate random aliases
    new_aliases = create_random_aliases(NUM_RANDOM_ALIASES)

    # duplicate some aliases: pick a random subset of the new aliases and
    # append it to the new aliases
    subset_new_aliases = random.sample(new_aliases, random.randint(1, len(new_aliases)))
    duplicated_new_aliases = new_aliases + subset_new_aliases

    # expect POST the duplicated aliases to fail
    res = client.post(get_endpoint(guid), json=to_payload(duplicated_new_aliases), headers=user)
    assert res.status_code == 400

def test_POST_aliases_valid_GUID_empty_aliases(client, user, guid, aliases):
    """
    expect to succeed with no effect if passed an empty list of aliases
    """
    # POST an empty list of aliases
    empty_aliases = []
    res = client.post(get_endpoint(guid), json=to_payload(empty_aliases), headers=user)
    assert res.status_code == 200

    # expect aliases in db to be unchanged
    res = client.get(get_endpoint(guid))
    assert res.status_code == 200
    aliases_in_db = payload_to_list(res.get_json())
    expected_aliases = aliases
    assert set(aliases_in_db) == set(expected_aliases)

def test_POST_aliases_alias_has_name_of_endpoint_on_root(client, user, guid, aliases, root_endpoints):
    """
    Expect 400 if one or more aliases has same name as an endpoint on the root 
    URL of the API. 
    """
    # Because of the `/{GUID|ALIAS}` endpoint, if an alias shared the name of an endpoint
    # on the root of the API such as `/latest`, if an alias was named "latest"  
    # it would cause difficulty resolving the alias. 
    for bad_alias in root_endpoints:
        bad_alias_payload = to_payload([bad_alias])
        res = client.post(get_endpoint(guid), json=bad_alias_payload, headers=user)
        assert res.status_code == 400
        
def test_POST_aliases_alias_has_name_of_existing_GUID(client, user, guid, aliases):
    """
    expect 400 if one or more aliases has same name as an existing GUID. This is
    because an alias with the same name as a GUID would cause a search on the
    `/{GUID|ALIAS}` endpoint to potentially resolve to two different records.
    """
    # Add a random GUID to the db
    other_guid = create_random_record(client, user)

    # Expect that adding an alias to our guid with the same name as other_guid
    # will fail.
    bad_alias = other_guid
    bad_payload = to_payload([bad_alias])
    res = client.post(get_endpoint(guid), json=bad_payload, headers=user)
    assert res.status_code == 400

def test_POST_no_body(client, user, guid, aliases):
    """
    expect POST with no body in request to fail with 400
    """
    res = client.post(get_endpoint(guid), headers=user)
    assert res.status_code == 400

def test_POST_bad_content_type(client, user, guid, aliases):
    """
    expect POST with a non-JSON content type (e.g., `text/plain`) to fail with 400
    """
    res = client.post(get_endpoint(guid), headers=user, content_type="text/plain")
    assert res.status_code == 400


# PUT /index/{GUID}/aliases
# -------------------------
def test_PUT_aliases_valid_GUID_valid_aliases(client, user, guid, aliases):
    """
    normal operation: expect to replace aliases and return list of new aliases 
    for this GUID
    """
    new_aliases = create_random_aliases(NUM_RANDOM_ALIASES)
    new_alias_payload = to_payload(new_aliases)
    res = client.put(get_endpoint(guid), json=new_alias_payload, headers=user)
    assert res.status_code == 200

    # expect new aliases to have replaced old aliases
    res = client.get(get_endpoint(guid))
    assert res.status_code == 200
    aliases_in_db = payload_to_list(res.get_json())
    expected_aliases = new_aliases
    assert set(aliases_in_db) == set(expected_aliases)

def test_PUT_aliases_unauthenticated(client, user, guid, aliases):
    """
    expect request to fail with 403 if user is unauthenticated
    """
    new_aliases = create_random_aliases(NUM_RANDOM_ALIASES)
    new_aliases_payload = to_payload(new_aliases)

    res = client.put(get_endpoint(guid), json=new_aliases_payload)
    assert res.status_code == 403

    bad_user = {'Authorization': 'Basic badpassword', 'Content-Type': 'application/json'}
    res = client.put(get_endpoint(guid), json=new_aliases_payload, headers=bad_user)
    assert res.status_code == 403

def test_PUT_aliases_invalid_GUID(client, user, guid, aliases):
    """
    expect to return 404 and have no effect for nonexistant GUID
    """
    fake_guid = guid + "but_fake"
    alias_endpoint = get_endpoint(fake_guid)
    new_aliases = create_random_aliases(NUM_RANDOM_ALIASES)
    new_aliases_payload = to_payload(new_aliases)

    res = client.put(alias_endpoint, json=new_aliases_payload, headers=user)
    assert res.status_code == 404

def test_PUT_aliases_nonunique_aliases(client, user, guid, aliases):
    """
    expect to return 400 and have no effect if valid GUID but one or more aliases 
    already associated with another GUID
    """
    # generate random aliases
    new_aliases = create_random_aliases(NUM_RANDOM_ALIASES)
    # add a subset of the generated aliases to a different GUID.
    other_guid_aliases = random.sample(new_aliases, 1)
    other_guid = create_random_record(client, user)
    res = client.put(get_endpoint(other_guid), json=to_payload(other_guid_aliases), headers=user)
    assert res.status_code == 200

    # expect that an attempt to add the original set of random aliases
    # will fail, as some of the aliases are already assigned to a different GUID.
    res = client.put(get_endpoint(guid), json=to_payload(new_aliases), headers=user)
    assert res.status_code == 400

    # expect aliases that were already associated with GUID to be unchanged.
    res = client.get(get_endpoint(guid))
    assert res.status_code == 200
    aliases_in_db = payload_to_list(res.get_json())
    expected_aliases = aliases
    assert set(aliases_in_db) == set(expected_aliases)

def test_PUT_aliases_previously_used_valid_alias(client, user, guid, aliases):
    """
    expect to replace aliases if valid GUID and one or more aliases were once
    associated with a different GUID, but are no longer associated wiht a different GUID.
    """
    # generate random aliases
    new_aliases = create_random_aliases(NUM_RANDOM_ALIASES)

    # add a subset of the generated aliases to a different GUID
    other_guid_aliases = random.sample(new_aliases, 1)
    other_guid = create_random_record(client, user)
    res = client.put(get_endpoint(other_guid), json=to_payload(other_guid_aliases), headers=user)
    assert res.status_code == 200

    # remove the subset of the generated aliases from the different GUID
    other_guid_new_aliases = create_random_aliases(NUM_RANDOM_ALIASES)
    res = client.put(get_endpoint(other_guid), json=to_payload(other_guid_new_aliases), headers=user)
    assert res.status_code == 200

    # expect an attempt to add the original set of random aliases will succeed
    res = client.put(get_endpoint(guid), json=to_payload(new_aliases), headers=user)
    assert res.status_code == 200

def test_PUT_aliases_GUID_already_has_alias(client, user, guid, aliases):
    """
    expect to replace aliases if valid GUID and one or more aliases already 
    associated with this GUID, but not already associated with another GUID
    """
    # pick a random subset of the aliases already associated with this GUID
    subset_old_aliases = random.sample(aliases, random.randint(1, len(aliases)))

    # expect a PUT request with the new subset of aliases to succeed
    res = client.put(get_endpoint(guid), json=to_payload(subset_old_aliases), headers=user)
    assert res.status_code == 200

    # expect the aliases of this GUID to be the new subset of the old aliases
    res = client.get(get_endpoint(guid))
    assert res.status_code == 200
    aliases_in_db = payload_to_list(res.get_json())
    expected_aliases = subset_old_aliases
    assert set(aliases_in_db) == set(subset_old_aliases)

def test_PUT_aliases_duplicate_aliases_in_request(client, user, guid, aliases):
    """
    expect to fail with 400 if valid GUID and one or more aliases duplicated 
    in request
    """
    # generate random aliases
    new_aliases = create_random_aliases(NUM_RANDOM_ALIASES)

    # duplicate some aliases: pick a random subset of the new aliases and
    # append it to the new aliases
    subset_new_aliases = random.sample(new_aliases, random.randint(1, len(new_aliases)))
    duplicated_new_aliases = new_aliases + subset_new_aliases

    # expect PUT the duplicated aliases to fail
    res = client.put(get_endpoint(guid), json=to_payload(duplicated_new_aliases), headers=user)
    assert res.status_code == 400

def test_PUT_aliases_valid_GUID_empty_aliases(client, user, guid, aliases):
    """
    expect succeed and remove all aliases if passed an empty list of aliases
    """
    empty_aliases = []
    res = client.put(get_endpoint(guid), json=to_payload(empty_aliases), headers=user)
    assert res.status_code == 200

    # expect aliases in db to be empty
    res = client.get(get_endpoint(guid))
    assert res.status_code == 200
    aliases_in_db = payload_to_list(res.get_json())
    expected_aliases = empty_aliases
    assert set(aliases_in_db) == set(expected_aliases)

def test_PUT_aliases_alias_has_name_of_endpoint_on_root(client, user, guid, aliases, root_endpoints):
    """
    Expect 400 if one or more aliases has same name as an endpoint on the root 
    URL of the API. 
    """
    # Because of the `/{GUID|ALIAS}` endpoint, if an alias shared the name of an endpoint
    # on the root of the API such as `/latest`, if an alias was named "latest"  
    # it would cause difficulty resolving the alias. 
    for bad_alias in root_endpoints:
        bad_alias_payload = to_payload([bad_alias])
        res = client.put(get_endpoint(guid), json=bad_alias_payload, headers=user)
        assert res.status_code == 400
        
def test_PUT_aliases_alias_has_name_of_existing_GUID(client, user, guid, aliases):
    """
    expect 400 if one or more aliases has same name as an existing GUID. This is
    because an alias with the same name as a GUID would cause a search on the
    `/{GUID|ALIAS}` endpoint to potentially resolve to two different records.
    """
    # Add a random GUID to the db
    other_guid = create_random_record(client, user)

    # Expect that adding an alias to our guid with the same name as other_guid
    # will fail.
    bad_alias = other_guid
    bad_payload = to_payload([bad_alias])
    res = client.put(get_endpoint(guid), json=bad_payload, headers=user)
    assert res.status_code == 400

def test_PUT_no_body(client, user, guid, aliases):
    """
    expect PUT with no body in request to fail with 400
    """
    res = client.put(get_endpoint(guid), json=None, headers=user)
    assert res.status_code == 400

def test_PUT_bad_content_type(client, user, guid, aliases):
    """
    expect PUT with a non-JSON content type (e.g., `text/plain`) to fail with 400
    """
    res = client.put(get_endpoint(guid), headers=user, content_type="text/plain")
    assert res.status_code == 400

# DELETE /index/{GUID}/aliases
# ----------------------------
def test_DELETE_all_aliases_valid_GUID(client, user, guid, aliases):
    """
    normal operation: expect to delete all aliases if valid GUID 
    """
    res = client.delete(get_endpoint(guid), headers=user)
    assert res.status_code == 200

    # expect all aliases to be gone
    res = client.get(get_endpoint(guid))
    aliases_in_db = payload_to_list(res.get_json())
    expected_aliases = to_payload([])
    assert aliases_in_db == expected_aliases

def test_DELETE_all_aliases_unauthenticated(client, user, guid, aliases):
    """
    expect request to fail with 403 if user is unauthenticated
    """
    res = client.delete(get_endpoint(guid))
    assert res.status_code == 403

    bad_user = {'Authorization': 'Basic badpassword', 'Content-Type': 'application/json'}
    res = client.delete(get_endpoint(guid), headers=bad_user)
    assert res.status_code == 403

def test_DELETE_all_aliases_invalid_GUID(client, user, guid, aliases):
    """
    expect to return 404 and have no effect for nonexistant GUID
    """
    fake_guid = guid + "_but_fake"
    res = client.delete(get_endpoint(fake_guid), headers=user)
    assert res.status_code == 404

# DELETE /index/{GUID}/aliases/{ALIAS}
# ------------------------------------
def test_DELETE_one_alias_valid_GUID(client, user, guid, aliases):
    """
    normal operation: expect to delete listed alias if valid GUID and 
    alias associated with this GUID
    """
    # pick one alias to delete
    alias_to_delete = random.choice(aliases)
    endpoint = get_endpoint(guid) + "/" + url_encode(alias_to_delete)
    res = client.delete(endpoint, headers=user)
    assert res.status_code == 200

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
    alias_to_delete = random.choice(aliases)
    endpoint = get_endpoint(guid) + "/" + url_encode(alias_to_delete)

    # expect an unauthenticated delete request to that alias to fail
    res = client.delete(endpoint)
    assert res.status_code == 403

    bad_user = {'Authorization': 'Basic badpassword', 'Content-Type': 'application/json'}
    res = client.delete(endpoint, headers=bad_user)
    assert res.status_code == 403

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
    assert res.status_code == 404

def test_DELETE_one_alias_GUID_does_not_have_alias(client, user, guid, aliases):
    """
    expect to return 404 and have no effect if alias not associated 
    with this GUID
    """
    # pick a nonexistant alias to delete on an existing guid
    alias_to_delete = "fake_alias"
    endpoint = get_endpoint(guid) + "/" + url_encode(alias_to_delete)
    res = client.delete(endpoint, headers=user)
    assert res.status_code == 404
