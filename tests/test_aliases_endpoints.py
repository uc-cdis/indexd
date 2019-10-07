import pytest

# GET /index/{GUID}/aliases
# -------------------------
def test_GET_aliases_invalid_GUID(client):
    """
    expect to return 404 for nonexistant GUID
    """
    pass

def test_GET_aliases_valid_GUID(client):
    """
    expect to return all aliases for a valid GUID
    """
    pass

# POST /index/{GUID}/aliases
# -------------------------
def test_POST_aliases_valid_GUID_valid_aliases(client, user):
    """
    normal operation: expect to append to aliases and return list of all aliases
    """
    pass
def test_POST_aliases_unauthenticated(client, user):
    """
    expect request to fail with 403 if user is unauthenticated
    """
    pass
def test_POST_aliases_invalid_GUID(client, user):
    """
    expect to return 404 and have no effect for nonexistant GUID
    """
    pass
def test_POST_aliases_nonunique_aliases(client, user):
    """
    expect to return 400 and have no effect if valid GUID but one or more aliases 
    already associated with another GUID
    """
    pass
def test_POST_aliases_GUID_already_has_alias(client, user):
    """
    expect to return 400 and have no effect if valid GUID and one or more aliases 
    already associated with this GUID
    """
    pass
def test_POST_aliases_duplicate_aliases_in_request(client, user):
    """
    expect to append to aliases if valid GUID and one or more aliases duplicated
    in request, but not already associated with another GUID
    """
    pass
def test_POST_aliases_valid_GUID_empty_aliases(client, user):
    """
    expect to succeed with no effect if passed an empty list of aliases
    """
    pass

# PUT /index/{GUID}/aliases
# -------------------------
def test_PUT_aliases_valid_GUID_valid_aliases(client, user):
    """
    normal operation: expect to replace aliases and return list of new aliases 
    for this GUID
    """
    pass
def test_PUT_aliases_unauthenticated(client, user):
    """
    expect request to fail with 403 if user is unauthenticated
    """
    pass
def test_PUT_aliases_invalid_GUID(client, user):
    """
    expect to return 404 and have no effect for nonexistant GUID
    """
    pass
def test_PUT_aliases_nonunique_aliases(client, user):
    """
    expect to return 400 and have no effect if valid GUID but one or more aliases 
    already associated with another GUID
    """
    pass
def test_PUT_aliases_GUID_already_has_alias(client, user):
    """
    expect to replace aliases if valid GUID and one or more aliases already 
    associated with this GUID, but not already associated with another GUID
    """
    pass
def test_PUT_aliases_duplicate_aliases_in_request(client, user):
    """
    expect to replace aliases if valid GUID and one or more aliases duplicated 
    in request, but not already associated with another GUID
    """
    pass
def test_PUT_aliases_valid_GUID_empty_aliases(client, user):
    """
    expect succeed and remove all aliases if passed an empty list of aliases
    """
    pass

# DELETE /index/{GUID}/aliases
# -------------------------
def test_DELETE_aliases_valid_GUID_valid_aliases(client, user):
    """
    normal operation: expect to delete listed aliases if valid GUID and 
    all aliases associated with this GUID
    """
    pass
def test_DELETE_aliases_unauthenticated(client, user):
    """
    expect request to fail with 403 if user is unauthenticated
    """
    pass
def test_DELETE_aliases_invalid_GUID(client, user):
    """
    expect to return 404 and have no effect for nonexistant GUID
    """
    pass
def test_DELETE_aliases_GUID_does_not_have_alias(client, user):
    """
    expect to return 404 and have no effect if one or more aliases not associated 
    with this GUID
    """
    pass
def test_DELETE_aliases_duplicate_aliases_in_request(client, user):
    """
    expect to delete listed aliases if valid GUID and all aliases associated with
    this GUID, but one or more aliases duplicated in request
    """
    pass
def test_DELETE_aliases_valid_GUID_empty_aliases(client, user):
    """
    expect to return 200 if passed an empty list of aliases
    """
    pass
