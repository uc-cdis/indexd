import copy
import json
import pytest
import re

GUID_REGEX = re.compile(
    r"([a-z0-9A-Z]*\.*[a-z0-9A-Z]*\/*)([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})"
)


def test_single_guid(app_client):
    """
    Test that generating a single GUID works
    """
    _, client = app_client
    response = client.get("/guid/mint")
    assert response.status_code == 200
    response_json = response.json()
    guid_list = response_json["guids"]

    reg = re.compile(GUID_REGEX)
    for guid in guid_list:
        assert reg.findall(guid)


@pytest.mark.parametrize("count", [-10, -1, 0, 1, 10, 10000000])
def test_guids(app_client, count):
    """
    Test that generating GUIDs works when provided various counts
    """
    _, client = app_client
    response = client.get(f"/guid/mint?count={count}")

    if count in [0, 1, 10]:
        assert response.status_code == 200
        response_json = response.json()
        guid_list = response_json["guids"]

        # make sure result is >=0 and contains valid guids
        reg = re.compile(GUID_REGEX)
        result_count = 0
        for guid in guid_list:
            result_count += 1
            assert reg.findall(guid)
        assert result_count == count
    else:
        assert response.status_code == 400


def test_guids_invalid_count(app_client):
    """
    Test that generating GUIDs doesn't work when provided invalid count
    """
    _, client = app_client
    response = client.get(f"/guid/mint?count=foobar")
    assert response.status_code == 400


def test_get_prefix(app_client):
    """
    Test that generating a prefix works
    """
    app, client = app_client
    original_config = copy.deepcopy(app.settings["config"]["INDEX"]["driver"].config)
    app.settings["config"]["INDEX"]["driver"].config["DEFAULT_PREFIX"] = "foobar:"
    app.settings["config"]["INDEX"]["driver"].config["ADD_PREFIX_ALIAS"] = False
    response = client.get("/guid/prefix")
    app.settings["config"]["INDEX"]["driver"].config = original_config

    assert response.status_code == 200
    response_json = response.json()
    prefix = response_json["prefix"]

    assert prefix == "foobar:"


def test_get_prefix_when_none(app_client):
    """
    Test that generating a prefix works even when there isn't a prefix
    """
    app, client = app_client
    original_config = copy.deepcopy(app.settings["config"]["INDEX"]["driver"].config)
    app.settings["config"]["INDEX"]["driver"].config["ADD_PREFIX_ALIAS"] = True
    response = client.get("/guid/prefix")
    app.settings["config"]["INDEX"]["driver"].config = original_config

    assert response.status_code == 200
    response_json = response.json()
    prefix = response_json["prefix"]

    assert prefix == ""
