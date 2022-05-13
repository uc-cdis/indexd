import json
import re

GUID_REGEX = re.compile(
    r"([a-z0-9A-Z]*\.*[a-z0-9A-Z]*\/*)([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})"
)


def test_single_guid(app, client, user):
    """
    Test that generating a single GUID works
    """
    response = client.get("/guid/mint")
    assert response.status_code == 200
    response_json = response.json
    guid_list = response_json["guids"]

    reg = re.compile(GUID_REGEX)
    for guid in guid_list:
        print(guid)
        assert reg.findall(guid)


def test_guids(app, client, user):
    """
    Test that generating many GUIDs works
    """
    response = client.get("/guid/mint?count=20")
    assert response.status_code == 200
    response_json = response.json
    guid_list = response_json["guids"]
    print(guid_list)

    reg = re.compile(GUID_REGEX)
    count = 0
    for guid in guid_list:
        count += 1
        print(guid)
        assert reg.findall(guid)
    assert count == 20


def test_get_prefix(app, client, user, monkeypatch):
    """
    Test that generating a prefix works
    """
    response = client.get("/guid/prefix")
    assert response.status_code == 200
    response_json = response.json
    prefix = response_json["prefix"]

    assert prefix == app.config["INDEX"]["driver"].config["DEFAULT_PREFIX"]


def test_get_prefix_when_none(app, client, user, monkeypatch):
    """
    Test that generating a prefix works even when there isn't a prefix
    """
    app.config["INDEX"]["driver"].config["ADD_PREFIX_ALIAS"] = True
    response = client.get("/guid/prefix")
    app.config["INDEX"]["driver"].config["ADD_PREFIX_ALIAS"] = False

    assert response.status_code == 200
    response_json = response.json
    prefix = response_json["prefix"]

    assert prefix == ""
