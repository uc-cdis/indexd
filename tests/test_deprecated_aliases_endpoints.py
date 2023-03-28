import pytest


# NOTE these tests apply to the '/alias/' endpoint, which is deprecated
# in favor of the 'index/{GUID}/aliases' endpoint.
def test_alias_list(client, user):
    assert client.get("/alias/").json["aliases"] == []


def test_get_alias_by_name(client, user):
    data = {
        "size": 123,
        "hashes": {"md5": "8b9942cf415384b27cadf1f4d2d682e5"},
        "release": "private",
        "keeper_authority": "CRI",
        "host_authorities": ["PDC"],
    }
    ark = "ark:/31807/TEST-abc"
    res = client.put("/alias/" + ark, json=data, headers=user)
    assert res.status_code == 200
    res = client.get("/alias/" + ark)
    assert res.status_code == 200
    rec = res.json
    assert rec["name"] == ark


def test_alias_list_by_hash(client, user):
    data = {
        "size": 123,
        "hashes": {"md5": "8b9942cf415384b27cadf1f4d2d682e5"},
        "release": "private",
        "keeper_authority": "CRI",
        "host_authorities": ["PDC"],
    }
    ark = "ark:/31807/TEST-abc"
    res = client.put("/alias/" + ark, json=data, headers=user)
    assert res.status_code == 200
    rec = res.json
    assert rec["name"] == ark
    # assert there is only one entry in the alias index
    res = client.get("/alias/")
    assert res.status_code == 200
    rec = res.json
    assert len(rec["aliases"]) == 1
    res = client.get("/alias/?hash=md5:" + data["hashes"]["md5"])
    rec = res.json
    # assert that the returned alias by hash is the same as the posted
    assert rec["aliases"][0] == ark
    assert rec["hashes"] == data["hashes"]


def test_alias_list_by_size(client, user):
    data = {
        "size": 123,
        "hashes": {"md5": "8b9942cf415384b27cadf1f4d2d682e5"},
        "release": "private",
        "keeper_authority": "CRI",
        "host_authorities": ["PDC"],
    }
    ark = "ark:/31807/TEST-abc"
    res = client.put("/alias/" + ark, json=data, headers=user)
    assert res.status_code == 200
    rec = res.json
    assert rec["name"] == ark
    # assert there is only one entry in the alias index
    res = client.get("/alias/")
    assert res.status_code == 200
    rec = res.json
    assert len(rec["aliases"]) == 1
    # assert that the returned alias by size is the same as the posted
    res = client.get("/alias/?size={}".format(data["size"]))
    rec = res.json
    assert rec["aliases"][0] == ark


def test_alias_list_with_start(client, user):
    data = {
        "size": 123,
        "hashes": {"md5": "8b9942cf415384b27cadf1f4d2d682e5"},
        "release": "private",
        "keeper_authority": "CRI",
        "host_authorities": ["PDC"],
    }
    ark1 = "ark:/31807/TEST-aaa"
    res = client.put("/alias/" + ark1, json=data, headers=user)
    assert res.status_code == 200
    rec = res.json
    assert rec["name"] == ark1

    ark2 = "ark:/31807/TEST-bbb"
    res = client.put("/alias/" + ark2, json=data, headers=user)
    assert res.status_code == 200
    rec = res.json
    assert rec["name"] == ark2

    ark3 = "ark:/31807/TEST-ccc"
    res = client.put("/alias/" + ark3, json=data, headers=user)
    assert res.status_code == 200
    rec = res.json
    assert rec["name"] == ark3

    res = client.get("/alias/?start={}&size={}".format(ark1, data["size"]))
    assert res.status_code == 200
    rec = res.json
    assert len(rec["aliases"]) == 2
    assert ark2 in rec["aliases"]
    assert ark3 in rec["aliases"]


def test_alias_create(client, user):
    data = {
        "size": 123,
        "hashes": {"md5": "8b9942cf415384b27cadf1f4d2d682e5"},
        "release": "private",
        "keeper_authority": "CRI",
        "host_authorities": ["PDC"],
    }
    ark = "ark:/31807/TEST-abc"
    res = client.put("/alias/" + ark, json=data, headers=user)
    assert res.status_code == 200
    rec = res.json
    assert rec["name"] == ark

    assert len(client.get("/alias/").json["aliases"]) == 1
    assert client.get("/alias/" + rec["name"]).json["name"] == ark


def test_alias_get_global_endpoint(client, user):
    data = {
        "size": 123,
        "hashes": {"md5": "8b9942cf415384b27cadf1f4d2d682e5"},
        "release": "private",
        "keeper_authority": "CRI",
        "host_authorities": ["PDC"],
    }
    ark = "ark:/31807/TEST-abc"

    res = client.put("/alias/" + ark, json=data, headers=user)
    assert res.status_code == 200

    assert client.get("/" + ark).json["size"] == 123


def test_alias_update(client, user):
    data = {
        "size": 123,
        "hashes": {"md5": "8b9942cf415384b27cadf1f4d2d682e5"},
        "release": "private",
        "keeper_authority": "CRI",
        "host_authorities": ["PDC"],
    }
    ark = "ark:/31807/TEST-abc"

    res_1 = client.put("/alias/" + ark, json=data, headers=user)
    assert res_1.status_code == 200
    rec_1 = res_1.json
    assert rec_1["rev"]

    dataNew = {
        "size": 456,
        "hashes": {"md5": "8b9942cf415384b27cadf1f4d2d682e5"},
        "release": "private",
        "keeper_authority": "CRI",
        "host_authorities": ["PDC"],
    }
    res_2 = client.put(
        "/alias/{}?rev={}".format(ark, rec_1["rev"]), json=dataNew, headers=user
    )
    assert res_2.status_code == 200
    rec_2 = res_2.json
    assert rec_2["rev"] != rec_1["rev"]


def test_alias_delete(client, user):
    data = {
        "size": 123,
        "hashes": {"md5": "8b9942cf415384b27cadf1f4d2d682e5"},
        "release": "private",
        "keeper_authority": "CRI",
        "host_authorities": ["PDC"],
    }
    ark = "ark:/31807/TEST-abc"

    res_1 = client.put("/alias/" + ark, json=data, headers=user)
    assert res_1.status_code == 200
    rec_1 = res_1.json
    assert rec_1["rev"]

    res = client.delete(
        "/alias/{}?rev={}".format(ark, rec_1["rev"]), json=data, headers=user
    )
    assert res.status_code == 200

    assert len(client.get("/alias/").json["aliases"]) == 0
