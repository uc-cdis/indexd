from indexd.blueprint import get_record
from tests.test_client import get_doc
import pytest
import json


def test_get_record_error(client, user):
    # test getting an existing ID
    data = get_doc()
    res = client.post("/index/", json=data, headers=user)
    assert res.status_code == 200
    rec = res.json
    response = client.get("/index/" + rec["did"])
    assert response.status_code == 200
    record = response.json
    assert record["urls"][0] == data["urls"][0]
    assert record["size"] == data["size"]

    # test getting an ID that does not exist
    fake_did = "testprefix:d96bab16-c4e1-44ac-923a-04328b6fe78f"
    res = client.get("/index/" + fake_did)
    assert res.status_code == 404
    res = client.get("/alias/" + fake_did)
    assert res.status_code == 404


def test_get_dos_record_error(client, user):
    data = get_doc(has_urls_metadata=True, has_metadata=True, has_baseid=True)

    res = client.post("/index/", json=data, headers=user)
    assert res.status_code == 200

    # test exception raised at nonexistent
    fake_did = "testprefix:d96bab16-c4e1-44ac-923a-04328b6fe78f"
    res = client.get("/ga4gh/dos/v1/dataobjects/" + fake_did)
    assert res.status_code == 500


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

    res = client.get("/alias/?start={}&size={}".format(ark1, str(data["size"])))
    assert res.status_code == 200
    rec = res.json
    assert len(rec["aliases"]) == 2
    assert ark2 in rec["aliases"]
    assert ark3 in rec["aliases"]


def test_index_stats(client, user):
    # populate the index with three different size records
    data1 = get_doc()
    res = client.post("/index/", json=data1, headers=user)
    assert res.status_code == 200
    data2 = get_doc()
    data2["size"] = 77
    res = client.post("/index/", json=data2, headers=user)
    assert res.status_code == 200
    data3 = get_doc()
    data3["size"] = 300
    res = client.post("/index/", json=data3, headers=user)
    assert res.status_code == 200
    data_size = data1["size"] + data2["size"] + data3["size"]
    index_stats = client.get("/_stats/").json

    # test that the stat file number and size is consistent with post
    assert index_stats["fileCount"] == 3
    assert index_stats["totalFileSize"] == data_size


def test_status_check(client):
    res = client.get("/_status/")
    assert res.status_code == 200


def test_version_check(client):
    res = client.get("/_version")
    assert res.status_code == 200


def test_index_list_by_size(client, user):
    # post two records of different size
    data = get_doc()
    res = client.post("/index/", json=data, headers=user)
    assert res.status_code == 200
    data["size"] = 100
    res = client.post("/index/", json=data, headers=user)
    assert res.status_code == 200
    res = client.get("/index/?size={}".format(data["size"]))
    assert res.status_code == 200
    rec = res.json
    # assert only one record returned and returned with proper size
    assert len(rec["records"]) == 1
    assert rec["records"][0]["size"] == 100


def test_index_list_by_file_name(client, user):
    # post three records of different name
    data = get_doc()
    res = client.post("/index/", json=data, headers=user)
    assert res.status_code == 200
    data["file_name"] = "test_file_1"
    res = client.post("/index/", json=data, headers=user)
    assert res.status_code == 200
    data["file_name"] = "test_file_2"
    res = client.post("/index/", json=data, headers=user)
    assert res.status_code == 200
    res = client.get("/index/?file_name=" + data["file_name"])
    assert res.status_code == 200
    rec = res.json
    # assert only one record returned and returned with proper name
    assert len(rec["records"]) == 1
    assert rec["records"][0]["file_name"] == data["file_name"]


def test_index_list_by_authz(client, user):
    # post three records of different authz
    data = get_doc()
    res = client.post("/index/", json=data, headers=user)
    assert res.status_code == 200
    data["authz"] = ["test_authz_1"]
    res = client.post("/index/", json=data, headers=user)
    assert res.status_code == 200
    data["authz"] = ["test_authz_2"]
    res = client.post("/index/", json=data, headers=user)
    assert res.status_code == 200
    res = client.get("/index/?authz=" + data["authz"][0])
    assert res.status_code == 200
    rec = res.json
    # assert only one record returned and returned with proper authz
    assert len(rec["records"]) == 1
    assert rec["records"][0]["authz"] == data["authz"]


def test_index_list_by_version(client, user):
    # post three records of different version
    data = get_doc()
    res = client.post("/index/", json=data, headers=user)
    assert res.status_code == 200
    data["version"] = "2"
    res = client.post("/index/", json=data, headers=user)
    assert res.status_code == 200
    data["version"] = "3"
    res = client.post("/index/", json=data, headers=user)
    assert res.status_code == 200
    res = client.get("/index/?version=" + data["version"])
    assert res.status_code == 200
    rec = res.json
    # assert only one record returned and returned with proper version
    assert len(rec["records"]) == 1
    assert rec["records"][0]["version"] == data["version"]


def test_negate_filter_file_name(client, user):
    # post two records of different file name
    data1 = get_doc()
    data1["file_name"] = "test_file_name_1"
    res_1 = client.post("/index/", json=data1, headers=user)
    assert res_1.status_code == 200

    data2 = get_doc()
    data2["file_name"] = "test_file_name_2"
    res_2 = client.post("/index/", json=data2, headers=user)
    assert res_2.status_code == 200

    data3 = get_doc()
    res_3 = client.post("/index/", json=data3, headers=user)
    assert res_3.status_code == 200

    negate_param = {"file_name": data2["file_name"]}
    res = client.get("/index/?negate_params=" + json.dumps(negate_param))
    assert res.status_code == 200
    rec = res.json
    print(rec)
    # assert record returned with proper non-negated file name
    assert len(rec["records"]) == 1
    assert rec["records"][0]["file_name"] == data1["file_name"]


def test_negate_filter_acl(client, user):
    # post two records of different acl
    data1 = get_doc()
    data1["acl"] = ["read"]
    res_1 = client.post("/index/", json=data1, headers=user)
    assert res_1.status_code == 200

    data2 = get_doc()
    data2["acl"] = ["unread"]
    res_2 = client.post("/index/", json=data2, headers=user)
    assert res_2.status_code == 200

    data3 = get_doc()
    res_3 = client.post("/index/", json=data3, headers=user)
    assert res_3.status_code == 200

    negate_param = {"acl": data2["acl"]}
    res = client.get("/index/?negate_params=" + json.dumps(negate_param))
    assert res.status_code == 200
    rec = res.json
    # assert record returned with proper non-negated acl
    assert len(rec["records"]) == 1
    assert rec["records"][0]["acl"] == data1["acl"]


def test_negate_filter_authz(client, user):
    # post two records of different authz
    data1 = get_doc()
    data1["authz"] = ["admin"]
    res_1 = client.post("/index/", json=data1, headers=user)
    assert res_1.status_code == 200

    data2 = get_doc()
    data2["authz"] = ["user"]
    res_2 = client.post("/index/", json=data2, headers=user)
    assert res_2.status_code == 200

    data3 = get_doc()
    res_2 = client.post("/index/", json=data3, headers=user)
    assert res_2.status_code == 200

    negate_param = {"authz": data2["authz"]}
    res = client.get("/index/?negate_params=" + json.dumps(negate_param))
    assert res.status_code == 200
    rec = res.json
    # assert record returned with proper non-negated authz
    assert len(rec["records"]) == 1
    assert rec["records"][0]["authz"] == data1["authz"]


def test_negate_filter_version(client, user):
    # post two records of different version
    data1 = get_doc()
    data1["version"] = "3"
    res_1 = client.post("/index/", json=data1, headers=user)
    assert res_1.status_code == 200

    data2 = get_doc()
    data2["version"] = "2"
    res_2 = client.post("/index/", json=data2, headers=user)
    assert res_2.status_code == 200

    data3 = get_doc()
    res_2 = client.post("/index/", json=data2, headers=user)
    assert res_2.status_code == 200

    negate_param = {"version": data2["version"]}
    res = client.get("/index/?negate_params=" + json.dumps(negate_param))
    assert res.status_code == 200
    rec = res.json
    # assert record returned with proper non-negated version
    assert len(rec["records"]) == 1
    assert rec["records"][0]["version"] == data1["version"]


def test_bad_update_blank_record(client, user):
    doc = {"file_name": "test_file_name_1", "uploader": "test_uploader_1"}
    res = client.post("/index/blank", json=doc, headers=user)
    assert res.status_code == 201
    rec = res.json

    # test that empty update throws 400 error
    data = {"size": "", "hashes": {"": ""}}
    res = client.put(
        "/index/blank/{}?rev={}".format(rec["did"], rec["rev"]), json=data, headers=user
    )
    assert res.status_code == 400

    # test that non-existent did throws 400 error
    data = {"size": 123, "hashes": {"md5": "8b9942cf415384b27cadf1f4d2d682e5"}}
    fake_did = "testprefix:455ffb35-1b0e-49bd-a4ab-3afe9f3aece9"
    fake_rev = "8d19b5c10"
    res = client.put(
        "/index/blank/{}?rev={}".format(fake_did, fake_rev), json=data, headers=user
    )
    assert res.status_code == 404


def test_unauthorized_create(client):
    # test that unauthorized post throws 403 error
    data = get_doc()
    res = client.post("/index/", json=data)
    assert res.status_code == 403


def test_index_list_with_start(client, user):
    data = {
        "did": "testprefix:11111111-1111-1111-1111-111111111111",
        "form": "object",
        "size": 123,
        "urls": ["s3://endpointurl/bucket/key"],
        "hashes": {"md5": "8b9942cf415384b27cadf1f4d2d682e5"},
    }
    res = client.post("/index/", json=data, headers=user)
    assert res.status_code == 200
    rec1 = res.json

    data["did"] = "testprefix:22222222-2222-2222-2222-222222222222"
    res = client.post("/index/", json=data, headers=user)
    assert res.status_code == 200
    rec2 = res.json

    data["did"] = "testprefix:33333333-3333-3333-3333-333333333333"
    res = client.post("/index/", json=data, headers=user)
    assert res.status_code == 200
    rec3 = res.json

    res = client.get("/index/?start=" + rec1["did"])
    assert res.status_code == 200
    rec = res.json

    dids = [record["did"] for record in rec["records"]]
    assert len(rec["records"]) == 2
    assert rec2["did"] in dids
    assert rec3["did"] in dids
