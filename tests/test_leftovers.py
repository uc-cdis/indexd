from indexd.blueprint import get_record
from tests.test_client import get_doc
import pytest
from indexd.index.errors import NoRecordFound as IndexNoRecordFound
from indexd.dos.blueprint import get_dos_record
import json


def test_get_record_call_dist_get_record(client, user):
    # test get_record correctly
    data = get_doc()
    res = client.post("/index/", json=data, headers=user)
    assert res.status_code == 200
    rec = res.json
    response = get_record(rec["did"])
    record = response[0].json
    assert record["urls"][0] == "s3://endpointurl/bucket/key"
    assert record["size"] == 123

    # test call dist_get_record for unposted record
    fake_did = "testprefix:d96bab16-c4e1-44ac-923a-04328b6fe78f"
    with pytest.raises(IndexNoRecordFound):
        get_record(fake_did)


def test_get_dos_record(client, user):
    data = get_doc(has_urls_metadata=True, has_metadata=True, has_baseid=True)

    res_1 = client.post("/index/", json=data, headers=user)
    assert res_1.status_code == 200
    rec_1 = res_1.json

    # test exception raised at nonexistent
    fake_did = "16e51a9a-af23-45a3-b4b2-94ae2b143feb"
    with pytest.raises(IndexNoRecordFound):
        get_dos_record(fake_did)


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
    assert len(client.get("/alias/").json["aliases"]) == 1
    res = client.get("/alias/?hash=" + "md5:8b9942cf415384b27cadf1f4d2d682e5")
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
    assert len(client.get("/alias/").json["aliases"]) == 1
    # assert that the returned alias by size is the same as the posted
    res = client.get("/alias/?size=" + str(data["size"]))
    rec = res.json
    assert rec["aliases"][0] == ark


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
    file_number = index_stats["fileCount"]
    index_file_size = index_stats["totalFileSize"]
    # test that the stat file number and size is consistent with post
    assert file_number == 3
    assert index_file_size == data_size


def test_status_check(client, user):
    data = get_doc()
    res = client.post("/index/", json=data, headers=user)
    assert res.status_code == 200
    res = client.get("/_status/")
    assert res.status_code == 200


def test_version_check(client, user):
    data = get_doc()
    res = client.post("/index/", json=data, headers=user)
    assert res.status_code == 200
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
    res = client.get("/index/?size=" + str(data["size"]))
    assert res.status_code == 200
    rec = res.json
    # assert only one record returned and returned with proper size
    assert len(rec["records"]) == 1
    assert rec["records"][0]["size"] == 100


def test_index_list_by_file_name(client, user):
    # post two records of different name
    data = get_doc()
    res = client.post("/index/", json=data, headers=user)
    assert res.status_code == 200
    data["file_name"] = "test_file"
    res = client.post("/index/", json=data, headers=user)
    assert res.status_code == 200
    res = client.get("/index/?file_name=" + data["file_name"])
    assert res.status_code == 200
    rec = res.json
    # assert only one record returned and returned with proper name
    assert len(rec["records"]) == 1
    assert rec["records"][0]["file_name"] == data["file_name"]


def test_index_list_by_authz(client, user):
    # post two records of different authz
    data = get_doc()
    res = client.post("/index/", json=data, headers=user)
    assert res.status_code == 200
    data["authz"] = ["test_authz"]
    res = client.post("/index/", json=data, headers=user)
    assert res.status_code == 200
    res = client.get("/index/?authz=" + data["authz"][0])
    assert res.status_code == 200
    rec = res.json
    # assert only one record returned and returned with proper authz
    assert len(rec["records"]) == 1
    assert rec["records"][0]["authz"] == data["authz"]


def test_index_list_by_version(client, user):
    # post two records of different version
    data = get_doc()
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
    rec_1 = res_1.json

    data2 = get_doc()
    data2["file_name"] = "test_file_name_2"
    res_2 = client.post("/index/", json=data2, headers=user)
    assert res_2.status_code == 200
    rec_2 = res_2.json

    negate_param = {"file_name": "test_file_name_2"}
    res = client.get("/index/?negate_params=" + json.dumps(negate_param))
    assert res.status_code == 200
    rec = res.json
    # assert record returned with proper non-negated file name
    assert len(rec["records"]) == 1
    assert rec["records"][0]["file_name"] == data1["file_name"]


def test_negate_filter_acl(client, user):
    # post two records of different acl
    data1 = get_doc()
    data1["acl"] = ["read"]
    res_1 = client.post("/index/", json=data1, headers=user)
    assert res_1.status_code == 200
    rec_1 = res_1.json

    data2 = get_doc()
    data2["acl"] = ["unread"]
    res_2 = client.post("/index/", json=data2, headers=user)
    assert res_2.status_code == 200
    rec_2 = res_2.json

    negate_param = {"acl": ["unread"]}
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
    rec_1 = res_1.json

    data2 = get_doc()
    data2["authz"] = ["user"]
    res_2 = client.post("/index/", json=data2, headers=user)
    assert res_2.status_code == 200
    rec_2 = res_2.json

    negate_param = {"authz": ["user"]}
    res = client.get("/index/?negate_params=" + json.dumps(negate_param))
    assert res.status_code == 200
    rec = res.json
    # assert record returned with proper non-negated authz
    assert len(rec["records"]) == 1
    assert rec["records"][0]["authz"] == data1["authz"]


def test_negate_filter_version(client, user):
    # post two records of different version
    data1 = get_doc()
    data1["version"] = "10"
    res_1 = client.post("/index/", json=data1, headers=user)
    assert res_1.status_code == 200
    rec_1 = res_1.json

    data2 = get_doc()
    data2["version"] = "1"
    res_2 = client.post("/index/", json=data2, headers=user)
    assert res_2.status_code == 200
    rec_2 = res_2.json

    negate_param = {"version": "1"}
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


def test_authz_response(client, user):
    # test that unauthorized post throws 403 error
    data = get_doc()
    res = client.post("/index/", json=data)
    assert res.status_code == 403
