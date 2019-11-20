import json

import pytest

from tests.util import assert_blank
from indexd.index.blueprint import ACCEPTABLE_HASHES


def get_doc(
    has_metadata=True, has_baseid=False, has_urls_metadata=False, has_version=False
):
    doc = {
        "form": "object",
        "size": 123,
        "urls": ["s3://endpointurl/bucket/key"],
        "hashes": {"md5": "8b9942cf415384b27cadf1f4d2d682e5"},
    }
    if has_metadata:
        doc["metadata"] = {"project_id": "bpa-UChicago"}
    if has_baseid:
        doc["baseid"] = "e044a62c-fd60-4203-b1e5-a62d1005f027"
    if has_urls_metadata:
        doc["urls_metadata"] = {"s3://endpointurl/bucket/key": {"state": "uploaded"}}
    if has_version:
        doc["version"] = "1"
    return doc


def test_index_list(client):
    res = client.get("/index/")
    assert res.status_code == 200
    rec = res.json
    assert rec["records"] == []


def test_index_list_with_params(client, user):
    data1 = get_doc()
    data1["urls"] = [
        "s3://endpointurl/bucket_2/key_2",
        "s3://anotherurl/bucket_2/key_2",
    ]
    data1["urls_metadata"] = {
        "s3://endpointurl/bucket_2/key_2": {"state": "error", "other": "xxx"},
        "s3://anotherurl/bucket_2/key_2": {"state": "error", "other": "xxx"},
    }
    res_1 = client.post("/index/", json=data1, headers=user)
    assert res_1.status_code == 200
    rec_1 = res_1.json

    data2 = get_doc()
    data2["metadata"] = {"project_id": "other-project", "state": "abc", "other": "xxx"}
    data2["urls"] = ["s3://endpointurl/bucket/key_2", "s3://anotherurl/bucket/key_2"]
    data2["urls_metadata"] = {
        "s3://endpointurl/bucket/key_2": {"state": "error", "other": "yyy"}
    }
    res_2 = client.post("/index/", json=data2, headers=user)
    assert res_2.status_code == 200
    rec_2 = res_2.json

    data1_by_md = client.get("/index/?metadata=project_id:bpa-UChicago")
    assert data1_by_md.status_code == 200
    data1_list = data1_by_md.json
    ids = [record["did"] for record in data1_list["records"]]
    assert rec_1["did"] in ids

    data2_by_md = client.get("/index/?metadata=project_id:other-project")
    assert data2_by_md.status_code == 200
    data2_list = data2_by_md.json
    ids = [record["did"] for record in data2_list["records"]]
    assert rec_2["did"] in ids

    data_by_hash = client.get("/index/?hash=md5:8b9942cf415384b27cadf1f4d2d682e5")
    assert data_by_hash.status_code == 200
    data_list_all = data_by_hash.json
    ids = [record["did"] for record in data_list_all["records"]]
    assert rec_1["did"] in ids
    assert rec_2["did"] in ids

    data_by_ids = client.get("/index/?ids={}".format(rec_1["did"]))
    assert data_by_ids.status_code == 200
    data_list_all = data_by_ids.json

    ids = [record["did"] for record in data_list_all["records"]]
    assert rec_1["did"] in ids
    assert not rec_2["did"] in ids

    data_with_limit = client.get("/index/?limit=1")
    assert data_with_limit.status_code == 200
    data_list_limit = data_with_limit.json
    assert len(data_list_limit["records"]) == 1

    param = {"bucket": {"state": "error", "other": "xxx"}}

    data_by_url_md = client.get("/index/?urls_metadata=" + json.dumps(param))
    assert data_by_url_md.status_code == 200
    data_list = data_by_url_md.json
    assert len(data_list["records"]) == 1
    assert data_list["records"][0]["did"] == rec_1["did"]
    assert data_list["records"][0]["urls_metadata"] == data1["urls_metadata"]


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


def test_index_list_by_filename(client, user):
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


def test_index_list_by_multiple_authz(client, user):
    data = get_doc()

    data["authz"] = ["abc"]
    res = client.post("/index/", json=data, headers=user)
    assert res.status_code == 200

    data["authz"] = ["abc", "rst", "xyz"]
    res = client.post("/index/", json=data, headers=user)
    assert res.status_code == 200

    # query the record with multiple authz elements
    res = client.get("/index/?authz=" + ",".join(data["authz"]))
    assert res.status_code == 200
    rec = res.json
    assert len(rec["records"]) == 1
    assert rec["records"][0]["authz"] == data["authz"]


def test_index_list_by_multiple_acl(client, user):
    data = get_doc()

    data["acl"] = ["abc"]
    res = client.post("/index/", json=data, headers=user)
    assert res.status_code == 200

    data["acl"] = ["abc", "rst", "xyz"]
    res = client.post("/index/", json=data, headers=user)
    assert res.status_code == 200

    # query the record with multiple ACL elements
    res = client.get("/index/?acl=" + ",".join(data["acl"]))
    assert res.status_code == 200
    rec = res.json
    assert len(rec["records"]) == 1
    assert rec["records"][0]["acl"] == data["acl"]


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


def test_index_list_with_params_negate(client, user):
    data = get_doc()
    res_1 = client.post("/index/", json=data, headers=user)
    assert res_1.status_code == 200
    rec_1 = res_1.json

    data["metadata"] = {"testkey": "test", "project_id": "negate-project"}
    res_2 = client.post("/index/", json=data, headers=user)
    assert res_2.status_code == 200
    rec_2 = res_2.json

    data["urls"] = ["s3://endpointurl/bucket_2/key_2", "s3://anotherurl/bucket_2/key_2"]
    data["urls_metadata"] = {"s3://endpointurl/bucket_2/key_2": {"state": "error"}}
    res_3 = client.post("/index/", json=data, headers=user)
    assert res_3.status_code == 200
    rec_3 = res_3.json

    data["urls"] = ["s3://endpointurl/bucket_2/key_2"]
    data["urls_metadata"] = {
        "s3://endpointurl/bucket_2/key_2": {"no_state": "uploaded"}
    }
    res_4 = client.post("/index/", json=data, headers=user)
    assert res_4.status_code == 200
    rec_4 = res_4.json

    data["urls"] = ["s3://anotherurl/bucket/key"]
    data["urls_metadata"] = {"s3://anotherurl/bucket/key": {"state": "error"}}
    res_5 = client.post("/index/", json=data, headers=user)
    assert res_5.status_code == 200
    rec_5 = res_5.json

    negate_params = {"metadata": {"testkey": ""}}
    data_neg_param = client.get("/index/?negate_params=" + json.dumps(negate_params))
    assert data_neg_param.status_code == 200
    data_list = data_neg_param.json
    assert len(data_list["records"]) == 1
    assert data_list["records"][0]["did"] == rec_1["did"]

    negate_params = {"metadata": {"project_id": "bpa-UChicago"}}
    data_neg_param = client.get("/index/?negate_params=" + json.dumps(negate_params))
    assert data_neg_param.status_code == 200
    data_list = data_neg_param.json
    ids = {record["did"] for record in data_list["records"]}
    assert len(ids) == 4
    assert rec_1["did"] not in ids
    assert rec_2["did"] in ids
    assert rec_3["did"] in ids
    assert rec_4["did"] in ids
    assert rec_5["did"] in ids

    # negate url
    negate_params = {"urls": ["s3://endpointurl/bucket_2/key_2"]}
    data_neg_param = client.get("/index/?negate_params=" + json.dumps(negate_params))
    assert data_neg_param.status_code == 200
    data_list = data_neg_param.json
    ids = {record["did"] for record in data_list["records"]}
    assert len(ids) == 3
    assert rec_1["did"] in ids
    assert rec_2["did"] in ids
    assert rec_3["did"] not in ids
    assert rec_4["did"] not in ids
    assert rec_5["did"] in ids

    # negate url key
    negate_params = {"urls_metadata": {"s3://endpointurl/": {}}}
    data_neg_param = client.get("/index/?negate_params=" + json.dumps(negate_params))
    assert data_neg_param.status_code == 200
    data_list = data_neg_param.json
    assert len(data_list["records"]) == 1
    assert data_list["records"][0]["did"] == rec_5["did"]

    negate_params = {"urls_metadata": {"s3://endpointurl/": {}, "s3://anotherurl/": {}}}
    data_neg_param = client.get("/index/?negate_params=" + json.dumps(negate_params))
    assert data_neg_param.status_code == 200
    data_list = data_neg_param.json
    assert len(data_list["records"]) == 0

    # negate url_metadata key
    negate_params = {
        "urls_metadata": {"s3://endpointurl/": {"state": ""}, "s3://anotherurl/": {}}
    }
    data_neg_param = client.get("/index/?negate_params=" + json.dumps(negate_params))
    assert data_neg_param.status_code == 200
    data_list = data_neg_param.json
    assert len(data_list["records"]) == 1
    assert data_list["records"][0]["did"] == rec_4["did"]

    # negate url_metadata value
    negate_params = {"urls_metadata": {"s3://endpointurl/": {"state": "uploaded"}}}
    data_neg_param = client.get("/index/?negate_params=" + json.dumps(negate_params))
    assert data_neg_param.status_code == 200
    data_list = data_neg_param.json
    ids = {record["did"] for record in data_list["records"]}
    assert len(ids) == 3
    assert rec_1["did"] not in ids
    assert rec_2["did"] not in ids
    assert rec_3["did"] in ids
    assert rec_4["did"] in ids
    assert rec_5["did"] in ids


def test_index_list_invalid_param(client):
    # test 400 when limit > 1024
    res = client.get("/index/?limit=1025")
    assert res.status_code == 400

    # test 400 when limit < 0
    res = client.get("/index/?limit=-1")
    assert res.status_code == 400

    # test 400 when limit not int
    res = client.get("/index/?limit=string")
    assert res.status_code == 400

    # test 400 when size < 0
    res = client.get("/index/?size=-1")
    assert res.status_code == 400

    # test 400 when size not int
    res = client.get("/index/?size=string")
    assert res.status_code == 400


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
    res_3 = client.post("/index/", json=data3, headers=user)
    assert res_3.status_code == 200

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
    res_2 = client.post("/index/", json=data3, headers=user)
    assert res_2.status_code == 200

    negate_param = {"version": data2["version"]}
    res = client.get("/index/?negate_params=" + json.dumps(negate_param))
    assert res.status_code == 200
    rec = res.json
    # assert record returned with proper non-negated version
    assert len(rec["records"]) == 1
    assert rec["records"][0]["version"] == data1["version"]


def test_list_entries_with_uploader(client, user):
    """
    Test that return a list of record given uploader
    """
    data = get_doc()
    data["uploader"] = "uploader_1"
    res_1 = client.post("/index/", json=data, headers=user)
    assert res_1.status_code == 200

    data = get_doc()
    data["uploader"] = "uploader_123"
    res_2 = client.post("/index/", json=data, headers=user)
    assert res_2.status_code == 200
    rec_2 = res_2.json

    data = get_doc()
    data["uploader"] = "uploader_123"
    res_3 = client.post("/index/", json=data, headers=user)
    assert res_3.status_code == 200
    rec_3 = res_3.json

    data_grab = client.get("/index/?uploader=uploader_123")
    assert data_grab.status_code == 200
    data_list = data_grab.json
    assert len(data_list["records"]) == 2
    ids = {record["did"] for record in data_list["records"]}
    assert len(ids) == 2
    assert rec_2["did"] in ids
    assert rec_3["did"] in ids
    assert data_list["records"][0]["uploader"] == "uploader_123"
    assert data_list["records"][1]["uploader"] == "uploader_123"


def test_list_entries_with_uploader_wrong_uploader(client, user):
    """
    Test that returns no record due to wrong uploader id
    """
    data = get_doc()
    data["uploader"] = "uploader_1"
    res = client.post("/index/", json=data, headers=user)
    assert res.status_code == 200

    data = get_doc()
    data["uploader"] = "uploader_123"
    res = client.post("/index/", json=data, headers=user)
    assert res.status_code == 200

    data = get_doc()
    data["uploader"] = "uploader_123"
    res = client.post("/index/", json=data, headers=user)
    assert res.status_code == 200

    data_grab = client.get("/index/?uploader=wrong_uploader")
    data_list = data_grab.json
    assert len(data_list["records"]) == 0


def test_create_blank_record(client, user):
    """
    Test that new blank records only contain the uploader
    and optionally file_name fields: test without file name
    """

    doc = {"uploader": "uploader_123"}
    res = client.post("/index/blank/", json=doc, headers=user)
    assert res.status_code == 201
    rec = res.json
    assert rec["did"]
    assert rec["rev"]
    assert rec["baseid"]

    res = client.get("/index/?uploader=uploader_123")
    assert res.status_code == 200
    rec = res.json
    assert rec["records"][0]["uploader"] == "uploader_123"
    assert not rec["records"][0]["file_name"]

    # test that record is blank
    assert_blank(rec)


def test_create_blank_record_with_file_name(client, user):
    """
    Test that new blank records only contain the uploader
    and optionally file_name fields: test with file name
    """

    doc = {"uploader": "uploader_321", "file_name": "myfile.txt"}
    res = client.post("/index/blank/", json=doc, headers=user)
    assert res.status_code == 201
    rec = res.json
    assert rec["did"]
    assert rec["rev"]
    assert rec["baseid"]

    res = client.get("/index/?uploader=uploader_321")
    assert res.status_code == 200
    rec = res.json
    assert rec["records"][0]["uploader"] == "uploader_321"
    assert rec["records"][0]["file_name"] == "myfile.txt"

    # test that record is blank
    assert_blank(rec)


def test_fill_size_n_hash_for_blank_record(client, user):
    """
    Test that can fill size and hashes for empty record
    """
    doc = {"uploader": "uploader_123"}

    res = client.post("/index/blank/", json=doc, headers=user)
    assert res.status_code == 201
    rec = res.json
    assert rec["did"]
    assert rec["rev"]

    did, rev = rec["did"], rec["rev"]
    updated = {"size": 10, "hashes": {"md5": "8b9942cf415384b27cadf1f4d2d981f5"}}

    res = client.put(
        "/index/blank/{}?rev={}".format(did, rev), headers=user, json=updated
    )
    assert res.status_code == 200
    rec = res.json
    assert rec["did"] == did
    assert rec["rev"] != rev

    res = client.get("/index/" + did)
    assert res.status_code == 200
    rec = res.json
    assert rec["size"] == 10
    assert rec["hashes"]["md5"] == "8b9942cf415384b27cadf1f4d2d981f5"


def test_get_empty_acl_authz_record(client, user):
    """
    Test that can get a list of empty acl/authz given uploader
    """
    doc = get_doc()
    client.post("/index/", json=doc, headers=user)

    doc = {"uploader": "uploader_123"}
    res_2 = client.post("/index/blank/", json=doc, headers=user)
    assert res_2.status_code == 201
    rec_2 = res_2.json

    doc = {"uploader": "uploader_123"}
    res_3 = client.post("/index/blank/", json=doc, headers=user)
    assert res_3.status_code == 201
    rec_3 = res_3.json

    data_grab = client.get("/index/")
    assert data_grab.status_code == 200
    data_list = data_grab.json
    assert len(data_list["records"]) == 3

    data_by_acl_authz = client.get("/index/?uploader=uploader_123&acl=null&authz=null")
    assert data_by_acl_authz.status_code == 200
    data_list = data_by_acl_authz.json

    assert len(data_list["records"]) == 2
    ids = {record["did"] for record in data_list["records"]}
    assert len(ids) == 2
    assert rec_2["did"] in ids
    assert rec_3["did"] in ids
    assert data_list["records"][0]["acl"] == []
    assert data_list["records"][1]["acl"] == []
    assert data_list["records"][0]["authz"] == []
    assert data_list["records"][1]["authz"] == []


def test_get_empty_acl_authz_record_after_fill_size_n_hash(client, user):
    """
    Test create blank record -> fill hash and size -> get record with empty or none
    acl/authz
    """
    # create the first blank record, update size, hashes and acl/authz
    doc = {"uploader": "uploader_123"}
    res_1 = client.post("/index/blank/", json=doc, headers=user)
    assert res_1.status_code == 201
    rec_1 = res_1.json
    updated = {"size": 10, "hashes": {"md5": "8b9942cf415384b27cadf1f4d2d981f5"}}
    did_1 = rec_1["did"]
    rev_1 = rec_1["rev"]
    res_2 = client.put(
        "/index/blank/{}?rev={}".format(did_1, rev_1), headers=user, json=updated
    )
    assert res_2.status_code == 200
    rec_2 = res_2.json
    rev_2 = rec_2["rev"]
    body = {"acl": ["read"], "authz": ["read"]}
    res_1 = client.put("/index/{}?rev={}".format(did_1, rev_2), headers=user, json=body)
    assert res_1.status_code == 200
    rec_1 = res_1.json
    res_1 = client.get("/index/" + rec_1["did"])
    assert res_1.status_code == 200
    rec_1 = res_1.json
    assert rec_1["acl"] == ["read"]
    assert rec_1["authz"] == ["read"]
    assert rec_1["did"] == did_1

    # create the second blank record, only update size hashes and urls
    doc = {"uploader": "uploader_123"}
    res_2 = client.post("/index/blank/", json=doc, headers=user)
    assert res_2.status_code == 201
    rec_2 = res_2.json
    did_2 = rec_2["did"]
    updated = {
        "size": 4,
        "hashes": {"md5": "1b9942cf415384b27cadf1f4d2d981f5"},
        "urls": ["s3://example/1"],
    }
    res = client.put(
        "/index/blank/{}?rev={}".format(rec_2["did"], rec_2["rev"]),
        json=updated,
        headers=user,
    )
    assert res.status_code == 200

    # create the third blank record, only update size hashes and urls
    doc = {"uploader": "uploader_123"}
    res_3 = client.post("/index/blank/", json=doc, headers=user)
    assert res_3.status_code == 201
    rec_3 = res_3.json
    did_3 = rec_3["did"]
    updated = {
        "size": 4,
        "hashes": {"md5": "1b9942cf415384b27cadf1f4d2d981f5"},
        "urls": ["s3://example/2"],
    }
    res = client.put(
        "/index/blank/{}?rev={}".format(rec_3["did"], rec_3["rev"]),
        json=updated,
        headers=user,
    )
    assert res.status_code == 200

    res = client.get("/index/?uploader=uploader_123")
    assert res.status_code == 200
    rec = res.json
    print(rec)
    assert len(rec["records"]) == 3

    res = client.get("/index/?uploader=uploader_123&acl=read")
    assert res.status_code == 200
    rec = res.json
    assert len(rec["records"]) == 1
    assert rec["records"][0]["did"] == rec_1["did"]

    res = client.get("/index/?uploader=uploader_123&acl=write")
    assert res.status_code == 200
    rec = res.json
    assert len(rec["records"]) == 0

    res = client.get("/index/?uploader=uploader_123&acl=null")
    assert res.status_code == 200
    rec = res.json
    assert len(rec["records"]) == 2

    ids = {record["did"] for record in rec["records"]}
    assert did_2 in ids
    assert did_3 in ids
    assert len(ids) == 2


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


def test_update_urls_metadata(client, user):
    data = get_doc(has_urls_metadata=True)
    res = client.post("/index/", json=data, headers=user)
    assert res.status_code == 200
    rec = res.json

    res_2 = client.get("/index/" + rec["did"])
    assert res_2.status_code == 200
    rec_2 = res_2.json
    assert rec_2["urls_metadata"] == data["urls_metadata"]

    updated = {"urls_metadata": {data["urls"][0]: {"test": "b"}}}
    res = client.put(
        "/index/{}?rev={}".format(rec_2["did"], rec_2["rev"]),
        json=updated,
        headers=user,
    )
    assert res.status_code == 200

    res_3 = client.get("/index/" + rec["did"])
    assert res_3.status_code == 200
    rec_3 = res_3.json
    assert rec_3["urls_metadata"] == updated["urls_metadata"]


@pytest.mark.parametrize(
    "doc_urls,urls_meta,params,expected",
    [
        (
            [["s3://endpoint/key_1"], ["s3://endpoint/key_2"], ["s3://endpoint/key_3"]],
            {
                "s3://endpoint/key_1": {"state": "uploaded"},
                "s3://endpoint/key_2": {"state": "validated"},
                "s3://endpoint/key_3": {"state": "uploaded", "type": "ceph"},
            },
            {"s3://endpoint": {"state": "uploaded"}},
            ["s3://endpoint/key_1", "s3://endpoint/key_3"],
        ),
        (
            [["s3://endpoint/key_1"], ["s3://endpoint/key_2"]],
            {
                "s3://endpoint/key_1": {"state": "uploaded"},
                "s3://endpoint/key_2": {"state": "validated"},
            },
            {"s3://endpoint": {"key": "nonexistent"}},
            [],
        ),
        (
            [
                ["s3://endpoint/key_1"],
                ["s3://endpoint/key_2", "s3://endpoint/key_3"],
                ["s3://endpoint/key_4"],
            ],
            {
                "s3://endpoint/key_1": {"state": "uploaded", "type": "cleversafe"},
                "s3://endpoint/key_2": {"state": "uploaded", "type": "ceph"},
                "s3://endpoint/key_3": {"state": "validated", "type": "cleversafe"},
                "s3://endpoint/key_4": {"state": "uploaded"},
            },
            {"s3://endpoint": {"state": "uploaded", "type": "cleversafe"}},
            ["s3://endpoint/key_1"],
        ),
        (
            [["s3://endpoint/key"]],
            {"s3://endpoint/key": {"state": "whatever"}},
            {"s3://endpoint": {}},
            ["s3://endpoint/key"],
        ),
    ],
)
def test_urls_metadata_partial_match(
    client, doc_urls, urls_meta, params, expected, user
):
    url_doc_mapping = {}
    for url_group in doc_urls:
        data = get_doc(has_urls_metadata=True)
        data["urls"] = url_group
        data["urls_metadata"] = {}
        for url in url_group:
            data["urls_metadata"][url] = urls_meta[url]

        res = client.post("/index/", json=data, headers=user)
        assert res.status_code == 200
        rec = res.json
        for url in url_group:
            url_doc_mapping[url] = rec

    res = client.get("/index/?urls_metadata=" + json.dumps(params))
    assert res.status_code == 200
    rec = res.json

    ids = {r["did"] for r in rec["records"]}
    assert ids == {url_doc_mapping[url]["did"] for url in expected}


def test_get_urls(client, user):
    data = get_doc(has_urls_metadata=True)
    response = client.post("/index/", json=data, headers=user)
    assert response.status_code == 200
    record = response.json

    response = client.get("/urls/?ids=" + record["did"])
    assert response.status_code == 200
    record = response.json
    url = data["urls"][0]
    assert record["urls"][0]["url"] == url
    assert record["urls"][0]["metadata"] == data["urls_metadata"][url]

    response = client.get("/urls/?size={}".format(data["size"]))
    assert response.status_code == 200
    record = response.json
    url = data["urls"][0]
    assert record["urls"][0]["url"] == url
    assert record["urls"][0]["metadata"] == data["urls_metadata"][url]


def test_get_urls_size_0(client, user):
    data = get_doc(has_urls_metadata=True)
    data["size"] = 0
    response = client.post("/index/", json=data, headers=user)
    assert response.status_code == 200
    record = response.json

    response = client.get("/urls/?size={}".format(data["size"]))
    assert response.status_code == 200
    record = response.json
    url = data["urls"][0]
    assert record["urls"][0]["url"] == url
    assert record["urls"][0]["metadata"] == data["urls_metadata"][url]


def test_index_create(client, user):
    data = get_doc(has_baseid=True)

    res = client.post("/index/", json=data, headers=user)
    assert res.status_code == 200
    rec = res.json
    assert rec["did"]
    assert rec["baseid"] == data["baseid"]
    res = client.get("/index/" + rec["did"])
    assert res.status_code == 200
    rec = res.json
    assert rec["acl"] == []
    assert rec["authz"] == []


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


def test_unauthorized_create(client):
    # test that unauthorized post throws 403 error
    data = get_doc()
    res = client.post("/index/", json=data)
    assert res.status_code == 403


def test_index_get(client, user):
    data = get_doc(has_baseid=True)

    res = client.post("/index/", json=data, headers=user)
    assert res.status_code == 200
    rec = res.json
    res_1 = client.get("/index/" + rec["did"])
    assert res_1.status_code == 200
    rec_1 = res_1.json
    res_2 = client.get("/index/" + rec["baseid"])
    assert res_2.status_code == 200
    rec_2 = res_2.json
    assert rec_1["did"] == rec["did"]
    assert rec_2["did"] == rec["did"]


def test_get_id(client, user):
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


def test_index_prepend_prefix(client, user):
    data = get_doc()

    res_1 = client.post("/index/", json=data, headers=user)
    assert res_1.status_code == 200
    rec_1 = res_1.json
    res_2 = client.get("/index/" + rec_1["did"])
    assert res_2.status_code == 200
    rec_2 = res_2.json

    assert rec_1["did"] == rec_2["did"]
    assert rec_2["did"].startswith("testprefix:")


def test_index_get_with_baseid(client, user):
    data1 = get_doc(has_baseid=True)
    res = client.post("/index/", json=data1, headers=user)
    assert res.status_code == 200

    data2 = get_doc(has_baseid=True)
    res_1 = client.post("/index/", json=data2, headers=user)
    assert res_1.status_code == 200
    rec_1 = res_1.json

    res_2 = client.get("/index/" + data1["baseid"])
    assert res_2.status_code == 200
    rec_2 = res_2.json
    assert rec_2["did"] == rec_1["did"]


def test_delete_and_recreate(client, user):
    """
    Test that you can delete an IndexDocument and be able to
    recreate it with the same fields.
    """

    old_data = get_doc(has_baseid=True)
    new_data = get_doc(has_baseid=True)
    new_data["hashes"] = {"md5": "11111111111111111111111111111111"}

    old_result = client.post("/index/", json=old_data, headers=user)
    assert old_result.status_code == 200
    old_record = old_result.json
    assert old_record["did"]
    assert old_record["baseid"] == old_data["baseid"]

    # create a new doc with the same did
    new_data["did"] = old_record["did"]

    # delete the old doc
    res = client.delete(
        "/index/{}?rev={}".format(old_record["did"], old_record["rev"]),
        json=old_data,
        headers=user,
    )
    assert res.status_code == 200
    # make sure it's deleted
    res = client.get("/index/" + old_record["did"])
    assert res.status_code == 404

    # create new doc with the same baseid and did
    new_result = client.post("/index/", json=new_data, headers=user)
    assert new_result.status_code == 200
    new_record = new_result.json

    assert new_record["did"]
    # verify that they are the same
    assert new_record["baseid"] == new_data["baseid"]
    assert new_record["did"] == old_record["did"]
    assert new_record["baseid"] == old_record["baseid"]

    # verify that new data is in the new node
    new_result = client.get("/index/" + new_record["did"])
    assert new_result.status_code == 200
    new_record = new_result.json
    assert new_data["baseid"] == new_record["baseid"]
    assert new_data["urls"] == new_record["urls"]
    assert new_data["hashes"]["md5"] == new_record["hashes"]["md5"]


def test_index_create_with_multiple_hashes(client, user):
    data = get_doc()
    data["hashes"] = {
        "md5": "8b9942cf415384b27cadf1f4d2d682e5",
        "sha1": "fdbbca63fbec1c2b0d4eb2494ce91520ec9f55f5",
    }

    result = client.post("/index/", json=data, headers=user)
    assert result.status_code == 200
    record = result.json
    assert record["did"]


def test_index_create_with_valid_did(client, user):
    data = get_doc()
    data["did"] = "3d313755-cbb4-4b08-899d-7bbac1f6e67d"

    result = client.post("/index/", json=data, headers=user)
    assert result.status_code == 200
    record = result.json
    assert record["did"] == "3d313755-cbb4-4b08-899d-7bbac1f6e67d"


def test_index_create_with_acl_authz(client, user):
    data = {
        "acl": ["a", "b"],
        "authz": ["x", "y"],
        "form": "object",
        "size": 123,
        "urls": ["s3://endpointurl/bucket/key"],
        "hashes": {"md5": "8b9942cf415384b27cadf1f4d2d682e5"},
    }

    res = client.post("/index/", json=data, headers=user)
    assert res.status_code == 200
    rec = res.json
    result = client.get("/index/" + rec["did"])
    assert result.status_code == 200
    record = result.json
    assert record["acl"] == ["a", "b"]
    assert record["authz"] == ["x", "y"]


def test_index_create_with_duplicate_acl_authz(client, user):
    data = {
        "acl": ["a", "b", "a"],
        "authz": ["x", "y", "x"],
        "form": "object",
        "size": 123,
        "urls": ["s3://endpointurl/bucket/key"],
        "hashes": {"md5": "8b9942cf415384b27cadf1f4d2d682e5"},
    }

    res = client.post("/index/", json=data, headers=user)
    assert res.status_code == 200
    rec = res.json
    result = client.get("/index/" + rec["did"])
    assert result.status_code == 200
    record = result.json
    assert record["acl"] == ["a", "b"]
    assert record["authz"] == ["x", "y"]


def test_index_create_with_invalid_did(client, user):
    data = get_doc()

    data["did"] = "3d313755-cbb4-4b0fdfdfd8-899d-7bbac1f6e67dfdd"
    response = client.post("/index/", json=data, headers=user)
    assert response.status_code == 400


def test_index_create_with_prefix(client, user):
    data = get_doc()
    data["did"] = "cdis:3d313755-cbb4-4b08-899d-7bbac1f6e67d"

    response = client.post("/index/", json=data, headers=user)
    assert response.status_code == 200
    record = response.json
    assert record["did"] == "cdis:3d313755-cbb4-4b08-899d-7bbac1f6e67d"


def test_index_create_with_duplicate_did(client, user):
    data = get_doc()
    data["did"] = "3d313755-cbb4-4b08-899d-7bbac1f6e67d"

    response = client.post("/index/", json=data, headers=user)
    assert response.status_code == 200
    response = client.post("/index/", json=data, headers=user)
    assert response.status_code == 400


def test_index_create_with_file_name(client, user):
    data = get_doc()
    data["file_name"] = "abc"

    res = client.post("/index/", json=data, headers=user)
    assert res.status_code == 200
    rec = res.json
    res = client.get("/index/" + rec["did"])
    assert res.status_code == 200
    rec = res.json
    assert rec["file_name"] == "abc"


def test_index_create_with_version(client, user):
    data = get_doc()
    data["version"] = "ver_123"

    res = client.post("/index/", json=data, headers=user)
    assert res.status_code == 200
    rec = res.json
    res = client.get("/index/" + rec["did"])
    assert res.status_code == 200
    rec = res.json
    assert rec["version"] == data["version"]


def test_create_blank_record_with_baseid(client, user):
    doc = {"uploader": "uploader_123", "baseid": "baseid_123"}

    res = client.post("/index/blank/", json=doc, headers=user)
    assert res.status_code == 201
    rec = res.json
    assert rec["did"]
    res = client.get("/index/?baseid=" + doc["baseid"])
    assert res.status_code == 200
    rec = res.json
    assert_blank(rec)


def test_index_create_with_uploader(client, user):
    data = get_doc()
    data["uploader"] = "uploader_123"
    res = client.post("/index/", json=data, headers=user)
    assert res.status_code == 200
    rec = res.json
    res = client.get("/index/" + rec["did"])
    assert res.status_code == 200
    rec = res.json
    assert rec["uploader"] == data["uploader"]


def test_index_get_global_endpoint(client, user):
    data = get_doc()

    res = client.post("/index/", json=data, headers=user)
    assert res.status_code == 200
    rec = res.json
    res = client.get("/" + rec["did"])
    assert res.status_code == 200
    rec = res.json

    assert rec["metadata"] == data["metadata"]
    assert rec["form"] == "object"
    assert rec["size"] == data["size"]
    assert rec["urls"] == data["urls"]
    assert rec["hashes"]["md5"] == data["hashes"]["md5"]

    res_2 = client.get("/testprefix:" + rec["did"])
    assert res_2.status_code == 200
    rec_2 = res_2.json
    assert rec_2["did"] == rec["did"]


def test_index_update(client, user):
    data = get_doc()

    res = client.post("/index/", json=data, headers=user)
    assert res.status_code == 200
    rec = res.json
    assert rec["did"]
    assert rec["rev"]
    assert client.get("/index/" + rec["did"]).json["metadata"] == data["metadata"]
    dataNew = get_doc()
    del dataNew["hashes"]
    del dataNew["size"]
    del dataNew["form"]
    dataNew["metadata"] = {"test": "abcd"}
    dataNew["version"] = "ver123"
    dataNew["acl"] = ["a", "b"]
    dataNew["authz"] = ["x", "y"]
    res_2 = client.put(
        "/index/{}?rev={}".format(rec["did"], rec["rev"]), json=dataNew, headers=user
    )
    assert res_2.status_code == 200
    rec_2 = res_2.json
    assert rec_2["rev"] != rec["rev"]
    response = client.get("/index/" + rec_2["did"])
    assert response.status_code == 200
    record = response.json
    assert record["metadata"] == dataNew["metadata"]
    assert record["acl"] == dataNew["acl"]
    assert record["authz"] == dataNew["authz"]

    data = get_doc()
    data["did"] = "cdis:3d313755-cbb4-4b08-899d-7bbac1f6e67d"
    res = client.post("/index/", json=data, headers=user)
    assert res.status_code == 200
    rec = res.json
    assert rec["did"]
    assert rec["rev"]
    dataNew = {
        "urls": ["s3://endpointurl/bucket/key"],
        "file_name": "test",
        "version": "ver123",
    }
    res_2 = client.put(
        "/index/{}?rev={}".format(rec["did"], rec["rev"]), json=dataNew, headers=user
    )
    assert res_2.status_code == 200
    rec_2 = res_2.json
    assert rec_2["rev"] != rec["rev"]


def test_index_update_duplicate_acl_authz(client, user):
    data = get_doc()

    res = client.post("/index/", json=data, headers=user)
    assert res.status_code == 200
    rec = res.json
    assert rec["did"]
    assert rec["rev"]
    assert client.get("/index/" + rec["did"]).json["metadata"] == data["metadata"]
    dataNew = get_doc()
    del dataNew["hashes"]
    del dataNew["size"]
    del dataNew["form"]
    dataNew["metadata"] = {"test": "abcd"}
    dataNew["version"] = "ver123"
    dataNew["acl"] = ["c", "d", "c"]
    dataNew["authz"] = ["x", "y", "x"]
    res_2 = client.put(
        "/index/{}?rev={}".format(rec["did"], rec["rev"]), json=dataNew, headers=user
    )
    assert res_2.status_code == 200
    rec_2 = res_2.json
    assert rec_2["rev"] != rec["rev"]
    response = client.get("/index/" + rec["did"])
    assert response.status_code == 200
    record = response.json
    assert record["metadata"] == dataNew["metadata"]
    assert record["acl"] == ["c", "d"]
    assert record["authz"] == ["x", "y"]


def test_update_uploader_field(client, user):
    data = get_doc()
    data["uploader"] = "uploader_123"
    res = client.post("/index/", json=data, headers=user)
    assert res.status_code == 200
    rec = res.json
    assert rec["did"]
    assert rec["rev"]

    res = client.get("/index/" + rec["did"])
    assert res.status_code == 200
    rec = res.json
    assert rec["uploader"] == "uploader_123"

    updated = {"uploader": "new_uploader"}
    res = client.put(
        "/index/{}?rev={}".format(rec["did"], rec["rev"]), json=updated, headers=user
    )
    assert res.status_code == 200

    res = client.get("/index/" + rec["did"])
    assert res.status_code == 200
    rec = res.json
    assert rec["uploader"] == "new_uploader"

    updated = {"uploader": None}
    res = client.put(
        "/index/{}?rev={}".format(rec["did"], rec["rev"]), json=updated, headers=user
    )
    assert res.status_code == 200

    res = client.get("/index/" + rec["did"])
    assert res.status_code == 200
    rec = res.json
    assert rec["uploader"] is None


def test_index_delete(client, user):
    data = get_doc(has_metadata=False, has_baseid=False)

    res = client.post("/index/", json=data, headers=user)
    assert res.status_code == 200
    rec = res.json
    assert rec["did"]
    assert rec["rev"]

    res = client.get("/index/" + rec["did"])
    assert res.status_code == 200
    rec = res.json
    assert rec["did"]

    res = client.delete(
        "/index/{}?rev={}".format(rec["did"], rec["rev"]), json=data, headers=user
    )
    assert res.status_code == 200

    # make sure its deleted
    res = client.get("/index/{}?rev={}".format(rec["did"], rec["rev"]))
    assert res.status_code == 404


def test_create_index_version(client, user):
    data = get_doc(has_metadata=False, has_baseid=False)

    res = client.post("/index/", json=data, headers=user)
    assert res.status_code == 200
    rec = res.json
    assert rec["did"]
    assert rec["rev"]
    assert rec["baseid"]

    dataNew = {
        "did": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        "form": "object",
        "size": 244,
        "urls": ["s3://endpointurl/bucket2/key"],
        "hashes": {"md5": "8b9942cf415384b27cadf1f4d2d981f5"},
        "acl": ["a"],
    }

    res_2 = client.post("/index/" + rec["did"], json=dataNew, headers=user)
    assert res_2.status_code == 200
    rec_2 = res_2.json
    assert rec_2["baseid"] == rec["baseid"]
    assert rec_2["did"] == dataNew["did"]


def test_get_latest_version(client, user):
    data = get_doc(has_metadata=False, has_baseid=False, has_version=True)
    res_1 = client.post("/index/", json=data, headers=user)
    assert res_1.status_code == 200
    rec_1 = res_1.json
    assert rec_1["did"]

    data = get_doc(has_metadata=False, has_baseid=False, has_version=False)
    res_2 = client.post("/index/" + rec_1["did"], json=data, headers=user)
    assert res_2.status_code == 200
    rec_2 = res_2.json

    res_3 = client.get("/index/{}/latest".format(rec_2["did"]))
    assert res_3.status_code == 200
    rec_3 = res_3.json
    assert rec_3["did"] == rec_2["did"]

    res_4 = client.get("/index/{}/latest".format(rec_1["baseid"]))
    assert res_4.status_code == 200
    rec_4 = res_4.json
    assert rec_4["did"] == rec_2["did"]

    res_5 = client.get("/index/{}/latest?has_version=True".format(rec_1["baseid"]))
    assert res_5.status_code == 200
    rec_5 = res_5.json
    assert rec_5["did"] == rec_1["did"]


def test_get_all_versions(client, user):
    data = get_doc(has_metadata=False, has_baseid=False)
    res_1 = client.post("/index/", json=data, headers=user)
    assert res_1.status_code == 200
    rec_1 = res_1.json
    assert rec_1["did"]
    res = client.post("/index/" + rec_1["did"], json=data, headers=user)
    assert res.status_code == 200
    res_2 = client.get("/index/{}/versions".format(rec_1["did"]))
    rec_2 = res_2.json
    assert len(rec_2) == 2
    res_3 = client.get("/index/{}/versions".format(rec_1["baseid"]))
    rec_3 = res_3.json
    assert len(rec_3) == 2


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


@pytest.mark.parametrize(
    "typ,h",
    [
        ("md5", "8b9942cf415384b27cadf1f4d2d682e5"),
        ("etag", "8b9942cf415384b27cadf1f4d2d682e5"),
        ("etag", "8b9942cf415384b27cadf1f4d2d682e5-2311"),
        ("sha1", "1b64db0c5ef4fa349b5e37403c745e7ef4caa350"),
        (
            "sha256",
            "4ff2d1da9e33bb0c45f7b0e5faa1a5f5" + "e6250856090ff808e2c02be13b6b4258",
        ),
        (
            "sha512",
            "65de2c01a38d2d88bd182526305"
            + "56ed443b56fd51474cb7c0930d0b62b608"
            + "a3c7d9e27d53269f9a356a2af9bd4c18d5"
            + "368e66dd9f2412b82e325de3c5a4c21b3",
        ),
        ("crc", "997a6f5c"),
    ],
)
def test_good_hashes(client, user, typ, h):
    data = {
        "form": "object",
        "size": 123,
        "urls": ["s3://endpointurl/bucket/key"],
        "file_name": "abc",
        "version": "ver_123",
        "hashes": {typ: h},
    }

    resp = client.post("/index/", data=json.dumps(data), headers=user)

    assert resp.status_code == 200
    json_resp = resp.json
    assert "error" not in json_resp


@pytest.mark.parametrize(
    "typ,h",
    [
        ("", ""),
        ("blah", "aaa"),
        ("not_supported", "8b9942cf415384b27cadf1f4d2d682e5"),
        ("md5", "not valid"),
        ("crc", "not valid"),
        ("etag", ""),
        ("etag", "8b9942cf415384b27cadf1f4d2d682e5-"),
        ("etag", "8b9942cf415384b27cadf1f4d2d682e5-afffafb"),
        ("sha1", "8b9942cf415384b27cadf1f4d2d682e5"),
        ("sha256", "not valid"),
        ("sha512", "not valid"),
    ],
)
def test_bad_hashes(client, user, typ, h):
    data = {
        "form": "object",
        "size": 123,
        "urls": ["s3://endpointurl/bucket/key"],
        "file_name": "abc",
        "version": "ver_123",
        "hashes": {typ: h},
    }

    resp = client.post("/index/", data=json.dumps(data), headers=user)

    assert resp.status_code == 400
    json_resp = resp.json
    assert "error" in json_resp
    if typ not in ACCEPTABLE_HASHES:
        assert "is not valid" in json_resp["error"]
    else:
        assert "does not match" in json_resp["error"]


def test_dos_get(client, user):
    data = get_doc(has_urls_metadata=True, has_metadata=True, has_baseid=True)

    res_1 = client.post("/index/", json=data, headers=user)
    assert res_1.status_code == 200
    rec_1 = res_1.json
    res_2 = client.get("/ga4gh/dos/v1/dataobjects/" + rec_1["did"])
    assert res_2.status_code == 200
    rec_2 = res_2.json
    assert rec_2["data_object"]["id"] == rec_1["did"]
    assert rec_2["data_object"]["size"] == 123
    assert (
        rec_2["data_object"]["checksums"][0]["checksum"]
        == "8b9942cf415384b27cadf1f4d2d682e5"
    )
    assert rec_2["data_object"]["checksums"][0]["type"] == "md5"
    assert rec_2["data_object"]["urls"][0]["url"] == "s3://endpointurl/bucket/key"
    assert rec_2["data_object"]["urls"][0]["user_metadata"]["state"] == "uploaded"
    assert (
        rec_2["data_object"]["urls"][0]["system_metadata"]["project_id"]
        == "bpa-UChicago"
    )
    res_3 = client.get("/ga4gh/dos/v1/dataobjects/" + rec_1["baseid"])
    assert res_3.status_code == 200
    rec_3 = res_3.json
    assert rec_3["data_object"]["id"] == rec_1["did"]


def test_get_dos_record_error(client, user):
    # test exception raised at nonexistent
    fake_did = "testprefix:d96bab16-c4e1-44ac-923a-04328b6fe78f"
    res = client.get("/ga4gh/dos/v1/dataobjects/" + fake_did)
    assert res.status_code == 404


def test_dos_list(client, user):
    data = get_doc(has_urls_metadata=True, has_metadata=True, has_baseid=True)

    res_1 = client.post("/index/", json=data, headers=user)
    assert res_1.status_code == 200
    rec_1 = res_1.json

    res_2 = client.get("/ga4gh/dos/v1/dataobjects?page_size=100")
    assert res_2.status_code == 200
    rec_2 = res_2.json
    assert len(rec_2["data_objects"]) == 1
    assert rec_2["data_objects"][0]["id"] == rec_1["did"]
    assert rec_2["data_objects"][0]["size"] == 123
    assert (
        rec_2["data_objects"][0]["checksums"][0]["checksum"]
        == "8b9942cf415384b27cadf1f4d2d682e5"
    )
    assert rec_2["data_objects"][0]["checksums"][0]["type"] == "md5"
    assert rec_2["data_objects"][0]["urls"][0]["url"] == "s3://endpointurl/bucket/key"
    assert rec_2["data_objects"][0]["urls"][0]["user_metadata"]["state"] == "uploaded"
    assert (
        rec_2["data_objects"][0]["urls"][0]["system_metadata"]["project_id"]
        == "bpa-UChicago"
    )


def test_update_without_changing_fields(client, user):
    # setup test
    data = get_doc(has_urls_metadata=True, has_metadata=True, has_baseid=True)

    res = client.post("/index/", json=data, headers=user)
    assert res.status_code == 200
    rec = res.json
    first_doc = client.get("/index/" + rec["did"]).json

    # update
    updated = {"version": "at least 2"}
    res = client.put(
        "/index/{}?rev={}".format(first_doc["did"], first_doc["rev"]),
        json=updated,
        headers=user,
    )
    assert res.status_code == 200

    # Check if update successful.
    second_doc = client.get("/index/" + first_doc["did"]).json
    # Only `version` changed.
    assert first_doc["version"] != second_doc["version"]

    # The rest is the same.
    assert first_doc["urls"] == second_doc["urls"]
    assert first_doc["size"] == second_doc["size"]
    assert first_doc["file_name"] == second_doc["file_name"]
    assert first_doc["metadata"] == second_doc["metadata"]

    # Change `version` to null.
    # update
    updated = {"version": None}
    res = client.put(
        "/index/{}?rev={}".format(second_doc["did"], second_doc["rev"]),
        json=updated,
        headers=user,
    )
    assert res.status_code == 200

    # check if update successful
    third_doc = client.get("/index/" + rec["did"]).json
    # Only `version` changed.
    assert second_doc["version"] != third_doc["version"]


def test_bulk_get_documents(client, user):

    # just make a bunch of entries in indexd
    dids = [
        client.post("/index/", json=get_doc(has_baseid=True), headers=user).json["did"]
        for _ in range(20)
    ]
    # do a bulk query for them all
    res = client.post("/bulk/documents", json=dids, headers=user)
    assert res.status_code == 200
    docs = res.json

    # compare that they are the same by did
    for doc in docs:
        assert doc["did"] in dids


def test_status_check(client):
    res = client.get("/_status/")
    assert res.status_code == 200


def test_version_check(client):
    res = client.get("/_version")
    assert res.status_code == 200
