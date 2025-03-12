import json
import random
import uuid

import pytest
from swagger_client.rest import ApiException

from indexd.blueprint import dist_get_record
from indexd.index.blueprint import ACCEPTABLE_HASHES
from tests.integration.util import assert_blank


def get_doc(baseid=None, version=None, has_metadata=True):
    doc = {
        "form": "object",
        "size": 123,
        "urls": ["s3://endpointurl/bucket/key"],
        "hashes": {"md5": "8b9942cf415384b27cadf1f4d2d682e5"},
        "urls_metadata": {"s3://endpointurl/bucket/key": {"state": "uploaded"}},
    }
    if has_metadata:
        doc["metadata"] = {"project_id": "bpa-UChicago"}
    if baseid:
        doc["baseid"] = baseid
    if version:
        doc["version"] = version
    return doc


def test_index_list(swg_index_client):
    r = swg_index_client.list_index_entries()
    assert r.records == []


def test_index_list_with_params(swg_index_client):
    data = get_doc()
    data["urls"] = ["s3://endpointurl/bucket_2/key_2", "s3://anotherurl/bucket_2/key_2"]
    data["urls_metadata"] = {
        "s3://endpointurl/bucket_2/key_2": {"state": "error", "other": "xxx"},
        "s3://anotherurl/bucket_2/key_2": {"state": "error", "other": "xxx"},
    }
    r_1 = swg_index_client.add_index_entry(data)

    data2 = get_doc()
    data2["metadata"] = {"project_id": "other-project", "state": "abc", "other": "xxx"}
    data2["urls"] = ["s3://endpointurl/bucket/key_2", "s3://anotherurl/bucket/key_2"]
    data2["urls_metadata"] = {
        "s3://endpointurl/bucket/key_2": {"state": "error", "other": "xxx"},
        "s3://anotherurl/bucket/key_2": {"state": "error", "other": "xxx"},
    }
    r_2 = swg_index_client.add_index_entry(data2)
    r = swg_index_client.list_index_entries(metadata="project_id:bpa-UChicago")
    ids = [record.did for record in r.records]
    assert r_1.did in ids

    r = swg_index_client.list_index_entries(metadata="project_id:other-project")
    ids = [record.did for record in r.records]
    assert r_2.did in ids

    r = swg_index_client.list_index_entries(hash="md5:8b9942cf415384b27cadf1f4d2d682e5")
    ids = [record.did for record in r.records]
    assert r_1.did in ids
    assert r_2.did in ids

    r = swg_index_client.list_index_entries(ids=",".join(ids))

    ids = [record.did for record in r.records]
    assert r_1.did in ids
    assert r_2.did in ids

    r = swg_index_client.list_index_entries(limit=2)
    assert len(r.records) == 2

    param = {"bucket": {"state": "error", "other": "xxx"}}

    r = swg_index_client.list_index_entries(limit=2, urls_metadata=json.dumps(param))
    assert len(r.records) == 2


def test_index_list_with_params_negate(swg_index_client):
    data = get_doc()
    data["urls_metadata"]["s3://endpointurl/bucket/key"] = {}
    r_1 = swg_index_client.add_index_entry(data)

    data["metadata"] = {"testkey": "test", "project_id": "negate-project"}
    r_2 = swg_index_client.add_index_entry(data)

    data["urls_metadata"] = {
        "s3://endpointurl/bucket_2/key_2": {"state": "error"},
        "s3://endpointurl/bucket_3/key_3": {"state": "error"},
    }
    data["urls"] = sorted(data["urls_metadata"].keys())
    r_3 = swg_index_client.add_index_entry(data)

    data["urls_metadata"] = {
        "s3://endpointurl/bucket_2/key_2": {"no_state": "uploaded"}
    }
    data["urls"] = sorted(data["urls_metadata"].keys())
    r_4 = swg_index_client.add_index_entry(data)

    data["urls_metadata"] = {"s3://anotherurl/bucket/key": {"state": "error"}}
    data["urls"] = sorted(data["urls_metadata"].keys())
    r_5 = swg_index_client.add_index_entry(data)

    negate_params = {"metadata": {"testkey": ""}}
    r = swg_index_client.list_index_entries(negate_params=json.dumps(negate_params))
    ids = {record.did for record in r.records}
    assert {r_1.did} == ids

    negate_params = {"metadata": {"project_id": "bpa-UChicago"}}
    r = swg_index_client.list_index_entries(negate_params=json.dumps(negate_params))
    ids = {record.did for record in r.records}
    assert {r_2.did, r_3.did, r_4.did, r_5.did} == ids

    # negate url
    negate_params = {"urls": ["s3://endpointurl/bucket_2/key_2"]}
    r = swg_index_client.list_index_entries(negate_params=json.dumps(negate_params))
    ids = {record.did for record in r.records}
    assert ids == {r_1.did, r_2.did, r_5.did}

    # negate url key
    negate_params = {"urls_metadata": {"s3://endpointurl/": {}}}
    r = swg_index_client.list_index_entries(negate_params=json.dumps(negate_params))
    ids = {record.did for record in r.records}
    assert ids == {r_5.did}

    negate_params = {"urls_metadata": {"s3://endpointurl/": {}, "s3://anotherurl/": {}}}
    r = swg_index_client.list_index_entries(negate_params=json.dumps(negate_params))
    ids = {record.did for record in r.records}
    assert ids == set()

    # negate url_metadata key
    negate_params = {
        "urls_metadata": {"s3://endpointurl/": {"state": ""}, "s3://anotherurl/": {}}
    }
    r = swg_index_client.list_index_entries(negate_params=json.dumps(negate_params))
    ids = {record.did for record in r.records}
    assert ids == {r_1.did, r_2.did, r_3.did, r_4.did}

    # negate url_metadata value
    negate_params = {"urls_metadata": {"s3://endpointurl/": {"state": "uploaded"}}}
    r = swg_index_client.list_index_entries(negate_params=json.dumps(negate_params))
    ids = {record.did for record in r.records}
    assert ids == {r_1.did, r_2.did, r_3.did, r_4.did, r_5.did}


def test_list_entries_with_uploader(swg_index_client):
    """
    Test that return a list of record given uploader
    """
    data = get_doc()
    data["uploader"] = "uploader_1"
    swg_index_client.add_index_entry(data)

    data = get_doc()
    data["uploader"] = "uploader_123"
    r2 = swg_index_client.add_index_entry(data)

    data = get_doc()
    data["uploader"] = "uploader_123"
    r3 = swg_index_client.add_index_entry(data)

    r = swg_index_client.list_index_entries(uploader="uploader_123")
    assert len(r.records) == 2
    assert {r2.did, r3.did} == {r.records[0].did, r.records[1].did}
    assert r.records[0].uploader == "uploader_123"
    assert r.records[1].uploader == "uploader_123"


def test_list_entries_with_uploader_wrong_uploader(swg_index_client):
    """
    Test that returns no record due to wrong uploader id
    """
    data = get_doc()
    data["uploader"] = "uploader_1"
    swg_index_client.add_index_entry(data)

    data = get_doc()
    data["uploader"] = "uploader_123"
    swg_index_client.add_index_entry(data)

    data = get_doc()
    data["uploader"] = "uploader_123"
    swg_index_client.add_index_entry(data)

    r = swg_index_client.list_index_entries(uploader="wrong_uploader")
    assert len(r.records) == 0


def test_create_blank_record(swg_index_client):
    """
    Test that new blank records only contain the uploader
    and optionally file_name fields: test without file name
    """

    doc = {"uploader": "uploader_123"}
    r = swg_index_client.create_index_blank_entry(doc)
    assert r.did
    assert r.rev
    assert r.baseid

    r = swg_index_client.list_index_entries(uploader="uploader_123")
    assert r.records[0].uploader == "uploader_123"
    assert not r.records[0].file_name
    assert_blank(r)


def test_create_blank_record_with_file_name(swg_index_client):
    """
    Test that new blank records only contain the uploader
    and optionally file_name fields: test with file name
    """

    doc = {"uploader": "uploader_321", "file_name": "myfile.txt"}
    r = swg_index_client.create_index_blank_entry(doc)
    assert r.did
    assert r.rev
    assert r.baseid

    r = swg_index_client.list_index_entries(uploader="uploader_321")
    assert r.records[0].uploader == "uploader_321"
    assert r.records[0].file_name == "myfile.txt"
    assert_blank(r)


def test_fill_size_n_hash_for_blank_record(swg_index_client):
    """
    Test that can fill size and hashes for empty record
    """
    doc = {"uploader": "uploader_123"}

    r = swg_index_client.create_index_blank_entry(doc)
    assert r.did
    assert r.rev

    did, rev = r.did, r.rev
    updated = {
        "size": 10,
        "hashes": {"md5": "8b9942cf415384b27cadf1f4d2d981f5"},
    }

    r = swg_index_client.update_index_blank_entry(guid=did, rev=rev, body=updated)
    assert r.did == did
    assert r.rev != rev

    r = swg_index_client.get_index_entry(did)
    assert r.size == 10
    assert r.hashes.md5 == "8b9942cf415384b27cadf1f4d2d981f5"


def test_get_empty_acl_record(swg_index_client):
    """
    Test that can get a list of empty acl given uploader
    """
    doc = get_doc()
    swg_index_client.add_index_entry(doc)

    doc = {"uploader": "uploader_123"}
    r2 = swg_index_client.create_index_blank_entry(doc)

    doc = {"uploader": "uploader_123"}
    r3 = swg_index_client.create_index_blank_entry(doc)

    r = swg_index_client.list_index_entries()
    assert len(r.records) == 3

    r = swg_index_client.list_index_entries(uploader="uploader_123", acl="null")

    assert len(r.records) == 2
    assert {r2.did, r3.did} == {r.records[0].did, r.records[1].did}
    assert r.records[0].acl == []
    assert r.records[1].acl == []


def test_get_empty_acl_record_after_fill_size_n_hash(swg_index_client):
    """
    Test create blank record -> fill hash and size -> get record with empty or none acl
    """
    # create the first blank record, update size, hashes and acl
    doc = {"uploader": "uploader_123"}
    r1 = swg_index_client.create_index_blank_entry(doc)
    updated = {
        "size": 10,
        "hashes": {"md5": "8b9942cf415384b27cadf1f4d2d981f5"},
    }
    did1 = r1.did
    r1 = swg_index_client.update_index_blank_entry(
        guid=r1.did, rev=r1.rev, body=updated
    )
    r1 = swg_index_client.update_index_entry(
        guid=r1.did, rev=r1.rev, body={"acl": ["read"]}
    )
    r1 = swg_index_client.get_index_entry(r1.did)
    assert r1.acl == ["read"]
    assert r1.did == did1

    # create the second blank record, only update size hashes and urls
    doc = {"uploader": "uploader_123"}
    r2 = swg_index_client.create_index_blank_entry(doc)
    did2 = r2.did

    # create the second blank record, only update size hashes and urls
    doc = {"uploader": "uploader_123"}
    r3 = swg_index_client.create_index_blank_entry(doc)
    did3 = r3.did
    updated = {
        "size": 4,
        "hashes": {"md5": "1b9942cf415384b27cadf1f4d2d981f5"},
        "urls": ["s3://example/2"],
        "urls_metadata": {"s3://example/2": {}},
    }
    swg_index_client.update_index_blank_entry(guid=r3.did, rev=r3.rev, body=updated)

    r = swg_index_client.list_index_entries(uploader="uploader_123")
    assert len(r.records) == 3

    r = swg_index_client.list_index_entries(uploader="uploader_123", acl="read")
    assert len(r.records) == 1
    assert r.records[0].did == r1.did

    r = swg_index_client.list_index_entries(uploader="uploader_123", acl="write")
    assert len(r.records) == 0

    r = swg_index_client.list_index_entries(uploader="uploader_123", acl="null")
    print(r.records)
    assert len(r.records) == 2
    assert {r.records[0].did, r.records[1].did} == {did2, did3}


def test_urls_metadata(swg_index_client):
    data = get_doc()
    result = swg_index_client.add_index_entry(data)

    doc = swg_index_client.get_index_entry(result.did)
    assert doc.urls_metadata == data["urls_metadata"]

    updated = {"urls_metadata": {data["urls"][0]: {"test": "b"}}}
    swg_index_client.update_index_entry(guid=doc.did, rev=doc.rev, body=updated)

    doc = swg_index_client.get_index_entry(result.did)
    assert doc.urls_metadata == updated["urls_metadata"]


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
    swg_index_client, doc_urls, urls_meta, params, expected
):
    url_doc_mapping = {}
    for url_group in doc_urls:
        data = get_doc()
        data["urls"] = url_group
        data["urls_metadata"] = {}
        for url in url_group:
            data["urls_metadata"][url] = urls_meta[url]

        record = swg_index_client.add_index_entry(data)
        for url in url_group:
            url_doc_mapping[url] = record

    docs = swg_index_client.list_index_entries(urls_metadata=json.dumps(params))

    ids = {r.did for r in docs.records}
    assert ids == {url_doc_mapping[url].did for url in expected}


def test_get_urls(swg_index_client, swg_global_client):
    data = get_doc()
    result = swg_index_client.add_index_entry(data)

    result = swg_global_client.list_urls(ids=result.did)
    url = data["urls"][0]
    assert result.urls[0].url == url
    assert result.urls[0].metadata == data["urls_metadata"][url]


def test_index_create(swg_index_client):
    data = get_doc(baseid=str(uuid.uuid4()))

    result = swg_index_client.add_index_entry(data)
    assert result.did
    assert result.baseid == data["baseid"]
    r = swg_index_client.get_index_entry(result.did)
    assert r.acl == []


def test_index_get(swg_index_client):
    data = get_doc(baseid=str(uuid.uuid4()))

    result = swg_index_client.add_index_entry(data)
    r = swg_index_client.get_index_entry(result.did)
    r2 = swg_index_client.get_index_entry(result.baseid)
    assert r.did == result.did
    assert r2.did == result.did


def test_dist_get_record(swg_index_client):
    data = get_doc(baseid=str(uuid.uuid4()))

    result = swg_index_client.add_index_entry(data)
    r = dist_get_record(result.did)
    assert r["did"] == result.did


def test_index_prepend_prefix(swg_index_client):
    data = get_doc()

    result = swg_index_client.add_index_entry(data)
    r = swg_index_client.get_index_entry(result.did)
    assert r.did == result.did
    assert r.did.startswith("testprefix:")


def test_index_get_with_baseid(swg_index_client):
    data1 = get_doc(baseid=str(uuid.uuid4()))
    swg_index_client.add_index_entry(data1)

    data2 = get_doc(baseid=data1["baseid"])
    r2 = swg_index_client.add_index_entry(data2)

    r = swg_index_client.get_index_entry(data1["baseid"])
    assert r.did == r2.did


def test_delete_and_recreate(swg_index_client):
    """
    Test that you can delete an IndexDocument and be able to
    recreate it with the same fields.
    """

    old_data = get_doc(baseid=str(uuid.uuid4()))
    new_data = get_doc(old_data["baseid"])
    new_data["hashes"] = {"md5": "11111111111111111111111111111111"}

    old_result = swg_index_client.add_index_entry(old_data)
    assert old_result.did
    assert old_result.baseid == old_data["baseid"]

    # create a new doc with the same did
    new_data["did"] = old_result.did

    # delete the old doc
    swg_index_client.delete_index_entry(old_result.did, old_result.rev)
    with pytest.raises(ApiException):
        # make sure it's deleted
        swg_index_client.get_index_entry(old_result.did)

    # create new doc with the same baseid and did
    new_result = swg_index_client.add_index_entry(new_data)

    assert new_result.did
    # verify that they are the same
    assert new_result.baseid == new_data["baseid"]
    assert new_result.did == old_result.did
    assert new_result.baseid == old_result.baseid

    # verify that new data is in the new node
    new_doc = swg_index_client.get_index_entry(new_result.did)
    assert new_data["baseid"] == new_doc.baseid
    assert new_data["urls"] == new_doc.urls
    assert new_data["hashes"]["md5"] == new_doc.hashes.md5


def test_index_create_with_multiple_hashes(swg_index_client):
    data = get_doc()
    data["hashes"] = {
        "md5": "8b9942cf415384b27cadf1f4d2d682e5",
        "sha1": "fdbbca63fbec1c2b0d4eb2494ce91520ec9f55f5",
    }

    result = swg_index_client.add_index_entry(data)
    assert result.did


def test_index_create_with_valid_did(swg_index_client):
    data = get_doc()
    data["did"] = "3d313755-cbb4-4b08-899d-7bbac1f6e67d"

    result = swg_index_client.add_index_entry(data)
    assert result.did == "3d313755-cbb4-4b08-899d-7bbac1f6e67d"


def test_index_create_with_acl(swg_index_client):
    data = {
        "acl": ["a", "b"],
        "form": "object",
        "size": 123,
        "urls": ["s3://endpointurl/bucket/key"],
        "hashes": {"md5": "8b9942cf415384b27cadf1f4d2d682e5"},
        "urls_metadata": {
            "s3://endpointurl/bucket/key": {"state": "uploaded"},
        },
    }

    r = swg_index_client.add_index_entry(data)
    result = swg_index_client.get_index_entry(r.did)
    assert result.acl == ["a", "b"]


def test_index_create_with_invalid_did(swg_index_client):
    data = get_doc()

    data["did"] = "3d313755-cbb4-4b0fdfdfd8-899d-7bbac1f6e67dfdd"

    with pytest.raises(ApiException) as e:
        swg_index_client.add_index_entry(data)
        assert e.status == 400


def test_index_create_with_prefix(swg_index_client):
    data = get_doc()
    data["did"] = "cdis:3d313755-cbb4-4b08-899d-7bbac1f6e67d"

    r = swg_index_client.add_index_entry(data)
    assert r.did == "cdis:3d313755-cbb4-4b08-899d-7bbac1f6e67d"


def test_index_create_with_duplicate_did(swg_index_client):
    data = get_doc()
    data["did"] = "3d313755-cbb4-4b08-899d-7bbac1f6e67d"

    swg_index_client.add_index_entry(data)

    with pytest.raises(ApiException) as e:
        swg_index_client.add_index_entry(data)
        assert e.status == 400


def test_index_create_with_file_name(swg_index_client):
    data = get_doc()
    data["file_name"] = "abc"

    r = swg_index_client.add_index_entry(data)
    r = swg_index_client.get_index_entry(r.did)
    assert r.file_name == "abc"


def test_index_create_with_version(swg_index_client):
    data = get_doc()
    data["version"] = "ver_123"

    r = swg_index_client.add_index_entry(data)
    r = swg_index_client.get_index_entry(r.did)
    assert r.version == data["version"]


def test_index_create_blank_record(swg_index_client):
    doc = {"uploader": "uploader_123", "baseid": "baseid_123"}

    r = swg_index_client.create_index_blank_entry(doc)
    assert r.did
    res = swg_index_client.get_index_entry(r.did)
    assert res.acl == []
    assert res.urls_metadata == {}
    assert res.size is None
    assert res.version is None
    assert res.urls_metadata == {}


def test_index_create_with_uploader(swg_index_client):
    data = get_doc()
    data["uploader"] = "uploader_123"
    r = swg_index_client.add_index_entry(data)
    r = swg_index_client.get_index_entry(r.did)
    assert r.uploader == data["uploader"]


def test_index_get_global_endpoint(swg_global_client, swg_index_client):
    data = get_doc()

    r = swg_index_client.add_index_entry(data)
    r = swg_global_client.get_entry(r.did)

    assert r.metadata == data["metadata"]
    assert r.form == "object"
    assert r.size == data["size"]
    assert r.urls == data["urls"]
    assert r.hashes.md5 == data["hashes"]["md5"]

    r2 = swg_global_client.get_entry("testprefix:" + r.did)
    assert r2.did == r.did


def test_index_update(swg_index_client):
    data = get_doc()

    r = swg_index_client.add_index_entry(data)
    assert r.did
    assert r.rev
    assert swg_index_client.get_index_entry(r.did).metadata == data["metadata"]
    data_new = get_doc()
    data_new["hashes"]["md5"] = "8b9942cf415384b27cadf1f4d2d682e4"
    data_new["size"] = 321
    del data_new["form"]
    data_new["metadata"] = {"test": "abcd"}
    data_new["version"] = "ver123"
    data_new["acl"] = ["a", "b"]
    r2 = swg_index_client.update_index_entry(guid=r.did, rev=r.rev, body=data_new)
    assert r2.rev != r.rev
    result = swg_index_client.get_index_entry(r.did)
    assert result.metadata == data_new["metadata"]
    assert result.acl == data_new["acl"]
    assert result.hashes.md5 == data_new["hashes"]["md5"]
    assert result.size == 321

    data = get_doc()
    data["did"] = "cdis:3d313755-cbb4-4b08-899d-7bbac1f6e67d"
    r = swg_index_client.add_index_entry(data)
    assert r.did
    assert r.rev
    data_new = {
        "urls": ["s3://endpointurl/bucket/key"],
        "file_name": "test",
        "version": "ver123",
    }
    r2 = swg_index_client.update_index_entry(guid=r.did, rev=r.rev, body=data_new)
    assert r2.rev != r.rev


def test_update_uploader_field(swg_index_client):
    data = get_doc()
    data["uploader"] = "uploader_123"
    r = swg_index_client.add_index_entry(data)
    assert r.did
    assert r.rev

    r = swg_index_client.get_index_entry(r.did)
    assert r.uploader == "uploader_123"

    updated = {"uploader": "new_uploader"}
    swg_index_client.update_index_entry(guid=r.did, rev=r.rev, body=updated)

    r = swg_index_client.get_index_entry(r.did)
    assert r.uploader == "new_uploader"

    updated = {"uploader": None}
    swg_index_client.update_index_entry(guid=r.did, rev=r.rev, body=updated)

    r = swg_index_client.get_index_entry(r.did)
    assert r.uploader is None


def test_index_delete(swg_index_client):
    data = get_doc(has_metadata=False)

    r = swg_index_client.add_index_entry(data)
    assert r.did
    assert r.rev

    r = swg_index_client.get_index_entry(r.did)
    assert r.did

    swg_index_client.delete_index_entry(r.did, rev=r.rev)

    with pytest.raises(ApiException) as e:
        r = swg_index_client.get_index_entry(r.did)
        assert e.status == 400


def test_create_index_version(swg_index_client):
    data = get_doc(has_metadata=False)

    r = swg_index_client.add_index_entry(data)
    assert r.did
    assert r.rev
    assert r.baseid

    data_new = {
        "did": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        "form": "object",
        "size": 244,
        "urls": ["s3://endpointurl/bucket2/key"],
        "hashes": {"md5": "8b9942cf415384b27cadf1f4d2d981f5"},
        "acl": ["a"],
        "urls_metadata": {
            "s3://endpointurl/bucket2/key": {"state": "uploaded"},
        },
    }

    r2 = swg_index_client.add_index_new_version(guid=r.did, body=data_new)
    assert r2.baseid == r.baseid
    assert r2.did == data_new["did"]


def test_get_latest_version(swg_index_client):
    data = get_doc(has_metadata=False, version="1")
    r = swg_index_client.add_index_entry(data)
    assert r.did

    data = get_doc(has_metadata=False)
    r2 = swg_index_client.add_index_new_version(guid=r.did, body=data)
    r3 = swg_index_client.get_index_latest_version(r.did)
    assert r3.did == r2.did

    r4 = swg_index_client.get_index_latest_version(r.baseid)
    assert r4.did == r2.did

    r5 = swg_index_client.get_index_latest_version(r.baseid, has_version=True)
    assert r5.did == r.did

    # deleted documents are included by default, but they can be excluded if exclude_deleted flag is set to true
    data = get_doc(has_metadata=False)
    data["metadata"] = {"deleted": "True"}
    r6 = swg_index_client.add_index_new_version(guid=r.did, body=data)
    r7 = swg_index_client.get_index_latest_version(r.baseid, exclude_deleted=True)
    assert r7.did == r2.did
    r8 = swg_index_client.get_index_latest_version(r.baseid)
    assert r8.did == r6.did


def test_get_all_versions(swg_index_client):
    data = get_doc(has_metadata=False)
    r = swg_index_client.add_index_entry(data)
    assert r.did
    swg_index_client.add_index_new_version(guid=r.did, body=data)
    r3 = swg_index_client.get_index_all_versions(r.did)
    assert len(r3) == 2
    r4 = swg_index_client.get_index_all_versions(r.baseid)
    assert len(r4) == 2

    # deleted documents are included by default, but they can be excluded if exclude_deleted flag is set to true
    data = get_doc(has_metadata=False)
    data["metadata"] = {"deleted": "True"}
    swg_index_client.add_index_new_version(guid=r.did, body=data)
    r5 = swg_index_client.get_index_all_versions(r.did, exclude_deleted=True)
    assert len(r5) == 2
    r6 = swg_index_client.get_index_all_versions(r.did)
    assert len(r6) == 3


def test_alias_list(swg_alias_client):
    assert swg_alias_client.list_alias_entries().aliases == []


def test_alias_create(swg_alias_client):
    data = {
        "size": 123,
        "hashes": {"md5": "8b9942cf415384b27cadf1f4d2d682e5"},
        "release": "private",
        "keeper_authority": "CRI",
        "host_authorities": ["PDC"],
    }
    ark = "ark:/31807/TEST-abc"
    r = swg_alias_client.upsert_alias_entry(aliasstring=ark, body=data)
    assert r.name == ark

    assert len(swg_alias_client.list_alias_entries().aliases) == 1
    assert swg_alias_client.get_alias_entry(r.name).name


def test_alias_get_global_endpoint(swg_alias_client, swg_global_client):
    data = {
        "size": 123,
        "hashes": {"md5": "8b9942cf415384b27cadf1f4d2d682e5"},
        "release": "private",
        "keeper_authority": "CRI",
        "host_authorities": ["PDC"],
    }
    ark = "ark:/31807/TEST-abc"

    swg_alias_client.upsert_alias_entry(aliasstring=ark, body=data)

    assert swg_global_client.get_entry(ark).size == 123


def test_alias_update(swg_alias_client):
    data = {
        "size": 123,
        "hashes": {"md5": "8b9942cf415384b27cadf1f4d2d682e5"},
        "release": "private",
        "keeper_authority": "CRI",
        "host_authorities": ["PDC"],
    }
    ark = "ark:/31807/TEST-abc"

    r = swg_alias_client.upsert_alias_entry(aliasstring=ark, body=data)
    assert r.rev

    data_new = {
        "size": 456,
        "hashes": {"md5": "8b9942cf415384b27cadf1f4d2d682e5"},
        "release": "private",
        "keeper_authority": "CRI",
        "host_authorities": ["PDC"],
    }
    r2 = swg_alias_client.upsert_alias_entry(aliasstring=ark, rev=r.rev, body=data_new)
    assert r2.rev != r.rev


def test_alias_delete(swg_alias_client):
    data = {
        "size": 123,
        "hashes": {"md5": "8b9942cf415384b27cadf1f4d2d682e5"},
        "release": "private",
        "keeper_authority": "CRI",
        "host_authorities": ["PDC"],
    }
    ark = "ark:/31807/TEST-abc"

    r = swg_alias_client.upsert_alias_entry(aliasstring=ark, body=data)
    assert r.rev

    swg_alias_client.delete_alias_entry(ark, rev=r.rev)

    assert len(swg_alias_client.list_alias_entries().aliases) == 0


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
        "urls_metadata": {
            "s3://endpointurl/bucket/key": {"state": "uploaded"},
        },
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
        "urls_metadata": {
            "s3://endpointurl/bucket/key": {"state": "uploaded"},
        },
    }

    resp = client.post("/index/", data=json.dumps(data), headers=user)

    assert resp.status_code == 400
    json_resp = resp.json
    assert "error" in json_resp
    if typ not in ACCEPTABLE_HASHES:
        assert "Failed validating" in json_resp["error"]
    else:
        assert "does not match" in json_resp["error"]


def test_update_without_changing_fields(swg_index_client):
    # setup test
    data = get_doc(has_metadata=True, baseid=str(uuid.uuid4()))

    result = swg_index_client.add_index_entry(data)
    first_doc = swg_index_client.get_index_entry(result.did)

    # update
    updated = {"version": "at least 2"}
    swg_index_client.update_index_entry(
        guid=first_doc.did, rev=first_doc.rev, body=updated
    )

    # Check if update successful.
    second_doc = swg_index_client.get_index_entry(first_doc.did)
    # Only `version` changed.
    assert first_doc.version != second_doc.version

    # The rest is the same.
    assert first_doc.urls == second_doc.urls
    assert first_doc.size == second_doc.size
    assert first_doc.file_name == second_doc.file_name
    assert first_doc.metadata == second_doc.metadata

    # Change `version` to null.
    # update
    updated = {"version": None}
    swg_index_client.update_index_entry(
        guid=second_doc.did, rev=second_doc.rev, body=updated
    )

    # check if update successful
    third_doc = swg_index_client.get_index_entry(result.did)
    # Only `version` changed.
    assert second_doc.version != third_doc.version


def test_bulk_get_documents(swg_index_client, swg_bulk_client):
    # just make a bunch of entries in indexd
    dids = [
        swg_index_client.add_index_entry(get_doc(baseid=str(uuid.uuid4()))).did
        for _ in range(20)
    ]

    # do a bulk query for them all
    docs = swg_bulk_client.get_bulk_ids(dids)

    # compare that they are the same by did
    for doc in docs:
        assert doc["did"] in dids


@pytest.mark.parametrize("target_flag", ["skip_null", "exclude_deleted"])
@pytest.mark.parametrize("add_target_criteria", [True, False])
@pytest.mark.parametrize("target_flag_value", [True, False])
def test_bulk_get_latest_version(
    swg_index_client,
    swg_bulk_client,
    target_flag,
    add_target_criteria,
    target_flag_value,
):
    """
    Tests the bulk_get_latest_version targeting either the skip_null or exclude_deleted flag
    Args:
        target_flag (str): defines which flag to target for testing, skip_null or exclude_deleted
        add_target_criteria (boolean): add documentation details that will be ignored if target flag is set to true
        target_flag_value (boolean): set target flag value to True if True, False otherwise

    Setup:
    1. Create N docs in indexd that has version 1.
    2. Choose random N/3 docs, add version 2.
    3. If add_target_criteria: choose random N/3 docs and add version with target criteria

    NOTE: target version will always be the latest version.

    If target_flag is skip_null and add_target_criteria is True:
    1. N/3 to 2N/3 docs have version [1]
    2. 0 to N/3 docs have version [1,2]
    3. N/3 docs have version [1,2,None] or [1, None]

    If target_flag is exclude_deleted and add_target_criteria is True:
    1. N/3 to 2N/3 docs have version [1]
    2. 0 to N/3 docs have version [1,2]
    3. N/3 docs have version [1,2,X] or [1,X] and be flagged as deleted
    """

    assert target_flag in ["skip_null", "exclude_deleted"]

    total_files = 15
    # just make a bunch of entries in indexd
    dids = [
        swg_index_client.add_index_entry(
            get_doc(baseid=str(uuid.uuid4()), version="1")
        ).did
        for _ in range(total_files)
    ]

    # create new non-target versions for random 1/3 dids
    chosen = random.sample(dids, k=total_files // 3)
    latest_dids_excluding_target = []
    for did in dids:
        if did in chosen:
            latest_dids_excluding_target.append(
                swg_index_client.add_index_new_version(
                    guid=did, body=get_doc(version="2")
                ).did
            )
        else:
            latest_dids_excluding_target.append(did)
    assert len(latest_dids_excluding_target) == len(dids)

    # create new target version for random 1/3 dids
    latest_dids = []
    if add_target_criteria:
        chosen = random.sample(dids, k=total_files // 3)
        for did in latest_dids_excluding_target:
            if did in chosen:
                if target_flag == "skip_null":
                    latest_dids.append(
                        swg_index_client.add_index_new_version(
                            guid=did, body=get_doc()
                        ).did
                    )
                elif target_flag == "exclude_deleted":
                    data = get_doc(version="X", has_metadata=False)
                    data["metadata"] = {"deleted": "True"}
                    new_doc = swg_index_client.add_index_new_version(
                        guid=did, body=data
                    )
                    latest_dids.append(new_doc.did)
            else:
                latest_dids.append(did)
        assert len(latest_dids) == len(dids)

    # do a bulk query to get all latest version
    if target_flag == "skip_null":
        docs = swg_bulk_client.get_bulk_latest(dids, skip_null=target_flag_value)
    elif target_flag == "exclude_deleted":
        docs = swg_bulk_client.get_bulk_latest(dids, exclude_deleted=target_flag_value)

    if add_target_criteria and not target_flag_value:
        assert set(latest_dids) == {doc["did"] for doc in docs}
    else:
        assert set(latest_dids_excluding_target) == {doc["did"] for doc in docs}


def test_special_case_metadata_get_and_set(swg_index_client):
    """
    doc[metadata][release_number] is a special case in indexd. This has been
    copied to the main section of the indexd blob. The value is kept in the old
    location for backward compatibility reasons.

    Remove that part of the test if we remove the old location.
    """
    # release_number only, no extra metadata
    blob = get_doc(has_metadata=False)
    blob["metadata"] = {"release_number": "12.0"}
    did = swg_index_client.add_index_entry(blob).did

    doc = swg_index_client.get_index_entry(did).to_dict()
    assert "release_number" in doc["metadata"]
    assert doc["metadata"]["release_number"] == "12.0"

    # Extra metadata with release_number
    blob = get_doc(has_metadata=True)
    blob["metadata"]["release_number"] = "12.0"
    did = swg_index_client.add_index_entry(blob).did

    doc = swg_index_client.get_index_entry(did).to_dict()
    assert "release_number" in doc["metadata"]
    assert doc["metadata"]["release_number"] == "12.0"


def test_special_case_metadata_get_latest(swg_index_client):
    # release_number only, no extra metadata

    blob = get_doc(has_metadata=False)
    blob["metadata"] = {"release_number": "12.0"}
    did = swg_index_client.add_index_entry(blob).did
    blob["did"] = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    blob["metadata"] = {"release_number": "13.0"}
    new_did = swg_index_client.add_index_new_version(guid=did, body=blob).did

    doc = swg_index_client.get_index_latest_version(new_did).to_dict()
    assert "release_number" in doc["metadata"]
    assert doc["metadata"]["release_number"] == "13.0"

    # Extra metadata with release_number
    blob = get_doc(has_metadata=True)
    blob["metadata"]["release_number"] = "12.0"
    did = swg_index_client.add_index_entry(blob).did
    blob["did"] = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
    blob["metadata"]["release_number"] = "13.0"
    new_did = swg_index_client.add_index_new_version(guid=did, body=blob).did

    doc = swg_index_client.get_index_latest_version(new_did).to_dict()
    assert "release_number" in doc["metadata"]
    assert doc["metadata"]["release_number"] == "13.0"
