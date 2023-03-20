import json
import pytest
import uuid
import tests.conftest
import requests
import responses
from tests.default_test_settings import settings


def get_bundle_doc(bundles, bundle_id=None):
    doc = {
        "name": "test_bundle",
        "bundles": bundles,
    }

    if not bundle_id:
        bundle_id = uuid.uuid4()
    doc["bundle_id"] = bundle_id
    """
    add options to do bundles and objects
    """
    return doc


def get_index_doc(has_version=True, urls=list(), add_bundle=False):
    doc = {
        "form": "object",
        "size": 123,
        "urls": ["s3://endpointurl/bucket/key"],
        "hashes": {"md5": "8b9942cf415384b27cadf1f4d2d682e5"},
    }
    if has_version:
        doc["version"] = "1"
    if urls:
        doc["urls"] = urls

    return doc


def create_index(client, user, add_bundle=False):
    i_data = get_index_doc(add_bundle=add_bundle)
    res1 = client.post("/index/", json=i_data, headers=user)
    assert res1.status_code == 200
    rec1 = res1.json
    did_list = [rec1["did"]]

    return did_list, rec1


def test_bundle_post(client, user):
    """
    Bundle 1
        +-object1
    """
    did_list, _ = create_index(client, user)

    data = get_bundle_doc(bundles=did_list)
    res2 = client.post("/bundle/", json=data, headers=user)
    assert res2.status_code == 200


def test_bundle_get_post_with_optional_fields(client, user):
    """
    Bundle 1
        +-object1

    Bundel 2
        +-Bundle 1
            +-object1
        +-object1
    """
    did_list, _ = create_index(client, user)

    data = get_bundle_doc(bundles=did_list)
    data["description"] = "This is a cool bundle."
    data["version"] = "v13cde"
    data["aliases"] = ["123", "456"]

    res2 = client.post("/bundle/", json=data, headers=user)
    rec2 = res2.json
    did = rec2["bundle_id"]
    assert res2.status_code == 200

    res3 = client.get("/ga4gh/drs/v1/objects/" + did)
    rec3 = res3.json
    assert res3.status_code == 200
    assert rec3["description"] == data["description"]
    assert rec3["version"] == data["version"]
    assert rec3["aliases"] == data["aliases"]

    res4 = client.get("/bundle/" + did)
    rec4 = res4.json
    assert res4.status_code == 200
    assert rec4["description"] == data["description"]
    assert rec4["version"] == data["version"]
    assert rec4["aliases"] == data["aliases"]

    # Nested bundle shouldn't contain optional fields
    data2 = get_bundle_doc(bundles=[did, did_list[0]])
    res5 = client.post("/bundle/", json=data2, headers=user)
    did2 = res5.json["bundle_id"]
    assert res5.status_code == 200
    res6 = client.get("/bundle/" + did2 + "?expand=true")
    rec6 = res6.json
    contents = rec6["contents"]
    for content in contents:
        assert "description" not in content
        assert "version" not in content
        assert "aliases" not in content


def test_bundle_post_self_reference(client, user):
    """
    Make sure this doesnt exist
    Bundle 1
        Object 1
        Bundle 1
        .
        .
    """
    did_list, _ = create_index(client, user)
    bundle_id = str(uuid.uuid4)

    did_list.append(bundle_id)
    data = get_bundle_doc(bundles=did_list, bundle_id=bundle_id)
    res2 = client.post("/bundle/", json=data, headers=user)
    assert res2.status_code == 400


def test_bundle_post_defined_size_checksum(client, user):
    did_list, _ = create_index(client, user)
    bundle_id = str(uuid.uuid4)
    data = {
        "name": "test_bundle",
        "bundles": did_list,
        "bundle_id": bundle_id,
        "checksums": [{"checksum": "1bab24e003ac48840123e5bbe72a5ec9", "type": "md5"}],
        "size": 12345,
    }
    res2 = client.post("/bundle/", json=data, headers=user)
    assert res2.status_code == 200


def test_bundle_post_different_checksum_types(client, user):
    did_list, _ = create_index(client, user)
    bundle_id = str(uuid.uuid4)
    data = {
        "name": "test_bundle",
        "bundles": did_list,
        "bundle_id": bundle_id,
        "checksums": [
            {"checksum": "85136c79cbf9fe36bb9d05d0639c70c265c18d37", "type": "sha1"}
        ],
    }
    res = client.post("/bundle/", json=data, headers=user)
    assert res.status_code == 200
    res1 = client.get("/ga4gh/drs/v1/objects/" + bundle_id)
    rec1 = res1.json
    assert rec1["checksums"][0] == {
        "checksum": "85136c79cbf9fe36bb9d05d0639c70c265c18d37",
        "type": "sha1",
    }


def test_bundle_post_multiple_checksum_types(client, user):
    did_list, _ = create_index(client, user)
    bundle_id = str(uuid.uuid4)
    data = {
        "name": "test_bundle",
        "bundles": did_list,
        "bundle_id": bundle_id,
        "checksums": [
            {
                "checksum": "bc52d6bfe3ac965e069109dbd7d15e0ccaaa55678f6e2a6664bee2edf8ae1b2b",
                "type": "sha256",
            },
            {"checksum": "e93ccf5ffc90eefcc0bdb81f87d25d1a", "type": "md5"},
        ],
    }
    res = client.post("/bundle/", json=data, headers=user)
    assert res.status_code == 200

    res = client.get("/ga4gh/drs/v1/objects/" + bundle_id)
    rec = res.json
    checksums = rec["checksums"]
    for checksum in checksums:
        assert checksum in [
            {
                "checksum": "bc52d6bfe3ac965e069109dbd7d15e0ccaaa55678f6e2a6664bee2edf8ae1b2b",
                "type": "sha256",
            },
            {"checksum": "e93ccf5ffc90eefcc0bdb81f87d25d1a", "type": "md5"},
        ]


def test_bundle_post_checksum_with_incorrect_schema(client, user):
    did_list, _ = create_index(client, user)
    bundle_id = str(uuid.uuid4)

    # unknown checksum type
    data = {
        "name": "test_bundle",
        "bundles": did_list,
        "bundle_id": bundle_id,
        "checksums": [
            {"type": "md42", "checksum": "a"},
        ],
    }
    res = client.post("/bundle/", json=data, headers=user)
    assert res.status_code == 400

    # checksum value doesn't match checksum type
    data = {
        "checksums": [
            {"type": "md5", "checksum": "a"},
        ],
    }
    res = client.post("/bundle/", json=data, headers=user)
    assert res.status_code == 400

    res = client.get("/ga4gh/drs/v1/objects/" + bundle_id)
    assert res.status_code == 404


def test_bundle_bundle_data_not_found(client, user):
    bundle_id = str(uuid.uuid4)
    data = {
        "name": "test_bundle",
        "bundles": ["1987hgd09183hd0981hjd0h08ashjd80"],
        "bundle_id": bundle_id,
        "checksums": [{"checksum": "1bab24e003ac48840123e5bbe72a5ec9", "type": "md5"}],
        "size": 12345,
    }
    res2 = client.post("/bundle/", json=data, headers=user)
    assert res2.status_code == 404


def test_post_drs_no_duplicate_bundles(client, user):
    did_list, _ = create_index(client, user)

    data = get_bundle_doc(bundles=[did_list[0], did_list[0], did_list[0]])
    res2 = client.post("/bundle/", json=data, headers=user)
    assert res2.status_code == 400


def test_bundle_post_invalid_input(client, user):
    data = {}
    res2 = client.post("/bundle/", json=data, headers=user)
    assert res2.status_code == 400


def test_bundle_post_no_bundle_data(client, user):
    data = {
        "name": "test_bundle",
        "bundles": [],
    }
    res2 = client.post("/bundle/", json=data, headers=user)
    assert res2.status_code == 400
    assert res2.json["error"] == "Bundle data required."


def test_bundle_get(client, user):
    """
    Post with bundle_id and get.
    Bundle1
        +-object1
    """
    did_list, rec = create_index(client, user)
    res1 = client.get("/ga4gh/drs/v1/objects/" + rec["did"])
    rec1 = res1.json
    bundle_id = str(uuid.uuid4())
    data = get_bundle_doc(did_list, bundle_id=bundle_id)

    res1 = client.post("/bundle/", json=data, headers=user)
    assert res1.status_code == 200

    res2 = client.get("/bundle/" + bundle_id)
    assert res2.status_code == 200
    rec2 = res2.json

    assert rec2["id"] == bundle_id
    assert rec2["name"] == data["name"]
    assert rec2["created_time"]
    assert rec2["updated_time"]
    assert rec2["checksums"]
    assert rec2["size"] == 123


def test_bundle_get_form_type(client, user):
    """
    form = object when object
    form = bundle when bundle
    """
    did_list, rec = create_index(client, user)
    res1 = client.get("/ga4gh/drs/v1/objects/" + rec["did"])
    rec1 = res1.json
    rec1["form"] = "object"
    bundle_id = str(uuid.uuid4())
    data = get_bundle_doc(did_list, bundle_id=bundle_id)

    res1 = client.post("/bundle/", json=data, headers=user)
    assert res1.status_code == 200

    res2 = client.get("/ga4gh/drs/v1/objects/" + bundle_id)
    assert res2.status_code == 200

    rec2 = res2.json
    assert rec2["form"] == "bundle"


def test_bundle_get_no_bundle_id(client, user):
    did_list, _ = create_index(client, user)
    bundle_id = str(uuid.uuid4())
    data = get_bundle_doc(did_list, bundle_id=bundle_id)

    res1 = client.post("/bundle/", json=data, headers=user)
    assert res1.status_code == 200

    res2 = client.get("/bundle/" + "hc42397hf902-37g4hf970h23479fgh9euwh")
    assert res2.status_code == 404


def test_bundle_get_expand_false(client, user):
    did_list, rec = create_index(client, user)
    res1 = client.get("/ga4gh/drs/v1/objects/" + rec["did"])

    bundle_id = str(uuid.uuid4())
    data = get_bundle_doc(did_list, bundle_id=bundle_id)

    res1 = client.post("/bundle/", json=data, headers=user)
    assert res1.status_code == 200

    res2 = client.get("/bundle/" + bundle_id + "?expand=false")
    rec2 = res2.json
    assert res2.status_code == 200
    assert rec2["id"] == bundle_id
    assert rec2["name"] == data["name"]
    assert "bundle_data" not in rec2


def test_redirect_to_bundle_from_index(client, user):
    did_list, _ = create_index(client, user)
    bundle_id = str(uuid.uuid4())
    data = get_bundle_doc(did_list, bundle_id=bundle_id)

    res2 = client.post("/bundle/", json=data, headers=user)
    assert res2.status_code == 200

    res = client.get("/bundle/" + bundle_id)
    assert res.status_code == 200

    res3 = client.get("/index/" + bundle_id)
    assert res3.status_code == 200


def test_bundle_from_drs_endpoint(client, user):
    did_list, _ = create_index(client, user)
    bundle_id = str(uuid.uuid4())
    data = get_bundle_doc(did_list, bundle_id=bundle_id)

    res2 = client.post("/bundle/", json=data, headers=user)
    assert res2.status_code == 200

    res = client.get("/bundle/" + bundle_id)
    assert res.status_code == 200

    res3 = client.get("/ga4gh/drs/v1/objects/" + bundle_id)
    assert res3.status_code == 200


def test_get_bundle_list(client, user):
    """
    bundle1
        +-object1
    bundle2
        +-object2
    .
    .
    bundlen
        +-objectn
    """
    n_bundles = 6
    n_records = 6
    for _ in range(n_records):
        _, _ = create_index(client, user)
    n_records = 6 + n_bundles

    for _ in range(n_bundles):
        did_list, _ = create_index(client, user)
        bundle_id = str(uuid.uuid4())
        data = get_bundle_doc(did_list, bundle_id=bundle_id)

        res2 = client.post("/bundle/", json=data, headers=user)
        assert res2.status_code == 200

    res3 = client.get("/bundle/")
    assert res3.status_code == 200
    rec3 = res3.json
    assert len(rec3["records"]) == n_bundles
    # check to see bundle_data is not included
    assert "bundle_data" not in rec3["records"][0]

    res4 = client.get("/bundle/?form=object")
    assert res4.status_code == 200
    rec4 = res4.json
    assert len(rec4["records"]) == n_records

    res5 = client.get("/bundle/?form=all")
    assert res5.status_code == 200
    rec5 = res5.json
    assert len(rec5["records"]) == n_records + n_bundles


def test_multiple_bundle_data(client, user):
    """
    bundle1
        +-object1
        +-object2
        .
        .
        +-objectn
    """
    n_bundle_data = 5
    did_list = []
    for _ in range(n_bundle_data):
        did, _ = create_index(client, user)
        did_list.append(did[0])

    bundle_id = str(uuid.uuid4())
    data = get_bundle_doc(did_list, bundle_id=bundle_id)
    res2 = client.post("/bundle/", json=data, headers=user)
    assert res2.status_code == 200
    res3 = client.get("/bundle/" + bundle_id + "?expand=true")
    assert res3.status_code == 200

    rec3 = res3.json
    bundle_data = rec3["contents"]
    assert len(rec3["contents"]) == n_bundle_data

    for data in bundle_data:
        assert data["id"] in did_list


def test_bundle_delete(client, user):
    n_records = 6
    n_delete = 2
    bundle_ids = []
    for _ in range(n_records):
        did_list, _ = create_index(client, user)
        bundle_id = str(uuid.uuid4())
        bundle_ids.append(bundle_id)
        data = get_bundle_doc(did_list, bundle_id=bundle_id)

        res2 = client.post("/bundle/", json=data, headers=user)
        assert res2.status_code == 200

    res3 = client.get("/bundle/")
    assert res3.status_code == 200
    rec3 = res3.json
    assert len(rec3["records"]) == n_records

    for i in range(n_delete):
        res4 = client.delete("/bundle/" + bundle_ids[i], headers=user)
        assert res4.status_code == 200
        res5 = client.get("/bundle/" + bundle_ids[i])
        assert res5.status_code == 404

    res3 = client.get("/bundle/")
    assert res3.status_code == 200
    rec3 = res3.json
    assert len(rec3["records"]) == n_records - n_delete


def test_bundle_delete_invalid_bundle_id(client, user):
    bundle_id = "12938hd981h123hd18hd80h028"
    res = client.delete("/bundle/" + bundle_id, headers=user)
    assert res.status_code == 404


def test_bundle_delete_no_bundle_id(client, user):
    res = client.delete("/bundle/", headers=user)
    assert res.status_code == 405


def test_bundle_data_bundle_and_index(client, user):
    """
    bundle_main
        +-bundle1
            +-object1
        +-bundle2
            +-object2
        +-bundle3
            +-object3
        +-object1
        +-object2
        +-object3
    """
    n_records = 3
    bundle_data_ids = []
    for _ in range(n_records):
        did_list, _ = create_index(client, user)
        bundle_id = str(uuid.uuid4())
        bundle_data_ids.append(bundle_id)
        bundle_data_ids.append(did_list[0])
        data = get_bundle_doc(did_list, bundle_id=bundle_id)

        res = client.post("/bundle/", json=data, headers=user)
        assert res.status_code == 200

    bundle_id_main = str(uuid.uuid4())
    data_main = get_bundle_doc(bundle_data_ids, bundle_id=bundle_id_main)
    res1 = client.post("/bundle/", json=data_main, headers=user)
    assert res1.status_code == 200

    res2 = client.get("/bundle/" + bundle_id_main + "?expand=true")
    assert res2.status_code == 200
    rec3 = res2.json

    assert len(rec3["contents"]) == 2 * n_records

    assert rec3["size"] == len(rec3["contents"]) * 123


def test_nested_bundle_data(client, user):
    """
    bundle1
        +-bundle2
            +-bundle3
                +-bundle4
                    +-object1
    """
    n_nested = 6
    did_list, _ = create_index(client, user)

    base_bundle_id = str(uuid.uuid4())
    base_data = get_bundle_doc(did_list, bundle_id=base_bundle_id)
    res = client.post("/bundle/", json=base_data, headers=user)
    assert res.status_code == 200

    for _ in range(n_nested):
        bundle_id = str(uuid.uuid4())
        assert bundle_id != base_bundle_id
        data = get_bundle_doc([base_bundle_id], bundle_id=bundle_id)
        res1 = client.post("/bundle/", json=data, headers=user)
        assert res1.status_code == 200
        base_bundle_id = bundle_id

    assert base_bundle_id == bundle_id
    res2 = client.get("/bundle/" + bundle_id + "?expand=true")
    assert res2.status_code == 200
    rec3 = res2.json

    for _ in range(n_nested):
        check = "bundle_data" in rec3 or "contents" in rec3
        assert check
        key = "bundle_data" if "bundle_data" in rec3 else "contents"
        rec3 = rec3[key][0]


def test_bundle_no_bundle_name(client, user):
    did_list, _ = create_index(client, user)
    bundle_id = str(uuid.uuid4())

    data = get_bundle_doc(did_list, bundle_id=bundle_id)
    del data["name"]
    res = client.post("/bundle/", json=data, headers=user)
    assert res.status_code == 200
    rec = res.json
    assert rec["bundle_id"] == bundle_id
    assert rec["name"] == bundle_id


def build_bundle(client, user):
    """
    bundle1
        +-object1
        +-bundle2
            +-object2
        +-bundle3
            +-object3
            +-bundle4
                +-object4
                +-bundle5
                    +-bundle6
                        +-object5
    """
    object_list = []
    n_objects = 5
    for _ in range(n_objects):
        did_list, _ = create_index(client, user)
        object_list.append(did_list[0])

    bundle_id = str(uuid.uuid4())
    data = get_bundle_doc([object_list[0]], bundle_id=bundle_id)
    res = client.post("/bundle/", json=data, headers=user)
    assert res.status_code == 200

    bundle_id1 = str(uuid.uuid4())
    data = get_bundle_doc([bundle_id], bundle_id=bundle_id1)
    res = client.post("/bundle/", json=data, headers=user)
    assert res.status_code == 200

    bundle_id = str(uuid.uuid4())
    data = get_bundle_doc([bundle_id1, object_list[1]], bundle_id=bundle_id)
    res = client.post("/bundle/", json=data, headers=user)
    assert res.status_code == 200

    bundle_id1 = str(uuid.uuid4())
    data = get_bundle_doc([bundle_id, object_list[2]], bundle_id=bundle_id1)
    res = client.post("/bundle/", json=data, headers=user)
    assert res.status_code == 200

    bundle_id = str(uuid.uuid4())
    data = get_bundle_doc([object_list[3]], bundle_id=bundle_id)
    res = client.post("/bundle/", json=data, headers=user)
    assert res.status_code == 200

    bundle_id2 = str(uuid.uuid4())
    data = get_bundle_doc([object_list[4], bundle_id, bundle_id1], bundle_id=bundle_id2)
    res = client.post("/bundle/", json=data, headers=user)
    assert res.status_code == 200

    return bundle_id2


def content_validation(contents):
    for content in contents:
        if len(content) != 0:
            content_validation(content["contents"])
        elif "contents" not in content:
            return False
    return True


def test_get_drs_expand_contents_default(client, user):
    bundle_id = build_bundle(client, user)
    res = client.get("/bundle/" + bundle_id)
    assert res.status_code == 200

    res2 = client.get("/ga4gh/drs/v1/objects/" + bundle_id)
    assert res2.status_code == 200
    rec2 = res2.json

    contents = rec2["contents"]
    assert len(contents) == 3


def test_get_drs_expand_contents_false(client, user):
    bundle_id = build_bundle(client, user)
    res = client.get("/bundle/" + bundle_id)
    assert res.status_code == 200

    res2 = client.get("/ga4gh/drs/v1/objects/" + bundle_id + "?expand=false")
    assert res2.status_code == 200
    rec2 = res2.json

    contents = rec2["contents"][0].get("contents", [])
    assert len(contents) == 0


def test_get_drs_expand_contents_true(client, user):
    bundle_id = build_bundle(client, user)
    res = client.get("/bundle/" + bundle_id)
    assert res.status_code == 200

    res2 = client.get("/ga4gh/drs/v1/objects/" + bundle_id + "?expand=true")
    assert res2.status_code == 200
    rec2 = res2.json

    contents = rec2["contents"]

    assert content_validation(contents)
