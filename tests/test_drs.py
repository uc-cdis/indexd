import flask
import json
import tests.conftest
import requests
import responses
from tests.default_test_settings import settings
from tests.test_bundles import get_bundle_doc


def generate_presigned_url_response(did, status=200, **query_params):
    if query_params:
        query_string = "&".join(
            f"{param}={value}" for param, value in query_params.items()
        )
        full_url = (
            "https://fictitious-commons.io/data/download/" + did + "?" + query_string
        )
    else:
        full_url = "https://fictitious-commons.io/data/download/" + did
    presigned_url = {
        "url": "https://storage.googleapis.com/nih-mock-project-released-phs123-c2/RootStudyConsentSet_phs000007.Whatever.v666.p1.c2.FBI-BMW-CIA.tar.gz?GoogleAccessId=internal-someuser-1399@dcpstage-210518.iam.gserviceaccount.com&Expires=1582215120&Signature=hUsgjkegdsfkjbsajkafnsdjksdnfjknbdsajkfbsdkjfbjdfbkjdasfbnjsdnfjsnd2FTr%2FKs2kGKs0fJ8v5elFk5NQAYdrGcU3kROrzJuHUbI%2BMZ839SAbAz2rbMBuC9e46%2BdB91%2FA==&userProject=dcf-mock-project"
    }
    responses.add(responses.GET, full_url, json=presigned_url, status=status)
    return presigned_url


def get_doc(
    has_version=True,
    urls=list(),
    has_description=True,
    has_content_created_date=True,
    has_content_updated_date=True,
):
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
    if has_description:
        doc["description"] = "A description"
    if has_content_updated_date:
        doc["content_updated_date"] = "2023-03-14T17:02:54"
    if has_content_created_date:
        doc["content_created_date"] = "2023-03-13T17:02:54"

    return doc


def get_bundle(client, user, has_description=True):
    docs = [get_doc(), get_doc()]
    dids = []
    for doc in docs:
        res = client.post("/index/", json=doc, headers=user)
        assert res.status_code == 200
        dids.append(res.json["did"])
    bundle = get_bundle_doc(bundles=dids)
    if has_description:
        bundle["description"] = "A description"

    return bundle


def test_drs_get(client, user):
    data = get_doc()
    res_1 = client.post("/index/", json=data, headers=user)
    assert res_1.status_code == 200
    rec_1 = res_1.json
    res_2 = client.get("/ga4gh/drs/v1/objects/" + rec_1["did"])
    assert res_2.status_code == 200
    rec_2 = res_2.json
    assert rec_2["id"] == rec_1["did"]
    assert rec_2["size"] == data["size"]
    for k in data["hashes"]:
        assert rec_2["checksums"][0]["checksum"] == data["hashes"][k]
        assert rec_2["checksums"][0]["type"] == k
    assert rec_2["version"]
    assert rec_2["self_uri"] == "drs://testprefix:" + rec_1["did"].split(":")[1]
    # according to ga4gh DRS blobs objects are NOT supposed to have contents. Only DRS Bundle objects should include the contetnts field
    assert "contents" not in rec_2


def test_drs_get_no_default(client, user):
    # Change default index driver settings to use no prefix
    settings["config"]["INDEX"]["driver"].config["DEFAULT_PREFIX"] = None
    settings["config"]["INDEX"]["driver"].config["ADD_PREFIX_ALIAS"] = False

    data = get_doc()
    did = "ad8f4658-6acd-4f96-0dd8-3709890c959f"
    data["did"] = did
    res_1 = client.post("/index/", json=data, headers=user)
    assert res_1.status_code == 200
    res_2 = client.get("/ga4gh/drs/v1/objects/" + did)
    assert res_2.status_code == 200
    rec_2 = res_2.json
    assert rec_2["self_uri"] == "drs://" + did

    settings["config"]["INDEX"]["driver"].config["DEFAULT_PREFIX"] = "testprefix:"
    settings["config"]["INDEX"]["driver"].config["ADD_PREFIX_ALIAS"] = True


def verify_timestamps(expected_doc, did, client, has_updated_date=True):
    drs_resp = client.get(f"/ga4gh/drs/v1/objects/{did}")
    assert drs_resp.status_code == 200

    record_resp = client.get(f"/index/{did}")
    assert record_resp.status_code == 200
    assert expected_doc["content_created_date"] == drs_resp.json["created_time"]
    assert (
        expected_doc["content_created_date"] == record_resp.json["content_created_date"]
    )
    if has_updated_date:
        assert expected_doc["content_updated_date"] == drs_resp.json["updated_time"]
        assert (
            expected_doc["content_updated_date"]
            == record_resp.json["content_updated_date"]
        )
    else:
        assert expected_doc["content_created_date"] == drs_resp.json["updated_time"]
        assert (
            expected_doc["content_created_date"]
            == record_resp.json["content_updated_date"]
        )

    assert drs_resp.json["index_created_time"] == record_resp.json["created_date"]
    assert drs_resp.json["index_updated_time"] == record_resp.json["updated_date"]


def test_timestamps(client, user):
    data = get_doc()
    create_obj_resp = client.post("/index/", json=data, headers=user)
    assert create_obj_resp.status_code == 200
    obj_did = create_obj_resp.json["did"]
    verify_timestamps(data, obj_did, client)


def test_changing_timestamps(client, user):
    data = get_doc()
    create_obj_resp = client.post("/index/", json=data, headers=user)
    assert create_obj_resp.status_code == 200
    obj_did = create_obj_resp.json["did"]
    obj_rev = create_obj_resp.json["rev"]
    update_json = {
        "content_created_date": "2023-03-15T17:02:54",
        "content_updated_date": "2023-03-30T17:02:54",
    }
    update_obj_resp = client.put(
        f"/index/{obj_did}?rev={obj_rev}", json=update_json, headers=user
    )
    assert update_obj_resp.status_code == 200
    update_obj_did = update_obj_resp.json["did"]
    verify_timestamps(update_json, update_obj_did, client)


def test_timestamps_updated_sets_to_created(client, user):
    """
    Checks that content_updated_date is set to content_created_date when none is provided.
    """
    data = get_doc(has_content_updated_date=False)
    create_obj_resp = client.post("/index/", json=data, headers=user)
    assert create_obj_resp.status_code == 200
    obj_did = create_obj_resp.json["did"]
    verify_timestamps(data, obj_did, client, has_updated_date=False)


def test_timestamps_none(client, user):
    data = get_doc(has_content_updated_date=False, has_content_created_date=False)
    create_obj_resp = client.post("/index/", json=data, headers=user)
    assert create_obj_resp.status_code == 200
    obj_did = create_obj_resp.json["did"]
    drs_resp = client.get(f"/ga4gh/drs/v1/objects/{obj_did}")
    assert drs_resp.status_code == 200
    assert drs_resp.json.get("created_time") is None
    assert drs_resp.json.get("updated_time") is None
    record_resp = client.get(f"/index/{obj_did}")
    assert record_resp.status_code == 200
    assert record_resp.json.get("content_created_date") is None
    assert record_resp.json.get("content_updated_date") is None
    assert drs_resp.json["index_created_time"] == record_resp.json["created_date"]
    assert drs_resp.json["index_updated_time"] == record_resp.json["updated_date"]


def test_drs_get_description(client, user):
    data = get_doc(has_description=True)
    res_1 = client.post("/index/", json=data, headers=user)
    assert res_1.status_code == 200
    rec_1 = res_1.json
    res_2 = client.get("/ga4gh/drs/v1/objects/" + rec_1["did"])
    assert res_2.status_code == 200
    rec_2 = res_2.json
    assert rec_2["description"] == data["description"]


def test_drs_changing_description(client, user):
    data = get_doc(has_description=True)
    create_obj_resp = client.post("/index/", json=data, headers=user)
    assert create_obj_resp.status_code == 200
    created_obj = create_obj_resp.json
    obj_did = created_obj["did"]
    obj_rev = created_obj["rev"]
    update_json = {"description": "a newly updated description"}
    update_obj_resp = client.put(
        f"/index/{obj_did}?rev={obj_rev}", json=update_json, headers=user
    )
    assert update_obj_resp.status_code == 200
    update_obj = update_obj_resp.json
    drs_resp = client.get("/ga4gh/drs/v1/objects/" + update_obj["did"])
    assert drs_resp.status_code == 200
    drs_rec = drs_resp.json
    assert drs_rec["description"] == update_json["description"]


def test_drs_get_no_description(client, user):
    data = get_doc(has_description=False)
    res_1 = client.post("/index/", json=data, headers=user)
    assert res_1.status_code == 200
    rec_1 = res_1.json
    res_2 = client.get("/ga4gh/drs/v1/objects/" + rec_1["did"])
    assert res_2.status_code == 200
    rec_2 = res_2.json
    assert rec_2["description"] is None


def test_drs_get_bundle(client, user):
    bundle = get_bundle(client, user)
    bundle_res = client.post("/bundle/", json=bundle, headers=user)
    assert bundle_res.status_code == 200
    bundle_id = bundle_res.json["bundle_id"]
    drs_res = client.get(f"/ga4gh/drs/v1/objects/{bundle_id}", headers=user)
    assert drs_res.status_code == 200
    assert drs_res.json["description"] == bundle["description"]


def test_drs_get_bundle_no_description(client, user):
    bundle = get_bundle(client, user, has_description=False)
    bundle_res = client.post("/bundle/", json=bundle, headers=user)
    assert bundle_res.status_code == 200
    bundle_id = bundle_res.json["bundle_id"]
    drs_res = client.get(f"/ga4gh/drs/v1/objects/{bundle_id}", headers=user)
    assert drs_res.status_code == 200
    assert drs_res.json["description"] is ""


def test_drs_multiple_endpointurl(client, user):
    object_urls = {
        "sftp": "sftp://endpointurl/bucket/key",
        "ftp": "ftp://endpointurl/bucket/key",
        "gs": "gs://endpointurl/bucket/key",
        "s3": "s3://endpointurl/bucket/key",
    }
    data = get_doc(urls=list(object_urls.values()))
    res_1 = client.post("/index/", json=data, headers=user)
    assert res_1.status_code == 200
    rec_1 = res_1.json
    res_2 = client.get("/ga4gh/drs/v1/objects/" + rec_1["did"])

    assert res_2.status_code == 200
    rec_2 = res_2.json
    assert rec_2["id"] == rec_1["did"]

    for url in rec_2["access_methods"]:
        protocol = url["type"]
        assert url["access_url"]["url"] == object_urls[protocol]


def test_drs_list(client, user):
    record_length = 7
    data = get_doc()
    submitted_guids = []
    for _ in range(record_length):
        res_1 = client.post("/index/", json=data, headers=user)
        did = res_1.json["did"]
        submitted_guids.append(did)
        bundle_data = get_bundle_doc(bundles=[did])
        res2 = client.post("/bundle/", json=bundle_data, headers=user)
        assert res_1.status_code == 200

    res_2 = client.get("/ga4gh/drs/v1/objects")
    assert res_2.status_code == 200
    rec_2 = res_2.json
    assert len(rec_2["drs_objects"]) == 2 * record_length
    assert submitted_guids.sort() == [r["id"] for r in rec_2["drs_objects"]].sort()

    res_3 = client.get("/ga4gh/drs/v1/objects/?form=bundle")
    assert res_3.status_code == 200
    rec_3 = res_3.json
    assert len(rec_3["drs_objects"]) == record_length

    res_4 = client.get("/ga4gh/drs/v1/objects/?form=object")
    assert res_4.status_code == 200
    rec_4 = res_4.json
    assert len(rec_4["drs_objects"]) == record_length


def test_get_drs_record_not_found(client, user):
    # test exception raised at nonexistent
    fake_did = "testprefix:d96bab16-c4e1-44ac-923a-04328b6fe78f"
    res = client.get("/ga4gh/drs/v1/objects/" + fake_did)
    assert res.status_code == 404


def test_get_drs_with_encoded_slash(client, user):
    data = get_doc()
    data["did"] = "testprefix:ed8f4658-6acd-4f96-9dd8-3709890c959e"
    res_1 = client.post("/index/", json=data, headers=user)
    assert res_1.status_code == 200
    rec_1 = res_1.json
    did = "testprefix%3aed8f4658-6acd-4f96-9dd8-3709890c959e"
    res_2 = client.get("/ga4gh/drs/v1/objects/" + did)
    assert res_2.status_code == 200
    rec_2 = res_2.json
    assert rec_2["id"] == rec_1["did"]
    assert rec_2["size"] == data["size"]
    for k in data["hashes"]:
        assert rec_2["checksums"][0]["checksum"] == data["hashes"][k]
        assert rec_2["checksums"][0]["type"] == k
    assert rec_2["version"]
    assert rec_2["self_uri"] == "drs://testprefix:" + rec_1["did"].split(":")[1]


def test_drs_service_info_endpoint(client):
    """
    Test drs service endpoint with drs service info friendly distribution information
    """
    app = flask.Flask(__name__)

    expected_info = {
        "id": "io.fictitious-commons",
        "name": "DRS System",
        "type": {
            "group": "org.ga4gh",
            "artifact": "drs",
            "version": "1.0.3",
        },
        "version": "1.0.3",
        "organization": {
            "name": "CTDS",
            "url": "https://fictitious-commons.io",
        },
    }

    res = client.get("/ga4gh/drs/v1/service-info")

    assert res.status_code == 200
    assert res.json == expected_info


def test_drs_service_info_no_information_configured(client):
    """
    Test drs service info endpoint when dist is not configured in the indexd config file
    """
    expected_info = {
        "id": "io.fictitious-commons",
        "name": "DRS System",
        "type": {
            "group": "org.ga4gh",
            "artifact": "drs",
            "version": "1.0.3",
        },
        "version": "1.0.3",
        "organization": {
            "name": "CTDS",
            "url": "https://fictitious-commons.io",
        },
    }
    backup = settings["config"]["DRS_SERVICE_INFO"].copy()

    try:
        settings["config"]["DRS_SERVICE_INFO"].clear()

        res = client.get("/ga4gh/drs/v1/service-info")

        assert res.status_code == 200
        assert res.json == expected_info
    finally:
        settings["config"]["DRS_SERVICE_INFO"] = backup
