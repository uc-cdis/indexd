import json
import tests.conftest
import requests
import responses
from tests.default_test_settings import settings


def generate_presigned_url_response(did, protocol="", status=200):
    full_url = (
        "https://fictitious-commons.io/data/download/" + did + "?protocol=" + protocol
    )
    presigned_url = {
        "url": "https://storage.googleapis.com/nih-mock-project-released-phs123-c2/RootStudyConsentSet_phs000007.Whatever.v666.p1.c2.FBI-BMW-CIA.tar.gz?GoogleAccessId=internal-someuser-1399@dcpstage-210518.iam.gserviceaccount.com&Expires=1582215120&Signature=hUsgjkegdsfkjbsajkafnsdjksdnfjknbdsajkfbsdkjfbjdfbkjdasfbnjsdnfjsnd2FTr%2FKs2kGKs0fJ8v5elFk5NQAYdrGcU3kROrzJuHUbI%2BMZ839SAbAz2rbMBuC9e46%2BdB91%2FA==&userProject=dcf-mock-project"
    }
    responses.add(responses.GET, full_url, json=presigned_url, status=status)
    return presigned_url


def get_doc(has_version=True, urls=list(), drs_list=0):
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
    # if drs_list > 0:
    #     ret = {"drs_objects": []}
    #     for _ in range(drs_list):
    #         ret["drs_objects"].append(doc)
    #     return ret
    return doc


def test_drs_get(client, user):
    data = get_doc()
    res_1 = client.post("/index/", json=data, headers=user)
    assert res_1.status_code == 200
    rec_1 = res_1.json
    res_2 = client.get("/ga4gh/drs/v1/objects/" + rec_1["did"])
    print(rec_1["did"])
    assert res_2.status_code == 200
    rec_2 = res_2.json
    assert rec_2["id"] == rec_1["did"]
    assert rec_2["size"] == data["size"]
    for k in data["hashes"]:
        assert rec_2["checksums"][0]["checksum"] == data["hashes"][k]
        assert rec_2["checksums"][0]["type"] == k
    assert rec_2["version"]
    assert rec_2["self_uri"] == "drs://fictitious-commons.io/" + rec_1["did"]


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


@responses.activate
def test_drs_get_with_presigned_url(client, user):
    data = get_doc()
    res_1 = client.post("/index/", json=data, headers=user)
    assert res_1.status_code == 200
    rec_1 = res_1.json
    presigned = generate_presigned_url_response(rec_1["did"])
    res_2 = client.get(
        "/ga4gh/drs/v1/objects/" + rec_1["did"], headers={"AUTHORIZATION": "12345"}
    )
    assert res_2.status_code == 200
    rec_2 = res_2.json
    assert rec_2["id"] == rec_1["did"]
    assert rec_2["size"] == data["size"]
    for k in data["hashes"]:
        assert rec_2["checksums"][0]["checksum"] == data["hashes"][k]
        assert rec_2["checksums"][0]["type"] == k

    assert rec_2["access_methods"][0]["access_url"] == presigned


def test_drs_list(client, user):
    record_length = 7
    data = get_doc()
    submitted_guids = []
    for _ in range(record_length):
        res_1 = client.post("/index/", json=data, headers=user)
        submitted_guids.append(res_1.json["did"])
        assert res_1.status_code == 200
    res_2 = client.get("/ga4gh/drs/v1/objects")
    assert res_2.status_code == 200
    rec_2 = res_2.json
    assert len(rec_2["drs_objects"]) == record_length
    assert submitted_guids.sort() == [r["id"] for r in rec_2["drs_objects"]].sort()


def test_get_drs_record_not_found(client, user):
    # test exception raised at nonexistent
    fake_did = "testprefix:d96bab16-c4e1-44ac-923a-04328b6fe78f"
    res = client.get("/ga4gh/drs/v1/objects/" + fake_did)
    assert res.status_code == 404


@responses.activate
def test_get_presigned_url_with_access_id(client, user):
    data = get_doc()
    res_1 = client.post("/index/", json=data, headers=user)
    assert res_1.status_code == 200
    rec_1 = res_1.json
    access_id_list = ["s3", "gs", "ftp"]
    for access_id in access_id_list:
        presigned = generate_presigned_url_response(rec_1["did"], access_id)
        res_2 = client.get(
            "/ga4gh/drs/v1/objects/" + rec_1["did"] + "/access/" + access_id,
            headers={"AUTHORIZATION": "12345"},
        )
        assert res_2.status_code == 200
        assert res_2.json == presigned


def test_get_presigned_url_no_access_id(client, user):
    data = get_doc()
    res_1 = client.post("/index/", json=data, headers=user)
    assert res_1.status_code == 200
    rec_1 = res_1.json
    generate_presigned_url_response(rec_1["did"], "s3")
    res_2 = client.get(
        "/ga4gh/drs/v1/objects/" + rec_1["did"] + "/access/",
        headers={"AUTHORIZATION": "12345"},
    )
    assert res_2.status_code == 400


def test_get_presigned_url_no_bearer_token(client, user):
    data = get_doc()
    res_1 = client.post("/index/", json=data, headers=user)
    assert res_1.status_code == 200
    rec_1 = res_1.json
    generate_presigned_url_response(rec_1["did"], "s3")
    res_2 = client.get("/ga4gh/drs/v1/objects/" + rec_1["did"] + "/access/s3")
    assert res_2.status_code == 403


@responses.activate
def test_get_presigned_url_wrong_access_id(client, user):
    data = get_doc()
    res_1 = client.post("/index/", json=data, headers=user)
    assert res_1.status_code == 200
    rec_1 = res_1.json
    generate_presigned_url_response(rec_1["did"], "s2", status=404)
    res_2 = client.get(
        "/ga4gh/drs/v1/objects/" + rec_1["did"] + "/access/s2",
        headers={"AUTHORIZATION": "12345"},
    )
    assert res_2.status_code == 404


def test_get_drs_with_encoded_slash(client, user):
    data = get_doc()
    data["did"] = "dg.TEST/ed8f4658-6acd-4f96-9dd8-3709890c959e"
    res_1 = client.post("/index/", json=data, headers=user)
    assert res_1.status_code == 200
    rec_1 = res_1.json
    did = "dg.TEST%2Fed8f4658-6acd-4f96-9dd8-3709890c959e"
    res_2 = client.get("/ga4gh/drs/v1/objects/" + did)
    assert res_2.status_code == 200
    rec_2 = res_2.json
    assert rec_2["id"] == rec_1["did"]
    assert rec_2["size"] == data["size"]
    for k in data["hashes"]:
        assert rec_2["checksums"][0]["checksum"] == data["hashes"][k]
        assert rec_2["checksums"][0]["type"] == k
    assert rec_2["version"]
    assert rec_2["self_uri"] == "drs://fictitious-commons.io/" + rec_1["did"]


@responses.activate
def test_get_presigned_url_with_encoded_slash(client, user):
    data = get_doc()
    data["did"] = "dg.TEST/ed8f4658-6acd-4f96-9dd8-3709890c959e"
    did = "dg.TEST%2Fed8f4658-6acd-4f96-9dd8-3709890c959e"
    res_1 = client.post("/index/", json=data, headers=user)
    assert res_1.status_code == 200
    rec_1 = res_1.json
    access_id_list = ["s3", "gs", "ftp"]
    for access_id in access_id_list:
        presigned = generate_presigned_url_response(rec_1["did"], access_id)
        res_2 = client.get(
            "/ga4gh/drs/v1/objects/" + did + "/access/" + access_id,
            headers={"AUTHORIZATION": "12345"},
        )
        assert res_2.status_code == 200
        assert res_2.json == presigned
