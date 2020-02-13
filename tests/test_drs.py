import json

import pytest

from tests.util import assert_blank
from indexd.index.blueprint import ACCEPTABLE_HASHES

from indexd.drs.blueprint import get_presigned_url


def get_doc(has_version=True, multiple_endpointurl = False, has_content=False):
    doc = {
        "form": "object",
        "size": 123,
        "urls": ["s3://endpointurl/bucket/key"],
        "hashes": {"md5": "8b9942cf415384b27cadf1f4d2d682e5"},
        }
    if has_version:
        doc["version"] = "1"
    if multiple_endpointurl:
        for url_type in ["gs", "ftp", "sftp"]:
            doc["urls"].append("{}://endpointurl/bucket/key".format(url_type))
    #add support for contents field (bundle stuff)

    return doc


def test_drs_get(client, user):
    data = get_doc()
    res_1 = client.post("/index/", json=data, headers=user)
    assert res_1.status_code == 200
    rec_1 = res_1.json
    res_2 = client.get("/ga4gh/drs/v1/objects/" + rec_1["did"])

    assert res_2.status_code == 200
    rec_2 = res_2.json
    print("WITHOUT SIGNED URL: {}".format(rec_2))
    assert rec_2["drs_object"]["id"] == rec_1["did"] 
    assert rec_2["drs_object"]["size"] == 123
    assert (
        rec_2["drs_object"]["checksums"][0]["checksum"]
        == "8b9942cf415384b27cadf1f4d2d682e5"
    )
    assert rec_2["drs_object"]["checksums"][0]["type"] == "md5"


def test_drs_multiple_endpointurl(client,user):
    data = get_doc(multiple_endpointurl=True)
    res_1 = client.post("/index/", json=data, headers=user)
    assert res_1.status_code == 200
    rec_1 = res_1.json
    res_2 = client.get("/ga4gh/drs/v1/objects/" + rec_1["did"])

    assert res_2.status_code == 200
    rec_2 = res_2.json
    assert rec_2["drs_object"]["id"] == rec_1["did"]

    endpointurls = [
        "sftp://endpointurl/bucket/key",
        "ftp://endpointurl/bucket/key",
        "gs://endpointurl/bucket/key",
        "s3://endpointurl/bucket/key"
    ]
    endpointtypes = ["sftp", "ftp", "gs", "s3"]
    for url in rec_2["drs_object"]["access_methods"]:
        assert url["access_url"]["url"] in endpointurls
    
    for url in rec_2["drs_object"]["access_methods"]:
        assert url["type"] in endpointtypes

    for methods in rec_2["drs_object"]["access_methods"]:
        assert methods["access_id"] in endpointtypes
    

def test_drs_list(client, user):
    data = get_doc()
    number_of_objects = 20

    res_1 = client.post("/index/", json=data, headers=user)
    assert res_1.status_code == 200
    rec_1 = res_1.json

    res_2 = client.get("/ga4gh/drs/v1/objects?page_size=100")
    assert res_2.status_code == 200
    
    rec_2 = res_2.json
    for _ in range(number_of_objects-1):
        #make copies of drs_objects
        rec_2["drs_objects"].append(res_2.json["drs_objects"])
    assert len(rec_2["drs_objects"]) == number_of_objects

    assert rec_2["drs_objects"][0]["id"] == rec_1["did"]
    assert rec_2["drs_objects"][0]["size"] == 123
    assert (
        rec_2["drs_objects"][0]["checksums"][0]["checksum"]
        == "8b9942cf415384b27cadf1f4d2d682e5"
    )


    assert rec_2["drs_objects"][0]["checksums"][0]["type"] == "md5"

def test_get_drs_record_error(client, user):
    # test exception raised at nonexistent
    fake_did = "testprefix:d96bab16-c4e1-44ac-923a-04328b6fe78f"
    res = client.get("/ga4gh/drs/v1/objects/" + fake_did)
    assert res.status_code == 404

def test_get_presigned_url(client, user):
    data = get_doc()
    res_1 = client.post("/index/", json=data, headers=user)
    assert res_1.status_code == 200
    rec_1 = res_1.json
    res_2 = client.get("/ga4gh/drs/v1/objects/{}/access/{}".format(rec_1["did"], "s3"))
    print(res_2.json)
    assert res_2.status_code == 200

