import sys

import pytest


def test_default_settings(app):
    """
    Test that the RBAC in default settings should be False.
    """
    from indexd import default_settings

    assert "RBAC" in default_settings.settings, "RBAC setting should be present in default settings"
    assert default_settings.settings["RBAC"] is False, "RBAC should be disabled by default"


def get_doc(
    has_metadata=True, has_baseid=False, has_urls_metadata=False, has_version=False
):
    doc = {
        "form": "object",
        "size": 123,
        "urls": ["s3://endpointurl/bucket/key"],
        "hashes": {"md5": "8b9942cf415384b27cadf1f4d2d682e5"},
        "authz": ["/programs/bpa/projects/UChicago"],
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


def test_index_no_parameters(client, user, mock_arborist_requests, is_rbac_configured):
    """
    Test that the index endpoint without parameters returns expected projects.
    """
    if not is_rbac_configured:
        pytest.skip("RBAC is not configured, skipping test.")

    # write 2 records with different authz
    data1 = get_doc()

    data1["urls"] = [
        "s3://endpointurl/bucket_2/key_2",
        "s3://anotherurl/bucket_2/key_2",
    ]
    data1["urls_metadata"] = {
        "s3://endpointurl/bucket_2/key_2": {"state": "error", "other": "xxx"},
        "s3://anotherurl/bucket_2/key_2": {"state": "error", "other": "xxx"},
    }
    data1["authz"] = ["/programs/bpa/projects/UChicago"]
    res_1 = client.post("/index/", json=data1, headers=user)
    assert res_1.status_code == 200

    data2 = get_doc()
    data2["metadata"] = {"project_id": "other-project", "state": "abc", "other": "xxx"}
    data2["urls"] = ["s3://endpointurl/bucket/key_2", "s3://anotherurl/bucket/key_2"]
    data2["authz"] = ["/programs/other/projects/project"]
    data2["urls_metadata"] = {
        "s3://endpointurl/bucket/key_2": {"state": "error", "other": "yyy"}
    }

    print("DEBUG >>>>>> User should have access to 2 records", file=sys.stderr)

    res_2 = client.post("/index/", json=data2, headers=user)
    assert res_2.status_code == 200

    data_all_by_md = client.get("/index", headers=user)
    data_all_list = data_all_by_md.json

    assert len(data_all_list[
                   "records"]) == 2, f"Should have access to 2 records, got {len(data_all_list['records'])} records: {data_all_list}"

    print("DEBUG >>>>>> User should have access to 1 records", file=sys.stderr)
    mock_arborist_requests(
        resource_method_to_authorized={
            "/programs/other/projects/project": {"read": True},
        }
    )

    data_all_by_md = client.get("/index", headers=user)
    data_all_list = data_all_by_md.json

    assert len(data_all_list[
                   "records"]) == 1, f"Should have access to 1 records, got {len(data_all_list['records'])} records: {data_all_list}"

    # user can't read any of the existing projects
    mock_arborist_requests(
        resource_method_to_authorized={
            "/programs/foo/projects/bar": {"read": True},
        }
    )

    print("DEBUG >>>>>> User should not have access to any records", file=sys.stderr)
    data_all_by_md = client.get("/index", headers=user)
    assert data_all_by_md.status_code == 200, f"Expected status code 200, got {data_all_by_md.status_code}"
    data_all_list = data_all_by_md.json

    assert len(data_all_list[
                   "records"]) == 0, f"Should have access to 0 records, got {len(data_all_list['records'])} records: {data_all_list}"

    # user has no access to anything
    mock_arborist_requests(
        resource_method_to_authorized={
        }
    )

    print("DEBUG >>>>>> User should not have access to anything", file=sys.stderr)
    data_all_by_md = client.get("/index", headers=user)
    assert data_all_by_md.status_code == 403, f"Expected status code 403, got {data_all_by_md.status_code}"

    print("DEBUG >>>>>> User missing", file=sys.stderr)
    data_all_by_md = client.get("/index")
    assert data_all_by_md.status_code == 403, f"Expected status code 403, got {data_all_by_md.status_code}"
