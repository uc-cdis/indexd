import os
import json
from fastapi import APIRouter, Request, HTTPException, status
from fastapi.responses import JSONResponse

from indexd.errors import UserError
from indexd.index.errors import NoRecordFound as IndexNoRecordFound
from indexd.utils import reverse_url

router = APIRouter(tags=["drs"])

router.index_driver = None
router.service_info = {}


def set_drs_config(app):
    index_config = app.settings["config"]["INDEX"]
    router.index_driver = index_config["driver"]
    router.service_info = app.settings.get("DRS_SERVICE_INFO", {})


@router.get("/ga4gh/drs/v1/service-info")
async def get_drs_service_info():
    """
    Returns DRS compliant service information
    """

    reverse_domain_name = reverse_url(url=os.environ.get("HOSTNAME"))
    ret = {
        "id": reverse_domain_name,
        "name": "DRS System",
        "version": "1.0.3",
        "type": {
            "group": "org.ga4gh",
            "artifact": "drs",
            "version": "1.0.3",
        },
        "organization": {
            "name": "CTDS",
            "url": "https://" + os.environ.get("HOSTNAME"),
        },
    }

    if router.service_info:
        for key, value in router.service_info.items():
            if key in ret:
                if isinstance(value, dict):
                    for inner_key, inner_value in value.items():
                        ret[key][inner_key] = inner_value
                else:
                    ret[key] = value
    return JSONResponse(content=ret, status_code=200)


@router.get("/ga4gh/drs/v1/objects/{object_id:path}")
async def get_drs_object(object_id: str, request: Request):
    """
    Returns a specific DRSobject with object_id
    """

    expand = request.query_params.get("expand") == "true"
    try:
        ret = router.index_driver.get_with_nonstrict_prefix(object_id)
    except IndexNoRecordFound as err:
        raise HTTPException(status_code=404, detail=str(err))

    data = indexd_to_drs(ret, expand=expand)
    return JSONResponse(content=data, status_code=200)


@router.get("/ga4gh/drs/v1/objects")
async def list_drs_records(request: Request):
    limit = request.query_params.get("limit")
    start = request.query_params.get("start")
    page = request.query_params.get("page")
    form = request.query_params.get("form")

    try:
        limit = 100 if limit is None else int(limit)
    except ValueError as err:
        raise UserError("Limit must be an integer.")

    if limit < 0 or limit > 1024:
        raise UserError("Limit must be between 0 and 1024.")

    if page is not None:
        try:
            page = int(page)
        except ValueError as err:
            raise UserError("Page must be an integer.")

    if form == "bundle":
        records = router.index_driver.get_bundle_list(
            start=start, limit=limit, page=page
        )
    elif form == "object":
        records = router.index_driver.ids(start=start, limit=limit, page=page)
    else:
        records = router.index_driver.get_bundle_and_object_list(
            start=start, limit=limit, page=page
        )

    ret = {
        "drs_objects": [indexd_to_drs(record, True) for record in records],
    }
    return JSONResponse(content=ret, status_code=200)


def create_drs_uri(did):
    """
    Return ga4gh-compilant drs format uri

    Args:
        did(str): did of drs object
    """

    default_prefix = router.index_driver.config.get("DEFAULT_PREFIX")

    if not default_prefix:
        # For env without DEFAULT_PREFIX, uri will not be drs compliant
        accession = did
        self_uri = f"drs://{accession}"
    else:
        accession = (
            did.replace(default_prefix, "", 1).replace("/", "", 1).replace(":", "", 1)
        )
        self_uri = f"drs://{default_prefix.replace('/', '', 1).replace(':', '', 1)}:{accession}"

    return self_uri


def indexd_to_drs(record, expand=False):
    """
    Convert record to ga4gh-compilant format

    Args:
        record(dict): json object record
        expand(bool): show contents of the descendants
    """

    did = record.get("id") or record.get("did") or record.get("bundle_id")
    self_uri = create_drs_uri(did)
    name = record.get("file_name") or record.get("name")
    index_created_time = record.get("created_date") or record.get("created_time")
    version = record.get("version") or record.get("rev") or ""
    index_updated_time = record.get("updated_date") or record.get("updated_time")
    content_created_date = record.get("content_created_date", "")
    content_updated_date = record.get("content_updated_date", "")
    form = record.get("form", "bundle")
    description = record.get("description")
    alias = (
        record.get("alias") or json.loads(record.get("aliases", "[]"))
        if "aliases" in record
        else []
    )

    drs_object = {
        "id": did,
        "mime_type": "application/json",
        "name": name,
        "index_created_time": index_created_time,
        "index_updated_time": index_updated_time,
        "created_time": content_created_date,
        "updated_time": content_updated_date,
        "size": record.get("size", 0),
        "aliases": alias,
        "self_uri": self_uri,
        "version": version,
        "form": form,
        "checksums": [],
        "description": description,
    }

    if "description" in record:
        drs_object["description"] = record["description"]

    if "bundle_data" in record:
        drs_object["contents"] = []
        for bundle in record["bundle_data"]:
            bundle_object = bundle_to_drs(bundle, expand=expand, is_content=True)
            if not expand:
                bundle_object.pop("contents", None)
            drs_object["contents"].append(bundle_object)

    # access_methods mapping
    if "urls" in record:
        drs_object["access_methods"] = []
        for location in record["urls"]:
            location_type = location.split(":")[0]
            drs_object["access_methods"].append(
                {
                    "type": location_type,
                    "access_url": {"url": location},
                    "access_id": location_type,
                    "region": "",
                }
            )

    drs_object["checksums"] = parse_checksums(record, drs_object)
    return drs_object


def bundle_to_drs(record, expand=False, is_content=False):
    """
    record(dict): json object record
    expand(bool): show contents of the descendants
    is_content: is an expanded content in a bundle
    """

    did = record.get("id") or record.get("did") or record.get("bundle_id")
    drs_uri = create_drs_uri(did)

    name = record.get("file_name") or record.get("name")

    contents = record.get("contents") or record.get("bundle_data") or []
    if not expand and isinstance(contents, list):
        for content in contents:
            if isinstance(content, dict):
                content.pop("contents", None)

    drs_object = {
        "id": did,
        "name": name,
        "drs_uri": drs_uri,
        "contents": contents,
    }

    description = record.get("description", "")
    aliases = (
        record.get("alias") or json.loads(record.get("aliases", "[]"))
        if "aliases" in record
        else []
    )
    version = record.get("version") or record.get("rev") or ""
    created_time = record.get("created_date") or record.get("created_time")
    updated_time = record.get("updated_date") or record.get("updated_time")

    if not is_content:
        drs_object["checksums"] = parse_checksums(record, drs_object)
        if created_time:
            drs_object["created_time"] = created_time
        if updated_time:
            drs_object["updated_time"] = updated_time
        drs_object["size"] = record.get("size", 0)
        drs_object["aliases"] = aliases
        drs_object["description"] = description
        drs_object["version"] = version

    return drs_object


def parse_checksums(record, drs_object):
    """
    Create valid checksums format from a DB object -
    either a record ("hashes") or a bundle ("checksum")
    """

    ret_checksum = []
    if "hashes" in record:
        for k in record["hashes"]:
            ret_checksum.append({"checksum": record["hashes"][k], "type": k})
    elif "checksum" in record:
        try:
            checksums = json.loads(record["checksum"])
        except json.decoder.JSONDecodeError:
            # TODO: Remove the code after fixing the record["checksum"] format
            checksums = [{"checksum": record["checksum"], "type": "md5"}]
        for checksum in checksums:
            ret_checksum.append(
                {"checksum": checksum["checksum"], "type": checksum["type"]}
            )
    return ret_checksum
