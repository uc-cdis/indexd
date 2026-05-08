import re
import json
import hashlib
import jsonschema
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from ..version_data import VERSION, COMMIT
from indexd import auth
from indexd.errors import UserError
from .schema import (
    PUT_RECORD_SCHEMA,
    POST_RECORD_SCHEMA,
    RECORD_ALIAS_SCHEMA,
    BUNDLE_SCHEMA,
    UPDATE_ALL_VERSIONS_SCHEMA,
)

from indexd.drs.blueprint import bundle_to_drs
from cdislogging import get_logger

logger = get_logger("indexd/index router", log_level="info")

router = APIRouter(tags=["index"])

router.config = dict()
router.index_driver = None
router.dist = []

ACCEPTABLE_HASHES = {
    "md5": re.compile(r"^[0-9a-f]{32}$").match,
    "sha1": re.compile(r"^[0-9a-f]{40}$").match,
    "sha256": re.compile(r"^[0-9a-f]{64}$").match,
    "sha512": re.compile(r"^[0-9a-f]{128}$").match,
    "crc": re.compile(r"^[0-9a-f]{8}$").match,
    "etag": re.compile(r"^[0-9a-f]{32}(-\d+)?$").match,
}


def set_index_config(app):
    router.index_driver = app.settings["INDEX"]["driver"]
    router.dist = app.settings.get("DIST", [])


def validate_hashes(**hashes):
    """
    Validate hashes against known and valid hashing algorithms.
    """

    if not all(h in ACCEPTABLE_HASHES for h in hashes):
        raise UserError("Invalid hash types specified.")
    if not all(ACCEPTABLE_HASHES[h](v) for h, v in hashes.items()):
        raise UserError("Invalid hash values specified.")


@router.get("/index/")
async def get_index(request: Request, form: str = None):
    """
    Returns a list of records.
    """

    qp = request.query_params
    limit = qp.get("limit")
    start = qp.get("start")
    page = qp.get("page")
    ids = qp.get("ids")
    ids = ids.split(",") if ids else None
    if ids and (start is not None or limit is not None or page is not None):
        raise UserError("Pagination is not supported when ids is provided.")
    try:
        limit = 100 if limit is None else int(limit)
    except ValueError:
        raise UserError("Limit must be an integer.")
    if limit < 0 or limit > 1024:
        raise UserError("Limit must be between 0 and 1024.")
    if page is not None:
        try:
            page = int(page)
        except ValueError:
            raise UserError("Page must be an integer.")
    size = qp.get("size")
    try:
        size = size if size is None else int(size)
    except ValueError:
        raise UserError("size must be an integer")
    if size is not None and size < 0:
        raise UserError("size must be > 0")
    uploader = qp.get("uploader")
    # TODO: Based on indexclient, url here should be urls instead. Or change urls to url in indexclient.
    urls = qp.getlist("url")
    file_name = qp.get("file_name")
    version = qp.get("version")
    hashes = qp.getlist("hash")
    hashes = dict(x.split(":", 1) for x in hashes)
    validate_hashes(**hashes)
    hashes = hashes if hashes else None
    metadata = qp.getlist("metadata")
    metadata = dict(x.split(":", 1) for x in metadata)
    acl = qp.get("acl")
    acl = [] if acl == "null" else acl.split(",") if acl else None
    authz = qp.get("authz")
    authz = [] if authz == "null" else authz.split(",") if authz else None
    urls_metadata = qp.get("urls_metadata")
    if urls_metadata:
        try:
            urls_metadata = json.loads(urls_metadata)
        except ValueError:
            raise UserError("urls_metadata must be a valid json string")
    negate_params = qp.get("negate_params")
    if negate_params:
        try:
            negate_params = json.loads(negate_params)
        except ValueError:
            raise UserError("negate_params must be a valid json string")
    form = qp.get("form") if not form else form
    if form == "bundle":
        records = router.index_driver.get_bundle_list(
            start=start, limit=limit, page=page
        )
    elif form == "all":
        records = router.index_driver.get_bundle_and_object_list(
            limit=limit,
            page=page,
            start=start,
            size=size,
            urls=urls,
            acl=acl,
            authz=authz,
            hashes=hashes,
            file_name=file_name,
            version=version,
            uploader=uploader,
            metadata=metadata,
            ids=ids,
            urls_metadata=urls_metadata,
            negate_params=negate_params,
        )
    else:
        records = router.index_driver.ids(
            start=start,
            limit=limit,
            page=page,
            size=size,
            file_name=file_name,
            version=version,
            urls=urls,
            acl=acl,
            authz=authz,
            hashes=hashes,
            uploader=uploader,
            ids=ids,
            metadata=metadata,
            urls_metadata=urls_metadata,
            negate_params=negate_params,
        )
    base = {
        "ids": ids,
        "records": records,
        "limit": limit,
        "start": start,
        "page": page,
        "size": size,
        "file_name": file_name,
        "version": version,
        "urls": urls,
        "acl": acl,
        "authz": authz,
        "hashes": hashes,
        "metadata": metadata,
        "urls_metadata": urls_metadata,
    }
    return JSONResponse(content=base, status_code=200)


@router.get("/urls/")
async def get_urls(request: Request):
    """
    Returns a list of urls.
    """
    qp = request.query_params
    ids = qp.get("ids")
    ids = ids.split(",") if ids else None
    hashes = qp.getlist("hash")
    hashes = dict(x.split(":", 1) for x in hashes)
    size = qp.get("size")
    if size:
        try:
            size = int(size)
        except Exception:
            raise UserError("Size must be an integer.")
        if size < 0:
            raise UserError("Size must be >= 0.")
    try:
        start = int(qp.get("start", 0))
    except Exception:
        raise UserError("Start must be an integer.")
    try:
        limit = int(qp.get("limit", 100))
    except Exception:
        raise UserError("Limit must be an integer.")
    if start < 0:
        raise UserError("Start must be >= 0.")
    if limit < 0:
        raise UserError("Limit must be >= 0.")
    if limit > 1024:
        raise UserError("Limit must be <= 1024.")
    validate_hashes(**hashes)
    urls = router.index_driver.get_urls(
        size=size, ids=ids, hashes=hashes, start=start, limit=limit
    )
    ret = {"urls": urls, "limit": limit, "start": start, "size": size, "hashes": hashes}
    return JSONResponse(content=ret, status_code=200)


# NOTE: /index/<record>/deeper-route methods are above /index/<record> so that routing
# prefers these first. Without this ordering, newer versions of the web framework
# were interpretting index/e383a3aa-316e-4a51-975d-d699eff41bd2/aliases/ as routing
# to /index/<record> where <record> was "e383a3aa-316e-4a51-975d-d699eff41bd2/aliases/"


@router.get("/index/{record:path}/aliases")
async def get_aliases(record: str):
    """
    Get all aliases associated with this DID / GUID
    """

    aliases = router.index_driver.get_aliases_for_did(record)
    aliases_payload = {"aliases": [{"value": alias} for alias in aliases]}
    return JSONResponse(content=aliases_payload, status_code=200)


@router.post("/index/{record:path}/aliases/")
async def append_aliases(record: str, request: Request):
    """
    Append one or more aliases to aliases already associated with this DID / GUID, if any.
    """

    aliases_json = await request.json()
    try:
        jsonschema.validate(aliases_json, RECORD_ALIAS_SCHEMA)
    except jsonschema.ValidationError as err:
        logger.warning(f"Bad request body:\n{err}")
        raise UserError(err)
    aliases = [item["value"] for item in aliases_json["aliases"]]
    router.index_driver.append_aliases_for_did(aliases, record)
    aliases = router.index_driver.get_aliases_for_did(record)
    aliases_payload = {"aliases": [{"value": alias} for alias in aliases]}
    return JSONResponse(content=aliases_payload, status_code=200)


@router.put("/index/{record:path}/aliases")
async def replace_aliases(record: str, request: Request):
    """
    Replace all aliases associated with this DID / GUID
    """

    aliases_json = await request.json()
    try:
        jsonschema.validate(aliases_json, RECORD_ALIAS_SCHEMA)
    except jsonschema.ValidationError as err:
        logger.warning(f"Bad request body:\n{err}")
        raise UserError(err)
    aliases = [item["value"] for item in aliases_json["aliases"]]
    router.index_driver.replace_aliases_for_did(aliases, record)
    aliases_payload = {"aliases": [{"value": alias} for alias in aliases]}
    return JSONResponse(content=aliases_payload, status_code=200)


@router.delete("/index/{record:path}/aliases")
async def delete_all_aliases(record: str):
    router.index_driver.delete_all_aliases_for_did(record)
    return JSONResponse(content="Aliases deleted successfully", status_code=200)


@router.delete("/index/{record:path}/aliases/{alias:path}")
async def delete_one_alias(record: str, alias: str):
    router.index_driver.delete_one_alias_for_did(alias, record)
    return JSONResponse(content="Aliases deleted successfully", status_code=200)


@router.get("/index/{record:path}/versions")
async def get_all_index_record_versions(record: str):
    """
    Get all record versions
    """
    ret = router.index_driver.get_all_versions(record)
    return JSONResponse(content=ret, status_code=200)


@router.put("/index/{record:path}/versions")
async def update_all_index_record_versions(record: str, request: Request):
    """
    Update metadata for all record versions.
    NOTE currently the only fields that can be updated for all versions are
    (`authz`, `acl`).
    """
    request_json = await request.json()
    try:
        jsonschema.validate(request_json, UPDATE_ALL_VERSIONS_SCHEMA)
    except jsonschema.ValidationError as err:
        logger.warning(f"Bad request body:\n{err}")
        raise UserError(err)
    acl = request_json.get("acl")
    authz = request_json.get("authz")
    ret = router.index_driver.update_all_versions(record, acl=acl, authz=authz)
    return JSONResponse(content=ret, status_code=200)


@router.get("/index/{record:path}/latest")
async def get_latest_index_record_versions(record: str, request: Request):
    """
    Get the latest record version
    """
    has_version = request.query_params.get("has_version", "").lower() == "true"
    ret = router.index_driver.get_latest_version(record, has_version=has_version)
    return JSONResponse(content=ret, status_code=200)


@router.get("/index/{record:path}")
async def get_index_record(record: str):
    """
    Returns a record.
    """
    ret = router.index_driver.get_with_nonstrict_prefix(record)
    return JSONResponse(content=ret, status_code=200)


@router.post("/index/")
async def post_index_record(request: Request):
    """
    Create a new record.
    """
    post_json = await request.json()
    try:
        jsonschema.validate(post_json, POST_RECORD_SCHEMA)
    except jsonschema.ValidationError as err:
        raise UserError(err)
    authz_val = post_json.get("authz", [])
    auth.authorize("create", authz_val)
    did = post_json.get("did")
    form = post_json["form"]
    size = post_json["size"]
    urls = post_json["urls"]
    acl = post_json.get("acl", [])
    hashes = post_json["hashes"]
    file_name = post_json.get("file_name")
    metadata = post_json.get("metadata")
    urls_metadata = post_json.get("urls_metadata")
    version = post_json.get("version")
    baseid = post_json.get("baseid")
    uploader = post_json.get("uploader")
    description = post_json.get("description")
    content_created_date = post_json.get("content_created_date")
    content_updated_date = post_json.get("content_updated_date")
    if content_updated_date is None:
        content_updated_date = content_created_date
    if content_updated_date is not None and content_created_date is None:
        raise UserError("Cannot set content_updated_date without content_created_date")
    if content_updated_date is not None and content_created_date is not None:
        if content_updated_date < content_created_date:
            raise UserError(
                "content_updated_date cannot come before content_created_date"
            )
    did, rev, baseid = router.index_driver.add(
        form,
        did,
        size=size,
        file_name=file_name,
        metadata=metadata,
        urls_metadata=urls_metadata,
        version=version,
        urls=urls,
        acl=acl,
        authz=authz_val,
        hashes=hashes,
        baseid=baseid,
        uploader=uploader,
        description=description,
        content_created_date=content_created_date,
        content_updated_date=content_updated_date,
    )
    ret = {"did": did, "rev": rev, "baseid": baseid}
    return JSONResponse(content=ret, status_code=200)


@router.post("/index/blank/")
async def post_index_blank_record(request: Request):
    """
    Create a blank new record with only uploader and optionally file_name fields filled
    """
    body = await request.json()
    uploader = body.get("uploader")
    file_name = body.get("file_name")
    authz = body.get("authz")
    did, rev, baseid = router.index_driver.add_blank_record(
        uploader=uploader, file_name=file_name, authz=authz
    )
    ret = {"did": did, "rev": rev, "baseid": baseid}
    return JSONResponse(content=ret, status_code=201)


@router.post("/index/blank/{record:path}")
async def add_index_blank_record_version(record: str, request: Request):
    """
    Create a new blank version of the record with this GUID.
    Authn/authz fields carry over from the previous version of the record.
    Only uploader and optionally file_name fields are filled.
    Returns the GUID of the new blank version and the baseid common to all versions of the record.
    """
    body = await request.json()
    new_did = body.get("did")
    uploader = body.get("uploader")
    file_name = body.get("file_name")
    authz = body.get("authz")
    did, baseid, rev = router.index_driver.add_blank_version(
        record, new_did=new_did, uploader=uploader, file_name=file_name, authz=authz
    )
    ret = {"did": did, "baseid": baseid, "rev": rev}
    return JSONResponse(content=ret, status_code=201)


@router.put("/index/blank/{record:path}")
async def put_index_blank_record(record: str, request: Request):
    """
    Update a blank record with size, hashes and url
    """
    rev = request.query_params.get("rev")
    body = await request.json()
    size = body.get("size")
    hashes = body.get("hashes")
    urls = body.get("urls")
    authz = body.get("authz")
    did, rev, baseid = router.index_driver.update_blank_record(
        did=record, rev=rev, size=size, hashes=hashes, urls=urls, authz=authz
    )
    ret = {"did": did, "rev": rev, "baseid": baseid}
    return JSONResponse(content=ret, status_code=200)


@router.put("/index/{record:path}")
async def put_index_record(record: str, request: Request):
    """
    Update an existing record.
    """
    put_json = await request.json()
    try:
        jsonschema.validate(put_json, PUT_RECORD_SCHEMA)
    except jsonschema.ValidationError as err:
        raise UserError(err)
    rev = request.query_params.get("rev")
    if (
        put_json.get("content_updated_date") is not None
        and put_json.get("content_created_date") is not None
    ):
        if put_json["content_updated_date"] < put_json["content_created_date"]:
            raise UserError(
                "content_updated_date cannot come before content_created_date"
            )
    did, baseid, rev = router.index_driver.update(record, rev, put_json)
    ret = {"did": did, "baseid": baseid, "rev": rev}
    return JSONResponse(content=ret, status_code=200)


@router.delete("/index/{record:path}")
async def delete_index_record(record: str, request: Request):
    """
    Delete an existing record.
    """
    rev = request.query_params.get("rev")
    if rev is None:
        raise UserError("No revision specified.")
    router.index_driver.delete(record, rev)
    return JSONResponse(content=None, status_code=200)


@router.post("/index/{record:path}")
async def add_index_record_version(record: str, request: Request):
    """
    Add a record version
    """
    post_json = await request.json()
    try:
        jsonschema.validate(post_json, POST_RECORD_SCHEMA)
    except jsonschema.ValidationError as err:
        raise UserError(err)
    new_did = post_json.get("did")
    form = post_json["form"]
    size = post_json["size"]
    urls = post_json["urls"]
    acl = post_json.get("acl", [])
    authz = post_json.get("authz", [])
    hashes = post_json["hashes"]
    file_name = post_json.get("file_name")
    metadata = post_json.get("metadata")
    urls_metadata = post_json.get("urls_metadata")
    version = post_json.get("version")
    description = post_json.get("description")
    content_created_date = post_json.get("content_created_date")
    content_updated_date = post_json.get("content_updated_date")
    if content_updated_date is None:
        content_updated_date = content_created_date
    if content_updated_date is not None and content_created_date is not None:
        if content_updated_date < content_created_date:
            raise UserError(
                "content_updated_date cannot come before content_created_date"
            )
    did, baseid, rev = router.index_driver.add_version(
        record,
        form,
        new_did=new_did,
        size=size,
        urls=urls,
        acl=acl,
        authz=authz,
        file_name=file_name,
        metadata=metadata,
        urls_metadata=urls_metadata,
        version=version,
        hashes=hashes,
        description=description,
        content_created_date=content_created_date,
        content_updated_date=content_updated_date,
    )
    ret = {"did": did, "baseid": baseid, "rev": rev}
    return JSONResponse(content=ret, status_code=200)


@router.get("/_dist")
async def get_dist_config():
    """
    Returns the dist configuration
    """
    return JSONResponse(content=router.dist, status_code=200)


@router.get("/_status")
async def health_check():
    """
    Health Check.
    """
    router.index_driver.health_check()
    return JSONResponse(content="Healthy", status_code=200)


@router.get("/_stats")
async def stats():
    """
    Return indexed data stats.
    """
    filecount = router.index_driver.len()
    totalfilesize = router.index_driver.totalbytes()
    base = {"fileCount": filecount, "totalFileSize": totalfilesize}
    return JSONResponse(content=base, status_code=200)


@router.get("/_version")
async def version():
    """
    Return the version of this service.
    """
    base = {"version": VERSION, "commit": COMMIT}
    return JSONResponse(content=base, status_code=200)


def get_checksum(data):
    """
    Collect checksums from bundles and objects in the bundle for compute_checksum
    """
    if "hashes" in data:
        return data["hashes"][list(data["hashes"])[0]]()
    elif "checksums" in data:
        return data["checksums"][0]["checksum"]
    elif "checksum" in data:
        return data["checksum"]


def compute_checksum(checksums):
    """
    Checksum created by sorting alphabetically then concatenating first layer of bundles/objects.

    Args:
        checksums (list): list of checksums from the first layer of bundles and objects

    Returns:
        md5 checksum
    """
    checksums.sort()
    checksum = "".join(checksums)
    return {
        "checksum": hashlib.md5(
            checksum.encode("utf-8"), usedforsecurity=False
        ).hexdigest(),
        "type": "md5",
    }


@router.post("/bundle/")
async def post_bundle(request: Request):
    """
    Create a new bundle
    """
    auth.authorize("create", ["/services/indexd/bundles"])
    post_json = await request.json()
    try:
        jsonschema.validate(post_json, BUNDLE_SCHEMA)
    except jsonschema.ValidationError as err:
        raise UserError(err)
    name = post_json.get("name")
    bundles = post_json.get("bundles")
    bundle_id = post_json.get("bundle_id")
    size = post_json.get("size") if post_json.get("size") else 0
    description = post_json.get("description", "")
    version = post_json.get("version", "")
    aliases = post_json.get("aliases", [])
    if not bundles or len(bundles) == 0:
        raise UserError("Bundle data required.")
    if len(bundles) != len(set(bundles)):
        raise UserError("Duplicate GUID in bundles.")
    if bundle_id in bundles:
        raise UserError("Bundle refers to itself.")
    bundle_data = []
    checksums = []

    # TODO: Remove this after updating to jsonschema>=3.0.0
    if post_json.get("checksums"):
        hashes = {
            checksum["type"]: checksum["checksum"]
            for checksum in post_json.get("checksums")
        }
        validate_hashes(**hashes)
    for bundle in bundles:
        data = router.index_driver.get_with_nonstrict_prefix(bundle)
        size += data["size"] if not post_json.get("size") else 0
        checksums.append(get_checksum(data))
        data = bundle_to_drs(data, expand=True, is_content=True)
        bundle_data.append(data)
    checksum = (
        post_json.get("checksums")
        if post_json.get("checksums")
        else [compute_checksum(checksums)]
    )
    ret = router.index_driver.add_bundle(
        bundle_id=bundle_id,
        name=name,
        size=size,
        bundle_data=json.dumps(bundle_data),
        checksum=json.dumps(checksum),
        description=description,
        version=version,
        aliases=json.dumps(aliases),
    )
    return JSONResponse(
        content={"bundle_id": ret[0], "name": ret[1], "contents": ret[2]},
        status_code=200,
    )


@router.get("/bundle/")
async def get_bundle_record_list(request: Request):
    """
    Returns a list of bundle records.
    """
    form = (
        request.query_params.get("form")
        if request.query_params.get("form")
        else "bundle"
    )
    return await get_index(request, form=form)


@router.get("/bundle/{bundle_id:path}")
async def get_bundle_record_with_id(bundle_id: str, request: Request):
    """
    Returns a record given bundle_id
    """
    expand = request.query_params.get("expand") == "true"
    ret = router.index_driver.get_with_nonstrict_prefix(bundle_id)
    ret = bundle_to_drs(ret, expand=expand, is_content=False)
    return JSONResponse(content=ret, status_code=200)


@router.delete("/bundle/{bundle_id:path}")
async def delete_bundle_record(bundle_id: str):
    """
    Delete bundle record given bundle_id
    """
    auth.authorize("delete", ["/services/indexd/bundles"])
    router.index_driver.delete_bundle(bundle_id)
    return JSONResponse(content=None, status_code=200)
