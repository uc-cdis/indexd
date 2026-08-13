import os
from urllib.parse import urlparse

import copy
import json
from cdislogging import get_logger
from fastapi import APIRouter, Request, HTTPException, status
from fastapi.responses import JSONResponse

from indexd.errors import UserError, IndexdUnexpectedError
from indexd.index.errors import NoRecordFound as IndexNoRecordFound
from indexd.utils import reverse_url, get_bucket_regions, lookup_bucket_region

router = APIRouter(tags=["drs"])

router.index_driver = None
router.service_info = {}
router.bucket_regions = {}
logger = get_logger(__name__)


def set_drs_config(app):
    index_config = app.settings["config"]["INDEX"]
    router.index_driver = index_config["driver"]
    router.service_info = app.settings.get("DRS_SERVICE_INFO", {})
    router.default_passport_issuer = None
    router.default_bearer_issuer = None
    if "DRS_SERVICE_INFO" in app.settings["config"]:
        router.service_info = app.settings["config"]["DRS_SERVICE_INFO"]
    if "DRS_AUTHORIZATION_METADATA" in app.settings["config"]:
        router.drs_authorization_metadata = app.settings["config"][
            "DRS_AUTHORIZATION_METADATA"
        ]
    else:
        logger.warning(
            "DRS_AUTHORIZATION_METADATA not configured. Unable to provide authorization metadata for acces_methods in DrsObjects!!!"
        )

    if "DEFAULT_BEARER_ISSUER" in app.settings["config"]:
        router.default_bearer_issuer = app.settings["config"]["DEFAULT_BEARER_ISSUER"]
    else:
        logger.warning(
            "No default bearer issuer configured. This should be set to the Fence token issuer!!!"
        )
    if "DEFAULT_PASSPORT_ISSUER" in app.settings["config"]:
        router.default_passport_issuer = app.settings["config"][
            "DEFAULT_PASSPORT_ISSUER"
        ]
    if "CLOUD_PROVIDER_MAP" in app.settings["config"]:
        router.cloud_provider_map = app.settings["config"]["CLOUD_PROVIDER_MAP"]
    else:
        logger.warning(
            "CLOUD_PROVIDER_MAP not configured. Unable to derive cloud providers from URLs"
        )
    if "DEFAULT_PREFERRED_TYPE" in app.settings["config"]:
        router.default_preferred_type = app.settings["config"]["DEFAULT_PREFERRED_TYPE"]
    else:
        router.default_preferred_type = "BearerAuth"
        logger.warning(
            "DEFAULT_PREFERRED_TYPE not configured. Defaulting to BearerAuth as the preferred supported_type"
        )

    router.max_bulk_request_length = app.settings["config"].get(
        "MAX_BULK_REQUEST_LENGTH", 100
    )

    router.bucket_regions = get_bucket_regions(app)


@router.get("/ga4gh/drs/v1/service-info")
async def get_drs_service_info():
    """
    Returns DRS 1.5 compliant service information
    """

    reverse_domain_name = reverse_url(url=os.environ.get("HOSTNAME"))
    ret = {
        "id": reverse_domain_name,
        "name": "DRS System",
        "version": "1.5.0",
        "type": {
            "group": "org.ga4gh",
            "artifact": "drs",
            "version": "1.5.0",
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
        # Fetch stats from stats table
    object_count = None
    total_object_size = None
    try:
        object_count, total_object_size = await router.index_driver.get_stats()
    except Exception as e:
        logger.warning(f"Could not retrieve stats for service-info response: {e}")

    # Build drs sub-object
    max_bulk = router.max_bulk_request_length

    drs_info = {
        "maxBulkRequestLength": max_bulk,
    }
    if object_count is not None:
        drs_info["objectCount"] = object_count
    if total_object_size is not None:
        drs_info["totalObjectSize"] = total_object_size

    ret["drs"] = drs_info

    # Backward compat: root-level maxBulkRequestLength (deprecated in DRS 1.5)
    ret["maxBulkRequestLength"] = max_bulk

    return JSONResponse(content=ret, status_code=200)


@router.get("/ga4gh/drs/v1/objects/{object_id:path}")
async def get_drs_object(object_id: str, request: Request):
    """
    Returns a specific DRSobject with object_id
    """

    expand = request.query_params.get("expand") == "true"
    try:
        ret = await router.index_driver.get_with_nonstrict_prefix(object_id)
    except IndexNoRecordFound as err:
        raise HTTPException(status_code=404, detail=str(err))

    data = await indexd_to_drs(ret, expand=expand)
    return JSONResponse(content=data, status_code=200)


@router.post("/ga4gh/drs/v1/objects/{object_id:path}")
async def post_drs_object(object_id):
    """
    Returns passport-authenticated DRS object retrieval with object_id.
    Not yet supported.
    """
    message = "Passport-authenticated DRS object retrieval is not yet supported."
    return JSONResponse(content={"msg": message}, status_code=405)


@router.options("/ga4gh/drs/v1/objects/{object_id:path}")
async def get_drs_object_options(object_id):
    """
    Returns a specific DRSobject metadata with object_id
    """
    # Get authz based on guid
    authz_metadata = await resolve_single_object_auth(object_id)
    return JSONResponse(content=authz_metadata, status_code=200)


@router.get("/ga4gh/drs/v1/objects")
@router.get("/ga4gh/drs/v1/objects/")
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
        records = await router.index_driver.get_bundle_list(
            start=start, limit=limit, page=page
        )
    elif form == "object":
        records = await router.index_driver.ids(start=start, limit=limit, page=page)
    else:
        records = await router.index_driver.get_bundle_and_object_list(
            start=start, limit=limit, page=page
        )

    ret_drs_objects = []
    for record in records:
        ret_drs_objects.append(await indexd_to_drs(record, True))

    ret = {
        "drs_objects": ret_drs_objects,
    }
    return JSONResponse(content=ret, status_code=200)


@router.post("/ga4gh/drs/v1/objects")
async def post_drs_records(request: Request):
    """Returns DRS objects for each provided DRS object id.
    Expects 'bulk_object_ids' in request body"""
    data = await request.json()
    # Exit with malformed error return if missing object id
    if "bulk_object_ids" not in data:
        raise UserError("Request is malformed. Missing bulk object ids.")
    ret = await resolve_bulk_object_auth(
        id_list=data["bulk_object_ids"], auth_only=False
    )
    return JSONResponse(content=ret, status_code=200)


@router.post("/ga4gh/drs/v1/objects")
async def get_drs_objects(request: Request):
    """Returns DRS objects for each provided DRS object id.
    Expects 'bulk_object_ids' in request body"""
    data = await request.json()
    # Exit with malformed error return if missing object id
    if "bulk_object_ids" not in data:
        raise UserError("Request is malformed. Missing bulk object ids.")
    ret = await resolve_bulk_object_auth(id_list=data["bulk_object_ids"])
    return JSONResponse(content=ret, status_code=200)


@router.options("/ga4gh/drs/v1/objects")
async def list_drs_records_options(request: Request):
    """Returns OPTIONS metadata for each provided DRS object id (drs object id = did)

    dids: list of str object ids (ex. ['123','456'])

    A response for a call with 5 dids where 3 were successfully resolved, 2 were not found,
    and 2 encountered an unexpected error would look like:

    {
        "summary": {
            "requested": 5,
            "resolved": 3,
            "unresolved": 4,
        },
        "unresolved_drs_objects": [
                {"error_code": 404, "object_ids": [did3, did4]},
                {"error_code": 500, "object_ids": [did5, did6]}
            ],
        "resolved_drs_objects": [
                {
                    "drs_object_id": "did1",
                    "bearer_auth_issuers": ["sample"],
                    "passport_auth_issuers": ["sample"],
                    "supported_types": ["BearerAuth", "PassportAuth"]
                },
                {
                    "drs_object_id": "did2",
                    "bearer_auth_issuers": ["sample"],
                    "passport_auth_issuers": ["sample"],
                    "supported_types": ["BearerAuth", "PassportAuth"]
                },
                {
                    "drs_object_id": "did3",
                    "bearer_auth_issuers": [],
                    "passport_auth_issuers": [],
                    "supported_types": []
                },
            ],
    }

    A malformed call (i.e. providing no did list) would result in a 400 response:
    {'msg': 'Request is malformed. Missing bulk object ids.', 'status_code': 400}
    """

    # Get data from json body
    data = await request.json()

    # Exit with malformed error return if missing object id key
    if "bulk_object_ids" not in data:
        raise UserError("Request is malformed. Missing bulk object ids.")

    try:
        compiled_info = await resolve_bulk_object_auth(id_list=data["bulk_object_ids"])

    # If unexpected error encountered, return defaults
    except Exception as err:
        raise IndexdUnexpectedError(err)

    return JSONResponse(content=compiled_info, status_code=200)


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


async def resolve_single_object_auth(object_id: str) -> dict:
    """Returns dict with object's authorization metadata"""

    # Extract authz metadata for object id
    try:
        ret = await router.index_driver.get_with_nonstrict_prefix(object_id)
        authz_path_list = ret["authz"]
        authz_metadata = copy.deepcopy(router.drs_authorization_metadata)

        preferred_type = router.default_preferred_type
        # Define default (empty) metadata details to return
        compiled_metadata_details = {
            "drs_object_id": object_id,
            "supported_types": [],
            "bearer_auth_issuers": [],
            "passport_auth_issuers": [],
        }

        # If index driver found no object auth path info, return empty authz data
        if not authz_path_list:
            return compiled_metadata_details

        # If auth path is for open project, just return default auth info
        # Note: if multiple paths exists and one is an open project, only default info is gserviceaccount
        if any(["/open" in path for path in authz_path_list]):
            compiled_metadata_details["supported_types"] = ["None"]
            return compiled_metadata_details

        # Extract & compile auth metadata details (for each path)
        compiled_passport_auth_issuers = set()
        compiled_bearer_auth_issuers = set()
        for authz in authz_path_list:
            authz_metadata_details = authz_metadata.get(authz, {})
            # Compile passport issuer list and remove duplicates
            if "passport_auth_issuers" in authz_metadata_details:
                compiled_passport_auth_issuers.update(
                    authz_metadata_details["passport_auth_issuers"]
                )
            elif router.default_passport_issuer:
                compiled_passport_auth_issuers.add(router.default_passport_issuer)

            # Compile bearer issuer list and remove duplicates
            if "bearer_auth_issuers" in authz_metadata_details:
                compiled_bearer_auth_issuers.update(
                    authz_metadata_details["bearer_auth_issuers"]
                )
            elif router.default_bearer_issuer:
                compiled_bearer_auth_issuers.add(router.default_bearer_issuer)
            else:
                logger.warning(
                    "Unable to determine bearer issuer - this should be configured to Fence's token issuer or in trustedIssuers!!!"
                )
            # Update issuer info
            compiled_metadata_details["passport_auth_issuers"] = sorted(
                compiled_passport_auth_issuers
            )
            compiled_metadata_details["bearer_auth_issuers"] = sorted(
                compiled_bearer_auth_issuers
            )
            if "preferred_type" in authz_metadata_details:
                preferred_type = authz_metadata_details["preferred_type"]

        # Update supported_types
        compiled_supported_types = []
        if preferred_type == "PassportAuth":
            if compiled_passport_auth_issuers:
                compiled_supported_types.append("PassportAuth")
            if compiled_bearer_auth_issuers:
                compiled_supported_types.append("BearerAuth")
        else:
            if compiled_bearer_auth_issuers:
                compiled_supported_types.append("BearerAuth")
            if compiled_passport_auth_issuers:
                compiled_supported_types.append("PassportAuth")

        compiled_metadata_details["supported_types"] = compiled_supported_types
        return compiled_metadata_details
    except IndexNoRecordFound as err:
        raise IndexNoRecordFound(err)
    except Exception as err:
        raise IndexdUnexpectedError(err)


async def resolve_bulk_object_auth(id_list: list[str], auth_only=True) -> dict:
    """Returns compiled dict of authorization metadata
    auth_only = True # defaults to only return resolved authorization
    auth_only = False # returned resolves drs object info (auth included)"""
    # Return unexpected error if unhandled issue encountered...
    # Prepare return defaults
    total_requested = len(id_list)
    unresolved_drs_objects = []
    resolved_drs_objects = []
    missing_error_guids = []  # 404
    unexpected_error_guids = []  # 500
    summary = {
        "requested": total_requested,
        "resolved": 0,
        "unresolved": total_requested,  # nothing is resolved at the start
    }
    # Bulk retrieve docs from id list
    docs = await router.index_driver.get_bulk(id_list)
    doc_dids = [doc["did"] for doc in docs]
    # Annotate if an original id(s) is not returned in bulk call (record as unresolved, index not found)
    for i in id_list:
        if i not in doc_dids:
            missing_error_guids.append(i)
    # Check the authz for each returned object:
    resolved_count = 0
    for doc in docs:
        # Resolve individual
        guid = doc["did"]
        if guid in missing_error_guids:
            continue
        try:
            # If index not found error occurs here, it will be caught as an unexpected error
            # becuase we already checked for missing guids before the try block. Any issue
            # encountered is likely not solely tied to an index-not-found issue.
            if auth_only:
                resolved_info = await resolve_single_object_auth(object_id=guid)
            else:
                resolved_info = await indexd_to_drs(record=doc)
        # Handle unexpected error and continue
        except Exception as err:
            unexpected_error_guids.append(guid)
            continue
        # If not unexpected error found, but we know auth is None, treat as 500
        if resolved_info is None:
            unexpected_error_guids.append(guid)
            continue
        resolved_drs_objects.append(resolved_info)
        resolved_count = resolved_count + 1

    # Update summary counts
    summary["resolved"] = resolved_count
    summary["unresolved"] = total_requested - resolved_count
    # Update unresolved list details
    if len(missing_error_guids) > 0:
        unresolved_drs_objects.append(
            {"error_code": 404, "object_ids": sorted(missing_error_guids)}
        )
    if len(unexpected_error_guids) > 0:
        unresolved_drs_objects.append(
            {"error_code": 500, "object_ids": sorted(unexpected_error_guids)}
        )
    # Update compiled results
    compiled_info = {}
    compiled_info["summary"] = summary
    compiled_info["unresolved_drs_objects"] = unresolved_drs_objects
    compiled_info["resolved_drs_objects"] = resolved_drs_objects
    return compiled_info


async def indexd_to_drs(record, expand=False):
    """
    Convert record to ga4gh-compilant format. Includes access_methods resolution.

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

    # Define current drs_object dict description
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
    # Get access method dict for each url
    bucket_regions = router.bucket_regions

    region = {}
    urls_metadata = record.get("urls_metadata", {})
    for url, meta in urls_metadata.items():
        if isinstance(meta, dict) and meta.get("region"):
            region[url] = meta["region"]

    if "urls" in record and record["urls"]:
        for url in record["urls"]:
            if url.startswith("s3://") and url not in region:
                bucket_name = url.split("/")[2]
                matched_region = lookup_bucket_region(bucket_name, bucket_regions)
                if matched_region:
                    region[url] = matched_region

    available = {}

    for url, url_meta in record.get("urls_metadata", {}).items():
        if isinstance(url_meta, dict) and "available" in url_meta:
            value = url_meta["available"]
            if isinstance(value, bool):
                available[url] = value
            elif isinstance(value, str):
                available[url] = value.lower() == "true"
            else:
                available[url] = bool(value)
        else:
            available[url] = True
    if "bundle_data" in record:
        drs_object["contents"] = []
        for bundle in record["bundle_data"]:
            bundle_object = bundle_to_drs(bundle, expand=expand, is_content=True)
            if not expand:
                bundle_object.pop("contents", None)
            drs_object["contents"].append(bundle_object)

    if "urls" in record:
        # Add access_method key-value pair (required to append url info)
        if "access_methods" not in drs_object:
            drs_object["access_methods"] = []
        for location in record["urls"]:
            location_type = location.split(":")[
                0
            ]  # (s3, gs, ftp, gsiftp, globus, htsget, https, file)
            cloud = get_cloud_provider(location)

            drs_object["access_methods"].append(
                {
                    "type": location_type,
                    "cloud": cloud,
                    "access_url": {"url": location},
                    "access_id": location_type,
                    "available": available.get(location, True),
                    "region": region.get(location, ""),
                }
            )
    # Add authorization metadata to access_methods if record object id NOT a bundle
    # AND drs_object['access_method'] is populated with an access url
    # Auth metadata is optional for bundles
    if form == "object" and drs_object["access_methods"] != []:
        for entry in drs_object["access_methods"]:
            # Take no action and continue to next if access_url missing
            if "access_url" not in entry:
                continue
            # Otherwise add auth info in entry
            did = record["did"]
            authorizations = await resolve_single_object_auth(object_id=did)
            entry.update({"authorizations": authorizations})
    # Parse out checksums
    drs_object["checksums"] = parse_checksums(record, drs_object)

    return drs_object


def get_cloud_provider(location):
    location_type = location.split(":")[0]
    value = router.cloud_provider_map.get(location_type)

    if isinstance(value, str) and value:
        return value

    elif isinstance(value, dict):
        parsed = urlparse(location)
        location_key = f"{parsed.netloc}{parsed.path}"

        for prefix, provider in value.items():
            if location_key.startswith(prefix):
                return provider

    logger.warning(
        f"Unable to determine cloud provider for {location} "
        f"from CLOUD_PROVIDER_MAP. Setting to None"
    )
    return None


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
