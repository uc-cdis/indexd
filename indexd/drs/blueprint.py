import os
import re
import flask
import json
import copy
from indexd.bulk.blueprint import bulk_get_documents
from cdislogging import get_logger
from indexd.errors import AuthError, AuthzError
from indexd.errors import UserError
from indexd.index.errors import NoRecordFound as IndexNoRecordFound
from indexd.errors import IndexdUnexpectedError
from indexd.utils import reverse_url
from flask import current_app as app
from indexd.utils import reverse_url, lookup_bucket_region, get_bucket_regions
from flask import current_app as app

logger = get_logger(__name__)

blueprint = flask.Blueprint("drs", __name__)

blueprint.config = dict()
blueprint.index_driver = None
blueprint.service_info = {}
blueprint.cloud_provider_map = {}
blueprint.max_bulk_request_length = 100


@blueprint.route("/ga4gh/drs/v1/service-info", methods=["GET"])
def get_drs_service_info():
    """
    Returns DRS 1.5 compliant service information
    """

    reverse_domain_name = reverse_url(url=os.environ["HOSTNAME"])

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
            "url": "https://" + os.environ["HOSTNAME"],
        },
    }

    # Merge config overrides (e.g. from DRS_SERVICE_INFO env var)
    if blueprint.service_info:
        for key, value in blueprint.service_info.items():
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
        object_count, total_object_size = blueprint.index_driver.get_stats()
    except Exception as e:
        logger.warning(f"Could not retrieve stats for service-info response: {e}")

    # Build drs sub-object
    max_bulk = blueprint.max_bulk_request_length

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

    return flask.jsonify(ret), 200


@blueprint.route(
    "/ga4gh/drs/v1/objects/<path:object_id>",
    methods=["GET"],
    provide_automatic_options=False,
)
def get_drs_object(object_id):
    """
    Returns a specific DRSobject with object_id
    """
    expand = True if flask.request.args.get("expand") == "true" else False

    ret = blueprint.index_driver.get_with_nonstrict_prefix(object_id)

    data = indexd_to_drs(ret, expand=expand)

    return flask.jsonify(data), 200


@blueprint.route("/ga4gh/drs/v1/objects/<path:object_id>", methods=["OPTIONS"])
def get_drs_object_options(object_id):
    """
    Returns a specific DRSobject metadata with object_id
    """
    # Get authz based on guid
    try:
        ret = blueprint.index_driver.get_with_nonstrict_prefix(object_id)
        authz = ret["authz"][0]
    # Handle known type error
    except IndexNoRecordFound as err:
        return handle_no_index_record_error(err)

    # Get static authz metadata based on blueprint
    try:
        # Get static authz metadata
        authz_metadata = copy.deepcopy(blueprint.drs_authorization_metadata)
        # Otherwise, match exists & we can updated with object id
        authz_metadata[authz].update({"drs_object_id": object_id})
        return flask.jsonify(authz_metadata[authz]), 200
    # Otherwise catch unknown error
    except Exception as err:
        return handle_unexpected_error(err)


@blueprint.route(
    "/ga4gh/drs/v1/objects", methods=["GET"], provide_automatic_options=False
)
def list_drs_records():
    limit = flask.request.args.get("limit")
    start = flask.request.args.get("start")
    page = flask.request.args.get("page")

    form = flask.request.args.get("form")

    try:
        limit = 100 if limit is None else int(limit)
    except ValueError as err:
        raise UserError("limit must be an integer")

    if limit < 0 or limit > 1024:
        raise UserError("limit must be between 0 and 1024")

    if page is not None:
        try:
            page = int(page)
        except ValueError as err:
            raise UserError("page must be an integer")

    if form == "bundle":
        records = blueprint.index_driver.get_bundle_list(
            start=start, limit=limit, page=page
        )
    elif form == "object":
        records = blueprint.index_driver.ids(start=start, limit=limit, page=page)
    else:
        records = blueprint.index_driver.get_bundle_and_object_list(
            start=start, limit=limit, page=page
        )
    ret = {
        "drs_objects": [indexd_to_drs(record, True) for record in records],
    }

    return flask.jsonify(ret), 200


@blueprint.route("/ga4gh/drs/v1/objects", methods=["OPTIONS"])
def list_drs_records_options():
    """Returns OPTIONS metadata for each provided DRS object id (drs object id = did)

    dids: list of str object ids (ex. ['123','456'])

    A response for a call with 6 dids where 2 were successfully resolved, 2 were not found,
    and 2 encountered an unexpected error would look like:

    {
        "summary": {
            "requested": 6,
            "resolved": 2,
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
            ],
    }

    A malformed call (i.e. providing no did list) would result in a 400 response:
    {'msg': 'Request is malformed. Missing bulk object ids.', 'status_code': 400}
    """

    # Get data from json body
    data = flask.request.get_json(force=True)

    # Exit with malformed error return if missing object id key
    if "bulk_object_ids" not in data:
        return handle_user_error("Request is malformed. Missing bulk object ids.")

    # Return unexpected error if unhandled issue encountered...
    try:
        # Prepare return defaults
        total_requested = len(data["bulk_object_ids"])
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
        id_list = data["bulk_object_ids"]
        docs = blueprint.index_driver.get_bulk(id_list)
        doc_dids = [doc["did"] for doc in docs]

        # Annotate if an original id(s) is not returned in bulk call (record as unresolved, index not found)
        for i in id_list:
            if i not in doc_dids:
                missing_error_guids.append(i)
        # Check the authz for each returned object:
        resolved_count = 0
        for doc in docs:
            # Get static authz metadata and confirm info matches
            authz = doc["authz"][0]
            authz_metadata = copy.deepcopy(blueprint.drs_authorization_metadata)
            # If static match not confirmed, record as unexpected error & continue to next
            if authz not in authz_metadata.keys():
                unexpected_error_guids.append(doc["did"])
                continue
            # otherwise update with object id and save info for return
            authz_metadata[authz].update({"drs_object_id": doc["did"]})
            resolved_drs_objects.append(authz_metadata[authz])
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

    # If unexpected error encountered, return defaults
    except Exception as err:
        return handle_unexpected_error(err)

    return flask.jsonify(compiled_info), 200


def create_drs_uri(did):
    """
    Return ga4gh-compilant drs format uri

    Args:
        did(str): did of drs object
    """

    default_prefix = blueprint.index_driver.config.get("DEFAULT_PREFIX")

    if not default_prefix:
        # For env without DEFAULT_PREFIX, uri will not be drs compliant
        accession = did
        self_uri = "drs://{}".format(accession)
    else:
        accession = (
            did.replace(default_prefix, "", 1).replace("/", "", 1).replace(":", "", 1)
        )

        self_uri = "drs://{}:{}".format(
            default_prefix.replace("/", "", 1).replace(":", "", 1), accession
        )

    return self_uri


def indexd_to_drs(record, expand=False):
    """
    Convert record to ga4gh-compilant format

    Args:
        record(dict): json object record
        expand(bool): show contents of the descendants
    """

    did = (
        record["id"]
        if "id" in record
        else record["did"] if "did" in record else record["bundle_id"]
    )

    self_uri = create_drs_uri(did)

    name = record["file_name"] if "file_name" in record else record["name"]

    index_created_time = (
        record["created_date"] if "created_date" in record else record["created_time"]
    )

    version = (
        record["version"]
        if "version" in record
        else record["rev"] if "rev" in record else ""
    )

    index_updated_time = (
        record["updated_date"] if "updated_date" in record else record["updated_time"]
    )

    content_created_date = record.get("content_created_date", "")

    content_updated_date = record.get("content_updated_date", "")

    form = record["form"] if "form" in record else "bundle"

    description = record["description"] if "description" in record else None

    alias = (
        record["alias"]
        if "alias" in record
        else json.loads(record["aliases"]) if "aliases" in record else []
    )

    bucket_regions = get_bucket_regions()

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

    drs_object = {
        "id": did,
        "mime_type": "application/json",
        "name": name,
        "index_created_time": index_created_time,
        "index_updated_time": index_updated_time,
        "created_time": content_created_date,
        "updated_time": content_updated_date,
        "size": record["size"],
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
            location_type = location.split(":")[
                0
            ]  # (s3, gs, ftp, gsiftp, globus, htsget, https, file)
            cloud = blueprint.cloud_provider_map.get(location_type)

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

    # parse out checksums
    drs_object["checksums"] = parse_checksums(record, drs_object)

    return drs_object


def bundle_to_drs(record, expand=False, is_content=False):
    """
    record(dict): json object record
    expand(bool): show contents of the descendants
    is_content: is an expanded content in a bundle
    """

    did = (
        record["id"]
        if "id" in record
        else record["did"] if "did" in record else record["bundle_id"]
    )

    drs_uri = create_drs_uri(did)

    name = record["file_name"] if "file_name" in record else record["name"]

    drs_object = {
        "id": did,
        "name": name,
        "drs_uri": drs_uri,
        "contents": [],
    }

    contents = (
        record["contents"]
        if "contents" in record
        else record["bundle_data"] if "bundle_data" in record else []
    )

    if not expand and isinstance(contents, list):
        for content in contents:
            if isinstance(content, dict):
                content.pop("contents", None)

    drs_object["contents"] = contents

    if not is_content:
        # Show these only if its the leading bundle
        description = record["description"] if "description" in record else ""
        aliases = (
            record["alias"]
            if "alias" in record
            else json.loads(record["aliases"]) if "aliases" in record else []
        )
        version = (
            record["version"]
            if "version" in record
            else record["rev"] if "rev" in record else ""
        )
        # version = record["version"] if "version" in record else ""
        drs_object["checksums"] = parse_checksums(record, drs_object)

        created_time = (
            record["created_date"]
            if "created_date" in record
            else record.get("created_time")
        )

        updated_time = (
            record["updated_date"]
            if "updated_date" in record
            else record.get("updated_time")
        )
        if created_time:
            drs_object["created_time"] = created_time
        if updated_time:
            drs_object["updated_time"] = updated_time
        drs_object["size"] = record["size"]
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


@blueprint.errorhandler(UserError)
def handle_user_error(err):
    ret = {"msg": str(err), "status_code": 400}
    return flask.jsonify(ret), 400


@blueprint.errorhandler(AuthzError)
def handle_authz_error(err):
    ret = {"msg": str(err), "status_code": 401}
    return flask.jsonify(ret), 401


@blueprint.errorhandler(AuthError)
def handle_requester_auth_error(err):
    ret = {"msg": str(err), "status_code": 403}
    return flask.jsonify(ret), 403


@blueprint.errorhandler(IndexNoRecordFound)
def handle_no_index_record_error(err):
    ret = {"msg": str(err), "status_code": 404}
    return flask.jsonify(ret), 404


@blueprint.errorhandler(IndexdUnexpectedError)
def handle_unexpected_error(err):
    ret = {"msg": err.message, "status_code": err.code}
    return flask.jsonify(ret), err.code


@blueprint.record
def get_config(setup_state):
    index_config = setup_state.app.config["INDEX"]
    blueprint.index_driver = index_config["driver"]
    if "DRS_SERVICE_INFO" in setup_state.app.config:
        blueprint.service_info = setup_state.app.config["DRS_SERVICE_INFO"]
    if "DRS_AUTHORIZATION_METADATA" in setup_state.app.config:
        blueprint.drs_authorization_metadata = setup_state.app.config[
            "DRS_AUTHORIZATION_METADATA"
        ]
    if "CLOUD_PROVIDER_MAP" in setup_state.app.config:
        blueprint.cloud_provider_map = setup_state.app.config["CLOUD_PROVIDER_MAP"]


@blueprint.record
def get_bulk_config(setup_state):
    blueprint.max_bulk_request_length = setup_state.app.config.get(
        "MAX_BULK_REQUEST_LENGTH", 100
    )
