import re
import json
import flask
import jsonschema
import os.path
import subprocess
from .version_data import VERSION, COMMIT

from indexd.auth import authorize

from indexd.errors import AuthError
from indexd.errors import UserError

from .schema import PUT_RECORD_SCHEMA
from .schema import POST_RECORD_SCHEMA
from .schema import RECORD_ALIAS_SCHEMA

from .errors import NoRecordFound
from .errors import MultipleRecordsFound
from .errors import RevisionMismatch
from .errors import UnhealthyCheck

from cdislogging import get_logger

logger = get_logger("indexd/index blueprint", log_level="info")

blueprint = flask.Blueprint("index", __name__)

blueprint.config = dict()
blueprint.index_driver = None

ACCEPTABLE_HASHES = {
    "md5": re.compile(r"^[0-9a-f]{32}$").match,
    "sha1": re.compile(r"^[0-9a-f]{40}$").match,
    "sha256": re.compile(r"^[0-9a-f]{64}$").match,
    "sha512": re.compile(r"^[0-9a-f]{128}$").match,
    "crc": re.compile(r"^[0-9a-f]{8}$").match,
    "etag": re.compile(r"^[0-9a-f]{32}(-\d+)?$").match,
}


def validate_hashes(**hashes):
    """
    Validate hashes against known and valid hashing algorithms.
    """
    if not all(h in ACCEPTABLE_HASHES for h in hashes):
        raise UserError("invalid hash types specified")

    if not all(ACCEPTABLE_HASHES[h](v) for h, v in hashes.items()):
        raise UserError("invalid hash values specified")


@blueprint.route("/index/", methods=["GET"])
def get_index():
    """
    Returns a list of records.
    """
    limit = flask.request.args.get("limit")
    start = flask.request.args.get("start")
    page = flask.request.args.get("page")

    ids = flask.request.args.get("ids")
    if ids:
        ids = ids.split(",")
        if start is not None or limit is not None or page is not None:
            raise UserError("pagination is not supported when ids is provided")
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

    size = flask.request.args.get("size")
    try:
        size = size if size is None else int(size)
    except ValueError as err:
        raise UserError("size must be an integer")

    if size is not None and size < 0:
        raise UserError("size must be > 0")

    uploader = flask.request.args.get("uploader")

    # TODO: Based on indexclient, url here should be urls instead. Or change urls to url in indexclient.
    urls = flask.request.args.getlist("url")

    file_name = flask.request.args.get("file_name")

    version = flask.request.args.get("version")

    hashes = flask.request.args.getlist("hash")
    hashes = {h: v for h, v in (x.split(":", 1) for x in hashes)}

    validate_hashes(**hashes)
    hashes = hashes if hashes else None

    metadata = flask.request.args.getlist("metadata")
    metadata = {k: v for k, v in (x.split(":", 1) for x in metadata)}

    acl = flask.request.args.get("acl")
    if acl is not None:
        acl = [] if acl == "null" else acl.split(",")

    authz = flask.request.args.get("authz")
    if authz is not None:
        authz = [] if authz == "null" else authz.split(",")

    urls_metadata = flask.request.args.get("urls_metadata")
    if urls_metadata:
        try:
            urls_metadata = json.loads(urls_metadata)
        except ValueError:
            raise UserError("urls_metadata must be a valid json string")

    negate_params = flask.request.args.get("negate_params")
    if negate_params:
        try:
            negate_params = json.loads(negate_params)
        except ValueError:
            raise UserError("negate_params must be a valid json string")

    records = blueprint.index_driver.ids(
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
    }

    return flask.jsonify(base), 200


@blueprint.route("/urls/", methods=["GET"])
def get_urls():
    """
    Returns a list of urls.
    """
    ids = flask.request.args.get("ids")
    if ids:
        ids = ids.split(",")
    hashes = flask.request.args.getlist("hash")
    hashes = {h: v for h, v in (x.split(":", 1) for x in hashes)}
    size = flask.request.args.get("size")
    if size:
        try:
            size = int(size)
        except TypeError:
            raise UserError("size must be an integer")

        if size < 0:
            raise UserError("size must be >= 0")

    try:
        start = int(flask.request.args.get("start", 0))
    except TypeError:
        raise UserError("start must be an integer")

    try:
        limit = int(flask.request.args.get("limit", 100))
    except TypeError:
        raise UserError("limit must be an integer")

    if start < 0:
        raise UserError("start must be >= 0")

    if limit < 0:
        raise UserError("limit must be >= 0")

    if limit > 1024:
        raise UserError("limit must be <= 1024")

    validate_hashes(**hashes)

    urls = blueprint.index_driver.get_urls(
        size=size, ids=ids, hashes=hashes, start=start, limit=limit
    )

    ret = {"urls": urls, "limit": limit, "start": start, "size": size, "hashes": hashes}

    return flask.jsonify(ret), 200


@blueprint.route("/index/<path:record>", methods=["GET"])
def get_index_record(record):
    """
    Returns a record.
    """

    ret = blueprint.index_driver.get(record)

    return flask.jsonify(ret), 200


@blueprint.route("/index/", methods=["POST"])
def post_index_record():
    """
    Create a new record.
    """
    try:
        jsonschema.validate(flask.request.json, POST_RECORD_SCHEMA)
    except jsonschema.ValidationError as err:
        raise UserError(err)

    authz = flask.request.json.get("authz", [])
    authorize("create", authz)

    did = flask.request.json.get("did")
    form = flask.request.json["form"]
    size = flask.request.json["size"]
    urls = flask.request.json["urls"]
    acl = flask.request.json.get("acl", [])

    hashes = flask.request.json["hashes"]
    file_name = flask.request.json.get("file_name")
    metadata = flask.request.json.get("metadata")
    urls_metadata = flask.request.json.get("urls_metadata")
    version = flask.request.json.get("version")
    baseid = flask.request.json.get("baseid")
    uploader = flask.request.json.get("uploader")

    did, rev, baseid = blueprint.index_driver.add(
        form,
        did,
        size=size,
        file_name=file_name,
        metadata=metadata,
        urls_metadata=urls_metadata,
        version=version,
        urls=urls,
        acl=acl,
        authz=authz,
        hashes=hashes,
        baseid=baseid,
        uploader=uploader,
    )

    ret = {"did": did, "rev": rev, "baseid": baseid}

    return flask.jsonify(ret), 200


@blueprint.route("/index/blank/", methods=["POST"])
@authorize
def post_index_blank_record():
    """
    Create a blank new record with only uploader and optionally
    file_name fields filled
    """

    uploader = flask.request.get_json().get("uploader")
    file_name = flask.request.get_json().get("file_name")
    if not uploader:
        raise UserError("no uploader specified")

    did, rev, baseid = blueprint.index_driver.add_blank_record(
        uploader=uploader, file_name=file_name
    )

    ret = {"did": did, "rev": rev, "baseid": baseid}

    return flask.jsonify(ret), 201


@blueprint.route("/index/blank/<path:record>", methods=["POST"])
def add_index_blank_record_version(record):
    """
    Create a new blank version of the record with this GUID.
    Authn/authz fields carry over from the previous version of the record.
    Only uploader and optionally file_name fields are filled.
    Returns the GUID of the new blank version and the baseid common to all versions
    of the record.
    """
    uploader = flask.request.get_json().get("uploader")
    file_name = flask.request.get_json().get("file_name")
    new_did = flask.request.json.get("did")
    if not uploader:
        raise UserError("no uploader specified")

    # authorize done in add_blank_version for the existing record's authz
    did, baseid, rev = blueprint.index_driver.add_blank_version(
        record, new_did=new_did, uploader=uploader, file_name=file_name
    )

    ret = {"did": did, "baseid": baseid, "rev": rev}

    return flask.jsonify(ret), 201


@blueprint.route("/index/blank/<path:record>", methods=["PUT"])
@authorize
def put_index_blank_record(record):
    """
    Update a blank record with size, hashes and url
    """
    rev = flask.request.args.get("rev")
    size = flask.request.get_json().get("size")
    hashes = flask.request.get_json().get("hashes")
    urls = flask.request.get_json().get("urls")

    did, rev, baseid = blueprint.index_driver.update_blank_record(
        did=record, rev=rev, size=size, hashes=hashes, urls=urls
    )
    ret = {"did": did, "rev": rev, "baseid": baseid}

    return flask.jsonify(ret), 200


@blueprint.route("/index/<path:record>", methods=["PUT"])
def put_index_record(record):
    """
    Update an existing record.
    """
    try:
        jsonschema.validate(flask.request.json, PUT_RECORD_SCHEMA)
    except jsonschema.ValidationError as err:
        raise UserError(err)

    rev = flask.request.args.get("rev")
    # authorize done in update
    did, baseid, rev = blueprint.index_driver.update(record, rev, flask.request.json)

    ret = {"did": did, "baseid": baseid, "rev": rev}

    return flask.jsonify(ret), 200


@blueprint.route("/index/<path:record>", methods=["DELETE"])
def delete_index_record(record):
    """
    Delete an existing record.
    """
    rev = flask.request.args.get("rev")
    if rev is None:
        raise UserError("no revision specified")

    # authorize done in delete
    blueprint.index_driver.delete(record, rev)

    return "", 200


@blueprint.route("/index/<path:record>", methods=["POST"])
def add_index_record_version(record):
    """
    Add a record version
    """
    try:
        jsonschema.validate(flask.request.json, POST_RECORD_SCHEMA)
    except jsonschema.ValidationError as err:
        raise UserError(err)

    new_did = flask.request.json.get("did")
    form = flask.request.json["form"]
    size = flask.request.json["size"]
    urls = flask.request.json["urls"]
    acl = flask.request.json.get("acl", [])
    authz = flask.request.json.get("authz", [])
    hashes = flask.request.json["hashes"]
    file_name = flask.request.json.get("file_name")
    metadata = flask.request.json.get("metadata")
    urls_metadata = flask.request.json.get("urls_metadata")
    version = flask.request.json.get("version")

    # authorize done in add_version for both the old and new authz
    did, baseid, rev = blueprint.index_driver.add_version(
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
    )

    ret = {"did": did, "baseid": baseid, "rev": rev}

    return flask.jsonify(ret), 200


@blueprint.route("/index/<path:record>/aliases", methods=["GET"])
def get_aliases(record):
    """
    Get all aliases associated with this DID / GUID
    """
    # error handling done in driver
    aliases = blueprint.index_driver.get_aliases_for_did(record)

    aliases_payload = {"aliases": [{"value": alias} for alias in aliases]}
    return flask.jsonify(aliases_payload), 200


@blueprint.route("/index/<path:record>/aliases", methods=["POST"])
def append_aliases(record):
    """
    Append one or more aliases to aliases already associated with this
    DID / GUID, if any.
    """
    # we set force=True so that if MIME type of request is not application/JSON,
    # get_json will still throw a UserError.
    aliases_json = flask.request.get_json(force=True)
    try:
        jsonschema.validate(aliases_json, RECORD_ALIAS_SCHEMA)
    except jsonschema.ValidationError as err:
        logger.warn(f"Bad request body:\n{err}")
        raise UserError(err)

    aliases = [record["value"] for record in aliases_json["aliases"]]

    # authorization and error handling done in driver
    blueprint.index_driver.append_aliases_for_did(aliases, record)

    aliases = blueprint.index_driver.get_aliases_for_did(record)
    aliases_payload = {"aliases": [{"value": alias} for alias in aliases]}
    return flask.jsonify(aliases_payload), 200


@blueprint.route("/index/<path:record>/aliases", methods=["PUT"])
def replace_aliases(record):
    """
    Replace all aliases associated with this DID / GUID
    """
    # we set force=True so that if MIME type of request is not application/JSON,
    # get_json will still throw a UserError.
    aliases_json = flask.request.get_json(force=True)
    try:
        jsonschema.validate(aliases_json, RECORD_ALIAS_SCHEMA)
    except jsonschema.ValidationError as err:
        logger.warn(f"Bad request body:\n{err}")
        raise UserError(err)

    aliases = [record["value"] for record in aliases_json["aliases"]]

    # authorization and error handling done in driver
    blueprint.index_driver.replace_aliases_for_did(aliases, record)

    aliases_payload = {"aliases": [{"value": alias} for alias in aliases]}
    return flask.jsonify(aliases_payload), 200


@blueprint.route("/index/<path:record>/aliases", methods=["DELETE"])
def delete_all_aliases(record):
    # authorization and error handling done in driver
    blueprint.index_driver.delete_all_aliases_for_did(record)

    return flask.jsonify("Aliases deleted successfully"), 200


@blueprint.route("/index/<path:record>/aliases/<path:alias>", methods=["DELETE"])
def delete_one_alias(record, alias):
    # authorization and error handling done in driver
    blueprint.index_driver.delete_one_alias_for_did(alias, record)

    return flask.jsonify("Aliases deleted successfully"), 200


@blueprint.route("/index/<path:record>/versions", methods=["GET"])
def get_all_index_record_versions(record):
    """
    Get all record versions
    """
    ret = blueprint.index_driver.get_all_versions(record)

    return flask.jsonify(ret), 200


@blueprint.route("/index/<path:record>/latest", methods=["GET"])
def get_latest_index_record_versions(record):
    """
    Get the latest record version
    """
    has_version = flask.request.args.get("has_version", "").lower() == "true"
    ret = blueprint.index_driver.get_latest_version(record, has_version=has_version)

    return flask.jsonify(ret), 200


@blueprint.route("/_status", methods=["GET"])
def health_check():
    """
    Health Check.
    """
    blueprint.index_driver.health_check()

    return "Healthy", 200


@blueprint.route("/_stats", methods=["GET"])
def stats():
    """
    Return indexed data stats.
    """

    filecount = blueprint.index_driver.len()
    totalfilesize = blueprint.index_driver.totalbytes()

    base = {"fileCount": filecount, "totalFileSize": totalfilesize}

    return flask.jsonify(base), 200


@blueprint.route("/_version", methods=["GET"])
def version():
    """
    Return the version of this service.
    """

    base = {"version": VERSION, "commit": COMMIT}

    return flask.jsonify(base), 200


@blueprint.errorhandler(NoRecordFound)
def handle_no_record_error(err):
    return flask.jsonify(error=str(err)), 404


@blueprint.errorhandler(MultipleRecordsFound)
def handle_multiple_records_error(err):
    return flask.jsonify(error=str(err)), 409


@blueprint.errorhandler(UserError)
def handle_user_error(err):
    return flask.jsonify(error=str(err)), 400


@blueprint.errorhandler(AuthError)
def handle_auth_error(err):
    return flask.jsonify(error=str(err)), 403


@blueprint.errorhandler(RevisionMismatch)
def handle_revision_mismatch(err):
    return flask.jsonify(error=str(err)), 409


@blueprint.errorhandler(UnhealthyCheck)
def handle_unhealthy_check(err):
    return "Unhealthy", 500


@blueprint.record
def get_config(setup_state):
    config = setup_state.app.config["INDEX"]
    blueprint.index_driver = config["driver"]
