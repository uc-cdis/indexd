import flask

from indexd.blueprint import dist_get_record

from indexd.errors import AuthError
from indexd.errors import UserError
from indexd.alias.errors import NoRecordFound as AliasNoRecordFound
from indexd.index.errors import NoRecordFound as IndexNoRecordFound

blueprint = flask.Blueprint("dos", __name__)

blueprint.config = dict()
blueprint.index_driver = None
blueprint.alias_driver = None
blueprint.dist = []


@blueprint.route("/ga4gh/dos/v1/dataobjects/<path:record>", methods=["GET"])
def get_dos_record(record):
    """
    Returns a record from the local ids, alias, or global resolvers.
    Returns DOS Schema
    """
    try:
        ret = blueprint.index_driver.get(record)
        # record may be a baseID or a DID / GUID. If record is a baseID, ret["did"] is the latest GUID for that record.
        ret["alias"] = blueprint.index_driver.get_aliases_for_did(ret["did"])
    except IndexNoRecordFound:
        try:
            ret = blueprint.index_driver.get_by_alias(record)
            ret["alias"] = blueprint.index_driver.get_aliases_for_did(ret["did"])
        except IndexNoRecordFound:
            try:
                ret = blueprint.alias_driver.get(record)
            except AliasNoRecordFound:
                if not blueprint.dist:
                    raise
                ret = dist_get_record(record)

    return flask.jsonify(indexd_to_dos(ret)), 200


@blueprint.route("/ga4gh/dos/v1/dataobjects", methods=["GET"])
def list_dos_records():
    """
    Returns a record from the local ids, alias, or global resolvers.
    Returns DOS Schema
    """
    start = flask.request.args.get("page_token")
    limit = flask.request.args.get("page_size")

    try:
        limit = 100 if limit is None else int(limit)
    except ValueError:
        raise UserError("limit must be an integer")

    if limit <= 0 or limit > 1024:
        raise UserError("limit must be between 1 and 1024")

    url = flask.request.args.get("url")

    # Support this in the future when we have
    # more fully featured aliases?
    # alias = flask.request.args.get('alias')

    checksum = flask.request.args.get("checksum")
    if checksum:
        hashes = {checksum["type"]: checksum["checksum"]}
    else:
        hashes = None

    records = blueprint.index_driver.ids(
        start=start, limit=limit, urls=url, hashes=hashes
    )

    for record in records:
        record["alias"] = blueprint.index_driver.get_aliases_for_did(record["did"])

    ret = {"data_objects": [indexd_to_dos(record)["data_object"] for record in records]}

    return flask.jsonify(ret), 200


def indexd_to_dos(record):
    data_object = {"id": record["did"], "description": "", "mime_type": ""}

    if "file_name" in record:
        data_object["name"] = record["file_name"]

    if "created_date" in record:
        data_object["created"] = record["created_date"]

    if "updated_date" in record:
        data_object["updated"] = record["updated_date"]

    if "rev" in record:
        data_object["version"] = record["rev"]

    if "size" in record:
        data_object["size"] = record["size"]

    if "alias" in record:
        data_object["aliases"] = record["alias"]

    # parse out checksums
    data_object["checksums"] = []
    for k in record["hashes"]:
        data_object["checksums"].append({"checksum": record["hashes"][k], "type": k})

    # parse out the urls
    data_object["urls"] = []
    for url in record["urls"]:
        url_object = {"url": url}
        if "metadata" in record and record["metadata"]:
            url_object["system_metadata"] = record["metadata"]
        if (
            "urls_metadata" in record
            and url in record["urls_metadata"]
            and record["urls_metadata"][url]
        ):
            url_object["user_metadata"] = record["urls_metadata"][url]
        data_object["urls"].append(url_object)

    result = {"data_object": data_object}
    return result


@blueprint.errorhandler(UserError)
def handle_user_error(err):
    ret = {"msg": str(err), "status_code": 400}
    return flask.jsonify(ret), 400


@blueprint.errorhandler(AuthError)
def handle_auth_error(err):
    ret = {"msg": str(err), "status_code": 403}
    return flask.jsonify(ret), 403


@blueprint.errorhandler(AliasNoRecordFound)
def handle_no_alias_record_error(err):
    ret = {"msg": str(err), "status_code": 404}
    return flask.jsonify(ret), 404


@blueprint.errorhandler(IndexNoRecordFound)
def handle_no_index_record_error(err):
    ret = {"msg": str(err), "status_code": 404}
    return flask.jsonify(ret), 404


@blueprint.record
def get_config(setup_state):
    index_config = setup_state.app.config["INDEX"]
    alias_config = setup_state.app.config["ALIAS"]
    blueprint.index_driver = index_config["driver"]
    blueprint.alias_driver = alias_config["driver"]
    if "DIST" in setup_state.app.config:
        blueprint.dist = setup_state.app.config["DIST"]
