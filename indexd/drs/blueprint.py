import flask

from indexd.blueprint import dist_get_record

from indexd.errors import AuthError
from indexd.errors import UserError
from indexd.index.errors import NoRecordFound as IndexNoRecordFound
from indexd.drs.errors import UnexpectedError

from indexd.fence import get_signed_url_for_object, FenceRequest

try:
    from local_settings import settings
except ImportError:
    from indexd.default_settings import settings

import requests

blueprint = flask.Blueprint("drs", __name__)

blueprint.config = dict()
blueprint.index_driver = None
blueprint.dist = []

@blueprint.route("/ga4gh/drs/v1/objects/<path:object_id>", methods=["GET"])
def get_drs_object(object_id):
    """
    Returns a specific DRSobject with object_id
    """
    ret = blueprint.index_driver.get(object_id)
    # access_token
    return flask.jsonify(indexd_to_drs(ret)), 200

    
@blueprint.route("/ga4gh/drs/v1/objects", methods=["GET"])
def list_drs_records():
    start = flask.request.args.get("page_token")
    limit = flask.request.args.get("page_size")

    try:
        limit = 100 if limit is None else int(limit)
    except ValueError:
        raise UserError("limit must be an integer")

    if limit <= 0 or limit > 1024:
        raise UserError("limit must be between 1 and 1024")

    url = flask.request.args.get("url")

    checksum = flask.request.args.get("checksum")
    if checksum:
        hashes = {checksum["type"]: checksum["checksum"]}
    else:
        hashes = None

    records = blueprint.index_driver.ids(
        start=start, limit=limit, urls=url, hashes=hashes
    )

    ret = {"drs_objects": [indexd_to_drs(record)["drs_object"] for record in records]}

    return flask.jsonify(ret), 200


@blueprint.route("/ga4gh/drs/v1/objects/<path:object_id>/access/<path:access_id>", methods=["GET"])
def get_signed_url(object_id, access_id=""):
    res = get_signed_url_for_object(object_id, access_id)
    if not res:
        raise IndexNoRecordFound("No signed url found")
    return res


def indexd_to_drs(record, access_id=""):
    drs_object = {"id": record["did"], "description": "", "mime_type": ""}
    if "file_name" in record:
        drs_object["name"] = record["file_name"]

    if "self_uri" in record:
        drs_object["self_uri"] = record["self_uri"]

    if "created_date" in record:
        drs_object["created_time"] = record["created_date"]

    if "updated_date" in record:
        drs_object["updated_time"] = record["updated_date"]

    if "size" in record:
        drs_object["size"] = record["size"]

    if "alias" in record:
        drs_object["aliases"].append(record["alias"])

    if "contents" in record:
        drs_object["contents"] = record["contents"]

    # access_methods mapping
    if "urls" in record:
        drs_object["access_methods"] = []
        for location in record["urls"]:
            location_type = location.split(":")[0] #(s3, gs, ftp, gsiftp, globus, htsget, https, file)

            drs_object["access_methods"].append({
                "type":location_type, 
                "access_url":get_signed_url_for_object(record["did"], access_id).json() if access_id and get_signed_url_for_object(record["did"], access_id) else {"url":location},
                "access_id":location_type, 
                "region": ""
            })
        print(drs_object)

    # parse out checksums
    drs_object["checksums"] = []
    for k in record["hashes"]:
        drs_object["checksums"].append({"checksum": record["hashes"][k], "type": k})

    result = {"drs_object": drs_object}
    return result


@blueprint.errorhandler(UserError)
def handle_user_error(err):
    ret = {"msg": str(err), "status_code": 400}
    return flask.jsonify(ret), 400

@blueprint.errorhandler(AuthError)
def handle_auth_error(err):
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

@blueprint.errorhandler(UnexpectedError)
def handle_unexpected_error(err):
    ret = {"msg": str(err), "status_code": 500}
    return flask.jsonify(ret), 500


@blueprint.record
def get_config(setup_state):
    index_config = setup_state.app.config["INDEX"]
    blueprint.index_driver = index_config["driver"]
    if "DIST" in setup_state.app.config:
        blueprint.dist = setup_state.app.config["DIST"]
