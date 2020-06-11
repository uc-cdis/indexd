import flask
from indexd.errors import AuthError, AuthzError
from indexd.errors import UserError
from indexd.index.errors import NoRecordFound as IndexNoRecordFound
from indexd.errors import UnexpectedError
from indexd.index.blueprint import get_index

blueprint = flask.Blueprint("drs", __name__)

blueprint.config = dict()
blueprint.index_driver = None


@blueprint.route("/ga4gh/drs/v1/objects/<path:object_id>", methods=["GET"])
def get_drs_object(object_id):
    """
    Returns a specific DRSobject with object_id
    """
    ret = blueprint.index_driver.get(object_id)

    return flask.jsonify(indexd_to_drs(ret)), 200


@blueprint.route("/ga4gh/drs/v1/objects", methods=["GET"])
def list_drs_records():
    records = get_index()[0].json["records"]
    ret = {"drs_objects": [indexd_to_drs(record, True) for record in records]}

    return flask.jsonify(ret), 200


@blueprint.route(
    "/ga4gh/drs/v1/objects/<path:object_id>/access",
    defaults={"access_id": None},
    methods=["GET"],
)
@blueprint.route(
    "/ga4gh/drs/v1/objects/<path:object_id>/access/<path:access_id>", methods=["GET"]
)
def get_signed_url(object_id, access_id):
    if not access_id:
        raise (UserError("Access ID/Protocol is required."))
    res = flask.current_app.fence_client.get_signed_url_for_object(
        object_id=object_id, access_id=access_id
    )
    if not res:
        raise IndexNoRecordFound("No signed url found")

    return res, 200


def indexd_to_drs(record, list_drs=False):
    bearer_token = flask.request.headers.get("AUTHORIZATION")
    self_uri = "drs://" + flask.current_app.hostname + "/" + record["did"]
    drs_object = {
        "id": record["did"],
        "description": "",
        "mime_type": "application/json",
        "name": record["file_name"],
        "created_time": record["created_date"],
        "updated_time": record["updated_date"],
        "size": record["size"],
        "aliases": [],
        "contents": [],
        "self_uri": self_uri,
        "version": record["rev"],
    }

    if "description" in record:
        drs_object["description"] = record["description"]
    if "alias" in record:
        drs_object["aliases"].append(record["alias"])

    if "contents" in record:
        drs_object["contents"] = record["contents"]

    # access_methods mapping
    if "urls" in record:
        drs_object["access_methods"] = []
        for location in record["urls"]:
            location_type = location.split(":")[
                0
            ]  # (s3, gs, ftp, gsiftp, globus, htsget, https, file)

            drs_object["access_methods"].append(
                {
                    "type": location_type,
                    "access_url": {"url": location},
                    "access_id": location_type,
                    "region": "",
                }
            )
        print(drs_object)

    # parse out checksums
    drs_object["checksums"] = []
    for k in record["hashes"]:
        drs_object["checksums"].append({"checksum": record["hashes"][k], "type": k})

    return drs_object


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


@blueprint.errorhandler(UnexpectedError)
def handle_unexpected_error(err):
    ret = {"msg": str(err), "status_code": 500}
    return flask.jsonify(ret), 500


@blueprint.record
def get_config(setup_state):
    index_config = setup_state.app.config["INDEX"]
    blueprint.index_driver = index_config["driver"]
