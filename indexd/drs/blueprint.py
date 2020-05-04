import flask
from indexd.errors import AuthError
from indexd.errors import UserError
from indexd.index.errors import NoRecordFound as IndexNoRecordFound
from indexd.errors import UnexpectedError

# from indexd.index.blueprint import get_index
from indexd.index.blueprint import get_index

blueprint = flask.Blueprint("drs", __name__)

blueprint.config = dict()
blueprint.index_driver = None


@blueprint.route("/ga4gh/drs/v1/objects/<path:object_id>", methods=["GET"])
def get_drs_object(object_id):
    """
    Returns a specific DRSobject with object_id
    """
    expand = True if flask.request.args.get("expand") == "true" else False

    ret = blueprint.index_driver.get(object_id)

    data = (
        indexd_to_drs(ret, expand=False, list_drs=False)
        if not flask.request.args.get("expand")
        else bundle_to_drs(ret, expand=expand)
    )

    return flask.jsonify(data), 200


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


def indexd_to_drs(record, expand=False, list_drs=False):

    bearer_token = flask.request.headers.get("AUTHORIZATION")

    did = (
        record["id"]
        if "id" in record
        else record["did"]
        if "did" in record
        else record["bundle_id"]
    )

    self_uri = "drs://" + flask.current_app.hostname + "/" + did

    name = record["file_name"] if "file_name" in record else record["name"]

    created_time = (
        record["created_date"] if "created_date" in record else record["created_time"]
    )

    version = record["rev"] if "rev" in record else ""

    updated_date = (
        record["updated_date"] if "updated_date" in record else record["updated_time"]
    )

    form = record["form"] if "form" in record else "bundle"

    drs_object = {
        "id": did,
        "description": "",
        "mime_type": "application/json",
        "name": name,
        "created_time": created_time,
        "updated_time": updated_date,
        "size": record["size"],
        "aliases": [],
        "contents": [],
        "self_uri": self_uri,
        "version": version,
        "form": form,
        "checksums": [],
    }

    if "description" in record:
        drs_object["description"] = record["description"]
    if "alias" in record:
        drs_object["aliases"].append(record["alias"])

    if "bundle_data" in record:
        bundle_data = record["bundle_data"]
        for bundle in bundle_data:
            drs_object["contents"].append(
                bundle_to_drs(bundle, expand=expand, is_content=True)
            )

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
                    "access_url": flask.current_app.fence_client.get_signed_url_for_object(
                        record["did"], ""
                    )
                    if bearer_token and not list_drs
                    else {"url": location},
                    "access_id": location_type,
                    "region": "",
                }
            )

    # parse out checksums
    parse_checksums(record, drs_object)

    return drs_object


def bundle_to_drs(record, expand=False, is_content=False):
    """
    is_content: is an expanded content in a bundle
    """

    did = (
        record["id"]
        if "id" in record
        else record["did"]
        if "did" in record
        else record["bundle_id"]
    )

    drs_uri = "drs://" + flask.current_app.hostname + "/" + did

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
        else record["bundle_data"]
        if "bundle_data" in record
        else []
    )

    if not expand:
        for content in contents:
            content["contents"] = []
    drs_object["contents"] = contents

    if not is_content:
        # Show these only if its the leading bundle
        drs_object["checksums"] = []
        parse_checksums(record, drs_object)

        created_time = (
            record["created_date"]
            if "created_date" in record
            else record["created_time"]
        )

        updated_time = (
            record["updated_date"]
            if "updated_date" in record
            else record["updated_time"]
        )
        drs_object["created_time"] = created_time
        drs_object["updated_time"] = updated_time
        drs_object["size"] = record["size"]

    return drs_object


def parse_checksums(record, drs_object):
    if "hashes" in record:
        for k in record["hashes"]:
            drs_object["checksums"].append({"checksum": record["hashes"][k], "type": k})
    else:
        if "checksums" in record:
            for checksum in record["checksums"]:
                drs_object["checksums"].append(
                    {"checksum": checksum["checksum"], "type": checksum["type"]}
                )
        else:
            drs_object["checksums"].append(
                {"checksum": record["checksum"], "type": "md5"}
            )


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
