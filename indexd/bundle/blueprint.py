import re
import json
import flask
import jsonschema
import os.path
import subprocess
import uuid
import hashlib
from indexd.index.version_data import VERSION, COMMIT

from indexd.auth import authorize

from indexd.errors import AuthError
from indexd.errors import UserError

from indexd.index.schema import BUNDLE_SCHEMA

from indexd.index.errors import NoRecordFound
from indexd.index.errors import MultipleRecordsFound
from indexd.index.errors import UnhealthyCheck

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


# @blueprint.route("/bundle/", methods=["GET"])
# def get_bundle():


# @blueprint.route("/bundle/<path:record>", methods=["GET"])
# def get_bundle_record(record):
#     """
#     Returns a record.
#     """

#     ret = blueprint.index_driver.get_bundle(record)

#     return flask.jsonify(ret), 200

# @blueprint.route("/bundle/", methods=["POST"])
# @authorize
# def post_bundle():
#     """
#     Create a new bundle
#     """
#     print(" ")
#     print("BUNDLE POST!!!!")
#     print(" ")

#     try:
#         jsonschema.validate(flask.request.json, BUNDLE_SCHEMA)
#     except jsonschema.ValidationError as err:
#         raise UserError(err)

#     name = flask.request.json.get("name")
#     bundle = flask.request.json.get("bundles")

#     bundle_id = str(uuid.uuid4())
#     checksum = str(hashlib.md5(bundle_id))

#     bundle_data = bundle

#     ret = blueprint.index_driver.add_bundle(
#         bundle_id=bundle_id,
#         name=name,
#         checksum=checksum,
#         size=1,
#         bundle_data=bundle_data,
#     )

#     return flask.jsonify(ret), 200


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


@blueprint.errorhandler(UnhealthyCheck)
def handle_unhealthy_check(err):
    return "Unhealthy", 500


@blueprint.record
def get_config(setup_state):
    config = setup_state.app.config["INDEX"]
    blueprint.index_driver = config["driver"]
