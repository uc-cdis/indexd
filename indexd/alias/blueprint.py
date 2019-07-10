import re
import flask
import jsonschema

from indexd.auth import authorize

from indexd.errors import AuthError
from indexd.errors import UserError

from .schema import PUT_RECORD_SCHEMA

from .errors import NoRecordFound
from .errors import MultipleRecordsFound
from .errors import RevisionMismatch


blueprint = flask.Blueprint("alias", __name__)

blueprint.config = {}
blueprint.alias_driver = None

ACCEPTABLE_HASHES = {
    "md5": re.compile(r"^[0-9a-f]{32}$").match,
    "sha1": re.compile(r"^[0-9a-f]{40}$").match,
    "sha256": re.compile(r"^[0-9a-f]{64}$").match,
    "sha512": re.compile(r"^[0-9a-f]{128}$").match,
}


def validate_hashes(**hashes):
    """
    Validate hashes against known and valid hashing algorithms.
    """
    if not all(h in ACCEPTABLE_HASHES for h in hashes):
        raise UserError("invalid hash types specified")

    if not all(ACCEPTABLE_HASHES[h](v) for h, v in hashes.items()):
        raise UserError("invalid hash values specified")


@blueprint.route("/alias/", methods=["GET"])
def get_alias():
    """
    Returns a list of records.
    """
    limit = flask.request.args.get("limit")
    try:
        limit = 100 if limit is None else int(limit)
    except ValueError as err:
        raise UserError("limit must be an integer")

    if limit <= 0 or limit > 1024:
        raise UserError("limit must be between 1 and 1024")

    size = flask.request.args.get("size")
    try:
        size = size if size is None else int(size)
    except ValueError as err:
        raise UserError("size must be an integer")

    if size is not None and size < 0:
        raise UserError("size must be > 0")

    start = flask.request.args.get("start")

    hashes = flask.request.args.getlist("hash")
    hashes = {h: v for h, v in (x.split(":", 1) for x in hashes)}

    # TODO FIXME this needs reworking
    validate_hashes(**hashes)
    hashes = hashes if hashes else None

    if limit < 0 or limit > 1024:
        raise UserError("limit must be between 0 and 1024")

    aliases = blueprint.alias_driver.aliases(
        start=start, limit=limit, size=size, hashes=hashes
    )

    base = {
        "aliases": aliases,
        "limit": limit,
        "start": start,
        "size": size,
        "hashes": hashes,
    }

    return flask.jsonify(base), 200


# @blueprint.route('/alias/<path:record>', methods=['GET'])
# def get_alias_record(record):
#    '''
#    Returns a record.
#    '''
#    ret = blueprint.alias_driver.get(record)
#
#    return flask.jsonify(ret), 200


@blueprint.route("/alias/<path:record>", methods=["PUT"])
@authorize
def put_alias_record(record):
    """
    Create or replace an existing record.
    """
    try:
        jsonschema.validate(flask.request.json, PUT_RECORD_SCHEMA)
    except jsonschema.ValidationError as err:
        raise UserError(err)

    rev = flask.request.args.get("rev")

    size = flask.request.json.get("size")
    hashes = flask.request.json.get("hashes")
    release = flask.request.json.get("release")
    metastring = flask.request.json.get("metadata")
    host_authorities = flask.request.json.get("host_authorities")
    keeper_authority = flask.request.json.get("keeper_authority")

    record, rev = blueprint.alias_driver.upsert(
        record,
        rev,
        size=size,
        hashes=hashes,
        release=release,
        metastring=metastring,
        host_authorities=host_authorities,
        keeper_authority=keeper_authority,
    )

    ret = {"name": record, "rev": rev}

    return flask.jsonify(ret), 200


@blueprint.route("/alias/<path:record>", methods=["DELETE"])
@authorize
def delete_alias_record(record):
    """
    Delete an alias.
    """
    rev = flask.request.args.get("rev")

    blueprint.alias_driver.delete(record, rev)

    return "", 200


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


@blueprint.record
def get_config(setup_state):
    config = setup_state.app.config["ALIAS"]
    blueprint.alias_driver = config["driver"]
