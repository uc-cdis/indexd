import flask
import uuid

blueprint = flask.Blueprint("guid", __name__)


@blueprint.route("/guid/mint", methods=["GET"])
def mint_guid():
    """
    Mint a GUID that is valid for this instance of indexd. The intention
    of this endpoint is to allow pre-computing valid GUIDs to be indexed
    WITHOUT actually creating a new record yet

    Allows for a `count` query parameter to get bulk GUIDs up to some limit
    """
    count = flask.request.args.get("count", 1)
    max_count = 10000

    try:
        count = int(count)
    except Exception:
        return f"Count {count} is not a valid integer", 400

    # clip value on 0, max_count
    if count < 0:
        count = 0
    elif count > max_count:
        count = max_count

    guids = []
    for _ in range(count):
        valid_guid = str(uuid.uuid4())
        if flask.current_app.config.get("PREPEND_PREFIX"):
            valid_guid = flask.current_app.config["DEFAULT_PREFIX"] + valid_guid
        guids.append(valid_guid)

    return flask.jsonify({"guids": guids}), 200


@blueprint.route("/guid/prefix", methods=["GET"])
def get_prefix():
    """
    Get the prefix for this instance of indexd.
    """
    prefix = ""
    if flask.current_app.config.get("PREPEND_PREFIX"):
        prefix = flask.current_app.config["DEFAULT_PREFIX"]

    return prefix, 200
