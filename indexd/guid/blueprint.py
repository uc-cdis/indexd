import flask
import uuid

blueprint = flask.Blueprint("guid", __name__)


@blueprint.route("/guid/mint", methods=["GET"])
def mint_guid():
    """
    Mint a GUID that is valid for this instance of indexd. The intention
    of this endpoint is to allow generating valid GUIDs to be indexed
    WITHOUT actually creating a new record yet.

    Allows for a `count` query parameter to get bulk GUIDs up to some limit
    """
    count = flask.request.args.get("count", 1)
    max_count = 10000

    try:
        count = int(count)
    except Exception:
        return f"Count {count} is not a valid integer", 400

    # error on < 0, > max_count
    if count < 0:
        return "You cannot provide a count less than 0", 400
    elif count > max_count:
        return f"You cannot provide a count greater than {max_count}", 400

    guids = []
    for _ in range(count):
        valid_guid = _get_prefix() + str(uuid.uuid4())
        guids.append(valid_guid)

    return flask.jsonify({"guids": guids}), 200


@blueprint.route("/guid/prefix", methods=["GET"])
def get_prefix():
    """
    Get the prefix for this instance of indexd.
    """
    return flask.jsonify({"prefix": _get_prefix()}), 200


def _get_prefix():
    """
    Return prefix if it's configured to be prepended to all GUIDs and NOT
    set as an alias
    """
    prefix = ""

    if flask.current_app.config["INDEX"]["driver"].config.get(
        "PREPEND_PREFIX"
    ) and not flask.current_app.config["INDEX"]["driver"].config.get(
        "ADD_PREFIX_ALIAS"
    ):
        prefix = flask.current_app.config["INDEX"]["driver"].config["DEFAULT_PREFIX"]

    return prefix
