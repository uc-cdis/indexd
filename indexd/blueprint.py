import os
from crypt import methods
import re
import flask

from indexclient.client import IndexClient
from doiclient.client import DOIClient
from dosclient.client import DOSClient
from hsclient.client import HSClient

from indexd.utils import hint_match, drs_service_info_id_url_reversal

from indexd.errors import AuthError
from indexd.errors import UserError
from indexd.alias.errors import NoRecordFound as AliasNoRecordFound
from indexd.index.errors import NoRecordFound as IndexNoRecordFound

blueprint = flask.Blueprint("cross", __name__)

blueprint.config = dict()
blueprint.index_driver = None
blueprint.alias_driver = None
blueprint.dist = []


@blueprint.route("/alias/<path:alias>", methods=["GET"])
def get_alias(alias):
    """
    Return alias associated information.
    """
    info = blueprint.alias_driver.get(alias)

    start = 0
    limit = 100

    size = info["size"]
    hashes = info["hashes"]

    urls = blueprint.index_driver.get_urls(
        size=size, hashes=hashes, start=start, limit=limit
    )

    info.update({"urls": urls, "start": start, "limit": limit})

    return flask.jsonify(info), 200


@blueprint.route("/<path:record>", methods=["GET"])
def get_record(record):
    """
    Returns a record from the local ids, alias, or global resolvers.
    """

    try:
        ret = blueprint.index_driver.get_with_nonstrict_prefix(record)
    except IndexNoRecordFound:
        try:
            ret = blueprint.index_driver.get_by_alias(record)
        except IndexNoRecordFound:
            try:
                ret = blueprint.alias_driver.get(record)
            except AliasNoRecordFound:
                if not blueprint.dist or "no_dist" in flask.request.args:
                    raise
                ret = dist_get_record(record)

    return flask.jsonify(ret), 200


def dist_get_record(record):
    # Sort the list of distributed ID services
    # Ones with which the request matches a hint will be first
    # Followed by those that don't match the hint
    sorted_dist = sorted(
        blueprint.dist, key=lambda k: hint_match(record, k["hints"]), reverse=True
    )

    for indexd in sorted_dist:
        try:
            if indexd["type"] == "doi":  # Digital Object Identifier
                fetcher_client = DOIClient(baseurl=indexd["host"])
                res = fetcher_client.get(record)
            elif indexd["type"] == "dos":  # Data Object Service
                fetcher_client = DOSClient(baseurl=indexd["host"])
                res = fetcher_client.get(record)
            elif indexd["type"] == "hs":  # HydroShare and CommonsShare
                fetcher_client = HSClient(baseurl=indexd["host"])
                res = fetcher_client.get(record)
            else:
                fetcher_client = IndexClient(baseurl=indexd["host"])
                res = fetcher_client.global_get(record, no_dist=True)
        except Exception:
            # a lot of things can go wrong with the get, but in general we don't care here.
            continue

        if res:
            json = res.to_json()
            json["from_index_service"] = {
                "host": indexd["host"],
                "name": indexd["name"],
            }
            return json

    raise IndexNoRecordFound("no record found")


@blueprint.route("/service-info", methods=["GET"])
def get_drs_service_info():
    """
    Returns DRS compliant service information
    """
    drs_dist = {}

    # Check to see if the information is of type drs. If not, use the available information to return DRS compliant service information
    for dist in blueprint.dist:
        if (
            "type" in dist
            and isinstance(dist["type"], dict)
            and "artifact" in dist["type"]
            and dist["type"]["artifact"] == "drs"
        ):
            drs_dist = dist
    if drs_dist == {}:
        drs_dist = blueprint.dist[0]

    reverse_domain_name = drs_service_info_id_url_reversal(url=os.environ["HOSTNAME"])

    ret = {
        "id": drs_dist.get("id", reverse_domain_name),
        "name": drs_dist.get("name", "DRS System"),
        "version": drs_dist.get("version", "1.0.0"),
        "type": {
            "group": drs_dist.get("group", "org.ga4gh"),
            "artifact": drs_dist.get("artifact", "drs"),
        },
        "organization": {
            "name": "Gen3",
        },
    }

    if "type" in drs_dist and isinstance(drs_dist["type"], dict):
        ret["type"]["version"] = drs_dist.get("type").get("version", "1.0.0")
    else:
        ret["type"]["version"] = "1.0.0"

    if "organization" in drs_dist and "url" in drs_dist["organization"]:
        ret["organization"]["url"] = drs_dist["organization"]["url"]
    else:
        ret["organization"]["url"] = "https://" + os.environ["HOSTNAME"]

    return flask.jsonify(ret), 200


@blueprint.errorhandler(UserError)
def handle_user_error(err):
    return flask.jsonify(error=str(err)), 400


@blueprint.errorhandler(AuthError)
def handle_auth_error(err):
    return flask.jsonify(error=str(err)), 403


@blueprint.errorhandler(AliasNoRecordFound)
def handle_no_record_error(err):
    return flask.jsonify(error=str(err)), 404


@blueprint.errorhandler(IndexNoRecordFound)
def handle_no_record_error(err):
    return flask.jsonify(error=str(err)), 404


@blueprint.record
def get_config(setup_state):
    index_config = setup_state.app.config["INDEX"]
    alias_config = setup_state.app.config["ALIAS"]
    blueprint.index_driver = index_config["driver"]
    blueprint.alias_driver = alias_config["driver"]
    if "DIST" in setup_state.app.config:
        blueprint.dist = setup_state.app.config["DIST"]
