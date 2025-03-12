import json
import logging

import flask

from indexd.errors import UserError
from indexd.index.drivers.query.urls import AlchemyURLsQueryDriver

logger = logging.getLogger(__name__)
blueprint = flask.Blueprint("urls", __name__)


@blueprint.route("/q", methods=["GET"])
def query():
    """Queries indexes based on URLs
    Params:
        exclude (str): only include documents (did) with urls that does not match this pattern
        include (str): only include documents (did) with a url matching this pattern
        versioned (str): return only records with version number
        exclude_deleted (str): return only records not marked as deleted
        fields (str): comma separated list of fields to return, if not specified return all fields
        limit (str): max results to return
        offset (str): where to start the next query from
    Returns:
        flask.Response: json list of matching entries
            `
                [
                    {"did": "AAAA-BB", "rev": "1ADs" "urls": ["s3://some-randomly-awesome-url"]},
                    {"did": "AAAA-CC", "rev": "2Nsf", "urls": ["s3://another-randomly-awesome-url"]}
                ]
            `
    """
    args_dict = flask.request.args.to_dict()
    if "versioned" in args_dict.keys():
        args_dict["versioned"] = args_dict["versioned"].lower() in [
            "true",
            "t",
            "yes",
            "y",
        ]
    if "exclude_deleted" in args_dict.keys():
        args_dict["exclude_deleted"] = args_dict["exclude_deleted"].lower() in [
            "true",
            "t",
            "yes",
            "y",
        ]

    record_list = blueprint.driver.query_urls(**args_dict)
    return flask.Response(
        json.dumps(record_list, indent=2, separators=(", ", ": ")),
        200,
        mimetype="application/json",
    )


@blueprint.route("/metadata/q")
def query_metadata():
    """Queries indexes by URLs metadata key and value
    Params:
        key (str): metadata key
        value (str): metadata value for key
        url (str): full url or pattern for limit to
        fields (str): comma separated list of fields to return, if not specified return all fields
        versioned (str): filter only records with a version number
        exclude_deleted (str): return only records not marked as deleted
        limit (str): max results to return
        offset (str): where to start the next query from
    Returns:
        flask.Response: json list of matching entries
            `
                [
                    {"did": "AAAA-BB", "rev": "1ADs" "urls": ["s3://some-randomly-awesome-url"]},
                    {"did": "AAAA-CC", "rev": "2Nsf", "urls": ["s3://another-randomly-awesome-url"]}
                ]
            `
    """
    args_dict = flask.request.args.to_dict()
    if "versioned" in args_dict.keys():
        args_dict["versioned"] = args_dict["versioned"].lower() in [
            "true",
            "t",
            "yes",
            "y",
        ]
    if "exclude_deleted" in args_dict.keys():
        args_dict["exclude_deleted"] = args_dict["exclude_deleted"].lower() in [
            "true",
            "t",
            "yes",
            "y",
        ]

    record_list = blueprint.driver.query_metadata_by_key(**args_dict)
    return flask.Response(
        json.dumps(record_list, indent=2, separators=(", ", ": ")),
        200,
        mimetype="application/json",
    )


@blueprint.record
def pre_config(state):
    driver = state.app.config["INDEX"]["driver"]
    blueprint.logger = logger
    blueprint.driver = AlchemyURLsQueryDriver(driver)


@blueprint.errorhandler(UserError)
def handle_user_error(err):
    return flask.jsonify(error=str(err)), 400
