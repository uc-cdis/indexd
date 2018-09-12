import json
import logging

from flask import Blueprint, request, Response

from indexd.errors import UserError
from indexd.index import request_args_to_params
from indexd.index.drivers.query.urls import AlchemyURLsQueryDriver


logger = logging.getLogger("index.urls.blueprint")
urls = Blueprint("urls", __name__)

urls.driver = None  # type: AlchemyURLsQueryDriver


@urls.route("/q", methods=["GET"])
@request_args_to_params
def query(exclude=None, include=None, version=None, fields=None, limit=100, offset=0, **kwargs):
    """ Queries indexes based on URLs
    Args:
        exclude (str): only include documents (did) with urls that does not match this pattern
        include (str): only include documents (did) with a url matching this pattern
        version (str): return only records with version number
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
    # parameter validation
    if kwargs:
        return Response("Unexpected Parameter(s) {}".format(kwargs), 400)

    records = urls.driver.query_urls(exclude=exclude, include=include,
                                     only_versioned=version and version.lower() in ["true", "t", "yes", "y"],
                                     offset=int(offset), limit=int(limit))

    fields = fields or "did,urls"
    fields_dict = requested_fields(fields)

    results = []
    for record in records:
        c_response = {}
        if fields_dict.get("did"):
            c_response["did"] = record[0]
        if fields_dict.get("urls"):
            c_response["urls"] = record[1]
        results.append(c_response)

    response = Response(json.dumps(results, indent=2, separators=(', ', ': ')), 200, mimetype="application/json")
    return response


@urls.route("/metadata/q")
@request_args_to_params
def query_metadata(key, value, url=None, version=None, fields=None, limit="100", offset="0", **kwargs):
    """ Queries indexes by URLs metadata key and value
    Args:
        key (str): metadata key
        value (str): metadata value for key
        url (str): full url or pattern for limit to
        fields (str): comma separated list of fields to return, if not specified return all fields
        version (str): filter only records with a version number
        limit (str): max results to return
        offset (str): where to start the next query from
    """

    if kwargs:
        return Response("Unexpected Parameter(s) {}".format(kwargs), 400)

    logger.debug("unused args: {}".format(kwargs))
    records = urls.driver.query_metadata_by_key(key, value, url=url,
                                                only_versioned=version and version.lower() in ["true", "t", "yes", "y"],
                                                offset=int(offset), limit=int(limit))
    fields = fields or "did,urls,rev"
    fields_dict = requested_fields(fields)

    results = []
    for record in records:
        c_response = {}
        if fields_dict.get("did"):
            c_response["did"] = record[0]
        if fields_dict.get("urls"):
            c_response["urls"] = [record[1]]
        if fields_dict.get("rev"):
            c_response["rev"] = record[2]
        results.append(c_response)

    response = Response(json.dumps(results, indent=2, separators=(', ', ': ')), 200, mimetype="application/json")
    return response


@urls.errorhandler(UserError)
def handle_user_error(err):
    """Raised while parsing request params"""
    return Response("path: {} missing required parameter(s) {}".format(request.path, err), 400)


@urls.record
def pre_config(state):
    driver = state.app.config["INDEX"]["driver"]
    urls.driver = AlchemyURLsQueryDriver(driver)


def requested_fields(fields):
    fields_dict = {}
    provided_fields_list = fields.split(",")
    for field in provided_fields_list:
        fields_dict[field] = True

    return fields_dict
