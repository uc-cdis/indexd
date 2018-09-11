from flask import Blueprint, jsonify

from indexd.index import request_args_to_params
from indexd.index.drivers.query.urls import AlchemyURLsQueryDriver

urls = Blueprint("urls", __name__)

query_driver = None


@urls.route("/q", methods=["GET"])
@request_args_to_params
def query(exclude=None, include=None, version=True, fields=None, limit=100, offset=0, **kwargs):
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
    
    return jsonify(dict(id="AAAAA")), 200


@urls.route("/metadata/q")
@request_args_to_params
def query_metadata(url, key, value, version=True, fields=None, limit=100, offset=0, **kwargs):
    """ Queries indexes by URLs metadata key and value
    Args:
        key (str): metadata key
        value (str): metadata value for key
        url (str): full url or pattern for limit to
        fields (str): comma separated list of fields to return, if not specified return all fields
        version (bool): filter only records with a version number
        limit (int): max results to return
        offset (int): where to start the next query from
    """
    return jsonify(dict(id="AAA")), 200


@urls.errorhandler(Exception)
def handle_unhealthy_check(err):
    print(err)
    return "It's pay time", 500


@urls.record
def pre_config(state):
    driver = state.app.config["INDEX"]["driver"]
    query_driver = AlchemyURLsQueryDriver(driver)
