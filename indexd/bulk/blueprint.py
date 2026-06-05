"""Bulk operations for indexd"""
import json

import flask

from indexd.errors import UserError
from indexd.index.drivers.alchemy import IndexRecord, IndexRecordUrl
from sqlalchemy.orm import joinedload


blueprint = flask.Blueprint("bulk", __name__)

blueprint.config = dict()
blueprint.index_driver = None


@blueprint.route("/bulk/documents", methods=["POST"])
def bulk_get_documents():
    """
    Returns a list of records.
    """
    ids = flask.request.json
    if not ids:
        raise UserError("No ids provided")
    if not isinstance(ids, list):
        raise UserError("ids is not a list")

    # ensure strings
    guids = [str(guid) for guid in ids]

    docs = blueprint.index_driver.get_bulk(guid_list=guids)

    return flask.Response(json.dumps(docs), 200, mimetype="application/json")


@blueprint.record
def get_config(setup_state):
    config = setup_state.app.config["INDEX"]
    blueprint.index_driver = config["driver"]
