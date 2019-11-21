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

    with blueprint.index_driver.session as session:
        # Comment it out to compare against the eager loading option.
        # query = session.query(IndexRecord)
        # query = query.filter(IndexRecord.did.in_(ids)

        # Use eager loading.
        query = session.query(IndexRecord)
        query = query.options(
            joinedload(IndexRecord.urls).joinedload(IndexRecordUrl.url_metadata)
        )
        query = query.options(joinedload(IndexRecord.acl))
        query = query.options(joinedload(IndexRecord.authz))
        query = query.options(joinedload(IndexRecord.hashes))
        query = query.options(joinedload(IndexRecord.index_metadata))
        query = query.options(joinedload(IndexRecord.aliases))
        query = query.filter(IndexRecord.did.in_(ids))

    docs = [q.to_document_dict() for q in query]
    return flask.Response(json.dumps(docs), 200, mimetype="application/json")


@blueprint.record
def get_config(setup_state):
    config = setup_state.app.config["INDEX"]
    blueprint.index_driver = config["driver"]
