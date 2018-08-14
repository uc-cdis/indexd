"""Bulk operations for indexd"""
import json

import flask

from indexd.errors import UserError
from indexd.index.drivers.alchemy import IndexRecord

blueprint = flask.Blueprint('bulk', __name__)

blueprint.config = dict()
blueprint.index_driver = None


@blueprint.route('/bulk/documents', methods=['POST'])
def bulk_get_documents():
    """
    Returns a list of records.
    """
    ids = flask.request.json
    if not ids:
        raise UserError('No ids provided')
    if not isinstance(ids, list):
        raise UserError('ids is not a list')

    with blueprint.index_driver.session as session:
        query = session.query(IndexRecord)
        query = query.filter(IndexRecord.did.in_(ids))

    docs = [q.to_document_dict() for q in query]
    return json.dumps(docs), 200


@blueprint.record
def get_config(setup_state):
    config = setup_state.app.config['INDEX']
    blueprint.index_driver = config['driver']
