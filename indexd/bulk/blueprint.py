"""Bulk operations for indexd"""
import json

import flask
from sqlalchemy.orm import joinedload

from indexd.errors import UserError
from indexd.index.drivers.alchemy import (
    IndexRecord,
    IndexRecordUrlMetadataJsonb,
)

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

    docs = []
    with blueprint.index_driver.session as session:
        # Comment it out to compare against the eager loading option.
        # query = session.query(IndexRecord)
        # query = query.filter(IndexRecord.did.in_(ids)

        # Use eager loading.
        query = session.query(IndexRecord)
        query = query.options(joinedload(IndexRecord.urls_metadata))
        query = query.options(joinedload(IndexRecord.acl))
        query = query.options(joinedload(IndexRecord.hashes))
        query = query.options(joinedload(IndexRecord.aliases))
        query = query.filter(IndexRecord.did.in_(ids))

        docs = [q.to_document_dict() for q in query]

    return json.dumps(docs), 200


@blueprint.route('/bulk/documents/latest', methods=['POST'])
def bulk_get_latest_documents():
    """
    From the given list of dids, get the latest version docs
    """

    ids = flask.request.json
    if not ids:
        raise UserError('No ids provided')
    if not isinstance(ids, list):
        raise UserError('ids is not a list')

    skip_null = flask.request.args.get('skip_null', "false").lower() in ["true", "t"]

    docs = blueprint.index_driver.bulk_get_latest_versions(ids, skip_null=skip_null)
    return json.dumps(docs), 200


@blueprint.record
def get_config(setup_state):
    config = setup_state.app.config['INDEX']
    blueprint.index_driver = config['driver']
