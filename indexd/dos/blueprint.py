import re
import flask
import jsonschema

from indexd.blueprint import dist_get_record

from indexd.errors import AuthError
from indexd.errors import UserError
from indexd.alias.errors import NoRecordFound as AliasNoRecordFound
from indexd.index.errors import NoRecordFound as IndexNoRecordFound

blueprint = flask.Blueprint('dos', __name__)

blueprint.config = dict()
blueprint.index_driver = None
blueprint.alias_driver = None

@blueprint.route('/ga4gh/dos/v1/dataobjects/<path:record>', methods=['GET'])
def get_dos_record(record):
    '''
    Returns a record from the local ids, alias, or global resolvers.
    Returns DOS Schema
    '''

    try:
        ret = blueprint.index_driver.get(record)
    except IndexNoRecordFound:
        try:
            ret = blueprint.index_driver.get_by_alias(record)
        except IndexNoRecordFound:
            try:
                ret = blueprint.alias_driver.get(record)
            except AliasNoRecordFound:
                if not blueprint.dist:
                    raise
                ret = dist_get_record(record)

    return flask.jsonify(indexd_to_dos(ret)), 200

def indexd_to_dos(record):
    data_object = {
        "id": record['did'],
        "name": record['file_name'],
        'created': record['created_date'],
        'updated': record['updated_date'],
        "size": record['size'],
        "version": record['rev'],
        "description": "",
        "mime_type": ""
    }

    data_object['aliases']: []

    # parse out checksums
    data_object['checksums'] = []
    for k in record['hashes']:
        data_object['checksums'].append(
            {'checksum': record['hashes'][k], 'type': k})

    # parse out the urls
    data_object['urls'] = []
    for url in record['urls']:
        data_object['urls'].append({
            'url': url,
            'system_metadata': None,
            'user_metadata': record['metadata']})

    return data_object


@blueprint.errorhandler(UserError)
def handle_user_error(err):
    ret = { msg = str(err), status_code = 0 }
    return flask.jsonify(ret), 400

@blueprint.errorhandler(AuthError)
def handle_auth_error(err):
    ret = { msg = str(err), status_code = 0 }
    return flask.jsonify(ret), 403

@blueprint.errorhandler(AliasNoRecordFound)
def handle_no_record_error(err):
    ret = { msg = str(err), status_code = 0 }
    return flask.jsonify(ret), 404

@blueprint.errorhandler(IndexNoRecordFound)
def handle_no_record_error(err):
    ret = { msg = str(err), status_code = 0 } 
    return flask.jsonify(ret), 404

@blueprint.record
def get_config(setup_state):
    index_config = setup_state.app.config['INDEX']
    alias_config = setup_state.app.config['ALIAS']
    blueprint.index_driver = index_config['driver']
    blueprint.alias_driver = alias_config['driver']
