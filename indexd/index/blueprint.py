import re
import flask
import jsonschema

from indexd.errors import UserError

from .schema import POST_RECORD_SCHEMA

from .errors import NoRecordError
from .errors import MultipleRecordsError
from .errors import IndexConfigurationError


blueprint = flask.Blueprint('index', __name__)

blueprint.config = dict()
blueprint.index_driver = None

@blueprint.route('/index/', methods=['GET'])
def get_index():
    '''
    Returns a list of records.
    '''
    limit = int(flask.request.args.get('limit', 100))
    start = str(flask.request.args.get('start', ''))

    hashes = flask.request.args.getlist('hash')
    hashes = [tuple(h.split(':', 1)) for h in hashes]

    if limit < 0 or limit > 1024:
        raise UserError('limit must be between 0 and 1024')

    ids = blueprint.index_driver.ids(
        limit=limit,
        start=start,
#        hashes=hashes,
    )

    base = {
        'ids': ids,
        'limit': limit,
        'start': start,
        'hashes': hashes,
    }

    return flask.jsonify(base), 200

@blueprint.route('/index/<record>', methods=['GET'])
def get_index_record(record):
    '''
    Returns a record.
    '''
    ret = blueprint.index_driver.get(record)

    return flask.jsonify(ret), 200

@blueprint.route('/index/', methods=['POST'])
def post_index_record():
    '''
    Create a new record.
    '''
    try: jsonschema.validate(flask.request.json, POST_RECORD_SCHEMA)
    except jsonschema.ValidationError as err:
        raise UserError(err)

    record = blueprint.index_driver.add(flask.request.json)

    ret = {
        'record': record,
    }

    return flask.jsonify(ret), 200

@blueprint.route('/index/<record>', methods=['PUT'])
def put_index_record(record):
    '''
    Update an existing record.
    '''
    blueprint.index_driver.update(record, flask.request.json)

    return '', 200

@blueprint.route('/index/<record>', methods=['DELETE'])
def delete_index_record(record):
    '''
    Delete an existing sign.
    '''
    blueprint.index_driver.delete(record)

    return '', 200

@blueprint.errorhandler(NoRecordError)
def handle_no_record_error(err):
    return flask.jsonify(error=str(err)), 404

@blueprint.errorhandler(MultipleRecordsError)
def handle_multiple_records_error(err):
    return flask.jsonify(error=str(err)), 409

@blueprint.record
def get_config(setup_state):
    config = setup_state.app.config
    index_config = config.get('INDEX', {})

    try: blueprint.index_driver = index_config['driver'](**index_config)
    except Exception as err:
        raise IndexConfigurationError(err)
