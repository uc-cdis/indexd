import re
import flask
import jsonschema

from . import schema
from . import errors


blueprint = flask.Blueprint('index', __name__)

blueprint.config = dict()
blueprint.index_driver = None
blueprint.alias_driver = None


URI_REGEX = r'^index:(?P<alias>.+)$'
uri_regex = re.compile(URI_REGEX)


@blueprint.route('/index', methods=['GET'])
def get_readme():
    '''
    Returns a JSON hyperschema response.
    '''
    return flask.jsonify(schema.HYPER), 200

@blueprint.route('/index/', methods=['GET'])
def get_index():
    '''
    Returns a list of records.
    '''
    limit = int(flask.request.args.get('limit', 100))
    start = str(flask.request.args.get('start', ''))

    if limit < 0 or limit > 1024:
        raise errors.UserError('limit must be between 0 and 1024')

    base = {
        'ids': blueprint.index_driver.ids(limit=limit, start=start),
        'limit': limit,
        'start': start,
    }

    return flask.jsonify(base), 200

@blueprint.route('/index/<record>', methods=['GET'])
def get_index_record(record):
    '''
    Returns a record.
    '''
    match = uri_regex.match(record)
    alias = match.group('alias') if match is not None else record

    record = blueprint.alias_driver.get(alias, record)

    ret = blueprint.index_driver.get(record)

    return flask.jsonify(ret), 200

@blueprint.route('/index/', methods=['POST'])
def post_index_record():
    '''
    Create a new record.
    '''
    jsonschema.validate(flask.request.json, schema.POST_RECORD)

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

@blueprint.route('/alias/<alias>', methods=['GET'])
def get_alias(alias):
    '''
    Retrieve the index record for a given alias.
    '''
    ret = {
        'alias': alias,
        'record': blueprint.alias_driver.get(alias),
    }

    return flask.jsonify(ret), 200

@blueprint.route('/alias/<alias>', methods=['PUT'])
def put_alias(alias):
    '''
    Add a alias for a given index record.
    '''
    try: record = flask.request.args['record']
    except KeyError as err:
        raise errors.UserError('no alias specified')

    blueprint.alias_driver.add(alias, record)

    ret = {
        alias: record,
    }

    return flask.jsonify(ret), 200

@blueprint.route('/alias/<alias>', methods=['DELETE'])
def delete_alias(alias):
    '''
    Delete a alias.
    '''
    blueprint.alias_driver.delete(alias)

    return '', 200

@blueprint.route('/alias/<alias>', methods=['POST'])
def update_alias(alias):
    '''
    Update a alias.
    '''
    record = flask.request.args.get('record')

    blueprint.alias_driver.update(alias, record)

    return '', 200

@blueprint.errorhandler(errors.NoRecordError)
def handle_no_record_error(err):
    return flask.jsonify(error=str(err)), 404

@blueprint.errorhandler(errors.NoAliasError)
def handle_no_alias_error(err):
    return flask.jsonify(error=str(err)), 404

@blueprint.errorhandler(errors.MultipleRecordsError)
def handle_multiple_records_error(err):
    return flask.jsonify(error=str(err)), 409

@blueprint.errorhandler(errors.AliasExistsError)
def handle_alias_exists_error(err):
    return flask.jsonify(error=str(err)), 409

@blueprint.errorhandler(errors.PermissionError)
def handle_permission_error(err):
    return flask.jsonify(error=str(err)), 401

@blueprint.errorhandler(errors.UserError)
def handle_user_error(err):
    return flask.jsonify(error=str(err)), 400

@blueprint.errorhandler(jsonschema.ValidationError)
def handle_validation_error(err):
    print(err.message)
    return flask.jsonify(err.message), 400

@blueprint.record
def get_config(setup_state):
    config = setup_state.app.config
    index_config = config.get('INDEX', {})
    alias_config = config.get('ALIAS', {})

    try: blueprint.index_driver = index_config['driver'](**index_config)
    except Exception as err:
        raise errors.ConfigurationError(err)
    try: blueprint.alias_driver = alias_config['driver'](**alias_config)
    except Exception as err:
        raise errors.ConfigurationError(err)
