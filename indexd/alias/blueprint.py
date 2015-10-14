import re
import flask

from indexd.errors import UserError

from .errors import NoAliasError
from .errors import AliasExistsError
from .errors import AliasConfigurationError


blueprint = flask.Blueprint('alias', __name__)

blueprint.config = dict()
blueprint.alias_driver = None

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
        raise UserError('no alias specified')

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

@blueprint.errorhandler(NoAliasError)
def handle_no_alias_error(err):
    return flask.jsonify(error=str(err)), 404

@blueprint.errorhandler(AliasExistsError)
def handle_alias_exists_error(err):
    return flask.jsonify(error=str(err)), 409

@blueprint.record
def get_config(setup_state):
    config = setup_state.app.config
    alias_config = config.get('ALIAS', {})

    try: blueprint.alias_driver = alias_config['driver'](**alias_config)
    except Exception as err:
        raise AliasConfigurationError(err)
