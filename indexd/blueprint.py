import re
import flask
import jsonschema

from indexd.errors import UserError
from indexd.errors import PermissionError


blueprint = flask.Blueprint('cross', __name__)

blueprint.config = dict()
blueprint.index_driver = None
blueprint.alias_driver = None


@blueprint.route('/alias/<path:alias>', methods=['GET'])
def get_alias(alias):
    '''
    Return alias associated information.
    '''
    info = blueprint.alias_driver.get(alias)

    start = 0
    limit = 100

    size = info['size']
    hashes = info['hashes']

    urls = blueprint.index_driver.hashes_to_urls(
        size=size,
        hashes=hashes,
        start=start,
        limit=limit,
    )

    info.update({
        'urls': urls,
        'start': start,
        'limit': limit,
    })

    return flask.jsonify(info), 200

@blueprint.errorhandler(UserError)
def handle_user_error(err):
    return flask.jsonify(error=str(err)), 400

@blueprint.errorhandler(PermissionError)
def handle_permission_error(err):
    return flask.jsonify(error=str(err)), 403

@blueprint.record
def get_config(setup_state):
    index_config = setup_state.app.config['INDEX']
    alias_config = setup_state.app.config['ALIAS']
    blueprint.index_driver = index_config['driver']
    blueprint.alias_driver = alias_config['driver']
