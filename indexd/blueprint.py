import re
import flask
import jsonschema

from indexclient.client import IndexClient
from doiclient.client import DOIClient

from indexd.utils import hint_match

from indexd.errors import AuthError
from indexd.errors import UserError
from indexd.alias.errors import NoRecordFound as AliasNoRecordFound
from indexd.index.errors import NoRecordFound as IndexNoRecordFound

blueprint = flask.Blueprint('cross', __name__)

blueprint.config = dict()
blueprint.index_driver = None
blueprint.alias_driver = None
blueprint.dist = []

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

@blueprint.route('/<path:record>', methods=['GET'])
def get_record(record):
    '''
    Returns a record from the local ids, alias, or global resolvers.
    '''

    try:
        ret = blueprint.index_driver.get(record)
    except IndexNoRecordFound:
        try:
            ret = blueprint.alias_driver.get(record)
        except AliasNoRecordFound:
            if not blueprint.dist or 'no_dist' in flask.request.args:
                raise
            return dist_get_index_record(record)


    return flask.jsonify(ret), 200

def dist_get_index_record(record):
    sorted_dist = sorted(blueprint.dist, key=lambda k: hint_match(record, k['hints']), reverse=True)

    for indexd in sorted_dist:
        try:
            if indexd['type'] == "doi":
                signpost = DOIClient(baseurl=indexd['host'])
                res = signpost.get(record)
            else:
                signpost = IndexClient(baseurl=indexd['host'])
                res = signpost.global_get(record)
        except:
            # a lot of things can go wrong with the get, but in general we don't care here.
            continue

        if res:
            json = res.to_json()
            json['from_index_service'] = {
                'host': indexd['host'],
                'name': indexd['name'],
            }
            return flask.jsonify(json), 200

    raise IndexNoRecordFound('no record found')


@blueprint.errorhandler(UserError)
def handle_user_error(err):
    return flask.jsonify(error=str(err)), 400

@blueprint.errorhandler(AuthError)
def handle_auth_error(err):
    return flask.jsonify(error=str(err)), 403

@blueprint.errorhandler(AliasNoRecordFound)
def handle_no_record_error(err):
    return flask.jsonify(error=str(err)), 404

@blueprint.errorhandler(IndexNoRecordFound)
def handle_no_record_error(err):
    return flask.jsonify(error=str(err)), 404

@blueprint.record
def get_config(setup_state):
    index_config = setup_state.app.config['INDEX']
    alias_config = setup_state.app.config['ALIAS']
    blueprint.index_driver = index_config['driver']
    blueprint.alias_driver = alias_config['driver']
    blueprint.dist = setup_state.app.config['DIST']
