import re
import flask
import jsonschema
import os.path
import subprocess
from .version_data import VERSION, COMMIT

from indexd.auth import authorize

from indexd.errors import AuthError
from indexd.errors import UserError

from .schema import PUT_RECORD_SCHEMA
from .schema import POST_RECORD_SCHEMA

from .errors import NoRecordFound
from .errors import MultipleRecordsFound
from .errors import RevisionMismatch
from .errors import UnhealthyCheck

blueprint = flask.Blueprint('index', __name__)

blueprint.config = dict()
blueprint.index_driver = None

ACCEPTABLE_HASHES = {
    'md5': re.compile(r'^[0-9a-f]{32}$').match,
    'sha1': re.compile(r'^[0-9a-f]{40}$').match,
    'sha256': re.compile(r'^[0-9a-f]{64}$').match,
    'sha512': re.compile(r'^[0-9a-f]{128}$').match,
}


def validate_hashes(**hashes):
    '''
    Validate hashes against known and valid hashing algorithms.
    '''
    if not all(h in ACCEPTABLE_HASHES for h in hashes):
        raise UserError('invalid hash types specified')

    if not all(ACCEPTABLE_HASHES[h](v) for h, v in hashes.items()):
        raise UserError('invalid hash values specified')


@blueprint.route('/index/', methods=['GET'])
def get_index():
    '''
    Returns a list of records.
    '''
    limit = flask.request.args.get('limit')
    try: limit = 100 if limit is None else int(limit)
    except ValueError as err:
        raise UserError('limit must be an integer')

    if limit <= 0 or limit > 1024:
        raise UserError('limit must be between 1 and 1024')

    size = flask.request.args.get('size')
    try: size = size if size is None else int(size)
    except ValueError as err:
        raise UserError('size must be an integer')

    if size is not None and size < 0:
        raise UserError('size must be > 0')

    start = flask.request.args.get('start')

    urls = flask.request.args.getlist('url')

    file_name = flask.request.args.get('file_name')

    version = flask.request.args.get('version')

    hashes = flask.request.args.getlist('hash')
    hashes = {h: v for h, v in map(lambda x: x.split(':', 1), hashes)}

    validate_hashes(**hashes)
    hashes = hashes if hashes else None

    if limit < 0 or limit > 1024:
        raise UserError('limit must be between 0 and 1024')

    ids = blueprint.index_driver.ids(
        start=start,
        limit=limit,
        size=size,
        file_name=file_name,
        version=version,
        urls=urls,
        hashes=hashes,
    )

    base = {
        'ids': ids,
        'limit': limit,
        'start': start,
        'size': size,
        'file_name': file_name,
        'version': version,
        'urls': urls,
        'hashes': hashes,
    }

    return flask.jsonify(base), 200


@blueprint.route('/urls/', methods=['GET'])
def get_urls():
    '''
    Returns a list of urls.
    '''
    hashes = flask.request.args.getlist('hash')
    hashes = {h: v for h, v in map(lambda x: x.split(':', 1), hashes)}

    try: size = int(flask.request.args.get('size'))
    except TypeError as err:
        raise UserError('size must be an integer')

    try: start = int(flask.request.args.get('start', 0))
    except TypeError as err:
        raise UserError('start must be an integer')

    try: limit = int(flask.request.args.get('limit', 100))
    except TypeError as err:
        raise UserError('limit must be an integer')

    if size < 0:
        raise UserError('size must be >= 0')

    if start < 0:
        raise UserError('start must be >= 0')

    if limit < 0:
        raise UserError('limit must be >= 0')

    if limit > 1024:
        raise UserError('limit must be <= 1024')

    validate_hashes(**hashes)

    urls = blueprint.index_driver.hashes_to_urls(
        size=size,
        hashes=hashes,
        start=start,
        limit=limit,
    )

    ret = {
        'urls': urls,
        'limit': limit,
        'start': start,
        'size': size,
        'hashes': hashes,
    }

    return flask.jsonify(ret), 200


@blueprint.route('/index/<record>', methods=['GET'])
def get_index_record(record):
    '''
    Returns a record.
    '''
    ret = blueprint.index_driver.get(record)

    return flask.jsonify(ret), 200


@blueprint.route('/index/', methods=['POST'])
@authorize
def post_index_record():
    '''
    Create a new record.
    '''
    try: jsonschema.validate(flask.request.json, POST_RECORD_SCHEMA)
    except jsonschema.ValidationError as err:
        raise UserError(err)

    did = flask.request.json.get('did')
    form = flask.request.json['form']
    size = flask.request.json['size']
    urls = flask.request.json['urls']

    hashes = flask.request.json['hashes']
    file_name = flask.request.json.get('file_name')
    metadata = flask.request.json.get('metadata')
    version = flask.request.json.get('version')

    did, rev, baseid = blueprint.index_driver.add(
        form,
        did,
        size=size,
        file_name=file_name,
        metadata=metadata,
        version=version,
        urls=urls,
        hashes=hashes,
    )

    ret = {
        'did': did,
        'rev': rev,
        'baseid': baseid,
    }

    return flask.jsonify(ret), 200


@blueprint.route('/index/<record>', methods=['PUT'])
@authorize
def put_index_record(record):
    '''
    Update an existing record.
    '''
    try:
        jsonschema.validate(flask.request.json, PUT_RECORD_SCHEMA)
    except jsonschema.ValidationError as err:
        raise UserError(err)

    rev = flask.request.args.get('rev')
    file_name = flask.request.json.get('file_name')
    version = flask.request.json.get('version')
    urls = flask.request.json.get('urls')

    did, baseid, rev = blueprint.index_driver.update(
        record,
        rev,
        file_name=file_name,
        version=version,
        urls=urls,
    )

    ret = {
        'did': did,
        'baseid': baseid,
        'rev': rev,
    }

    return flask.jsonify(ret), 200


@blueprint.route('/index/<record>', methods=['DELETE'])
@authorize
def delete_index_record(record):
    '''
    Delete an existing record.
    '''
    rev = flask.request.args.get('rev')
    if rev is None:
        raise UserError('no revision specified')

    blueprint.index_driver.delete(record, rev)

    return '', 200


@blueprint.route('/index/<record>', methods=['POST'])
@authorize
def add_index_record_version(record):
    '''
    Add a record version
    '''
    form = flask.request.json['form']
    size = flask.request.json['size']
    urls = flask.request.json['urls']
    hashes = flask.request.json['hashes']
    file_name = flask.request.json.get('file_name', None)
    metadata = flask.request.json.get('metadata', None)
    version = flask.request.json.get('version', None)

    did, baseid,rev = blueprint.index_driver.add_version(
        record,
        form,
        size=size,
        urls=urls,
        file_name=file_name,
        metadata=metadata,
        version=version,
        hashes=hashes,
    )

    ret = {
        'did': did,
        'baseid': baseid,
        'rev': rev,
    }

    return flask.jsonify(ret), 200


@blueprint.route('/index/<record>/versions', methods=['GET'])
def get_all_index_record_versions(record):
    '''
    Get all record versions
    '''
    ret = blueprint.index_driver.get_all_versions(record)

    return flask.jsonify(ret), 200


@blueprint.route('/index/<record>/latest', methods=['GET'])
def get_latest_index_record_versions(record):
    '''
    Get the latest record version
    '''
    ret = blueprint.index_driver.get_latest_version(record)

    return flask.jsonify(ret), 200


@blueprint.route('/_status', methods=['GET'])
def health_check():
    '''
    Health Check.
    '''
    blueprint.index_driver.health_check()

    return 'Healthy', 200


@blueprint.route('/_stats', methods=['GET'])
def stats():
    '''
    Return indexed data stats.
    '''

    filecount = blueprint.index_driver.len()
    totalfilesize = blueprint.index_driver.totalbytes()

    base = {
        'fileCount': filecount,
        'totalFileSize': totalfilesize,
    }

    return flask.jsonify(base), 200


@blueprint.route('/_version', methods=['GET'])
def version():
    '''
    Return the version of this service.
    '''

    base = {
        'version': VERSION,
        'commit': COMMIT,
    }

    return flask.jsonify(base), 200


@blueprint.errorhandler(NoRecordFound)
def handle_no_record_error(err):
    return flask.jsonify(error=str(err)), 404


@blueprint.errorhandler(MultipleRecordsFound)
def handle_multiple_records_error(err):
    return flask.jsonify(error=str(err)), 409


@blueprint.errorhandler(UserError)
def handle_user_error(err):
    return flask.jsonify(error=str(err)), 400


@blueprint.errorhandler(AuthError)
def handle_auth_error(err):
    return flask.jsonify(error=str(err)), 403


@blueprint.errorhandler(RevisionMismatch)
def handle_revision_mismatch(err):
    return flask.jsonify(error=str(err)), 409


@blueprint.errorhandler(UnhealthyCheck)
def handle_unhealthy_check(err):
    return "Unhealthy", 500


@blueprint.record
def get_config(setup_state):
    config = setup_state.app.config['INDEX']
    blueprint.index_driver = config['driver']
