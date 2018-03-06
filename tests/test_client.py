import json


def test_index_list(client):
    assert client.get('/index/').status_code == 200


def test_index_create(client, user):
    data = {
        'form': 'object',
        'size': 123,
        'urls': ['s3://endpointurl/bucket/key'],
        'hashes': {'md5': '8b9942cf415384b27cadf1f4d2d682e5'}}

    assert client.post(
        '/index/',
        data=json.dumps(data),
        headers=user).status_code == 200


def test_index_create_with_multiple_hashes(client, user):
    data = {
        'form': 'object',
        'size': 123,
        'urls': ['s3://endpointurl/bucket/key'],
        'hashes': {
            'md5': '8b9942cf415384b27cadf1f4d2d682e5',
            'sha1': 'fdbbca63fbec1c2b0d4eb2494ce91520ec9f55f5'
        }
    }
    assert client.post(
        '/index/',
        data=json.dumps(data),
        headers=user).status_code == 200

def test_index_create_with_valid_did(client, user):
    data = {
        'did':'3d313755-cbb4-4b08-899d-7bbac1f6e67d',
        'form': 'object',
        'size': 123,
        'urls': ['s3://endpointurl/bucket/key'],
        'hashes': {'md5': '8b9942cf415384b27cadf1f4d2d682e5'}}

    r = client.post(
        '/index/',
        data=json.dumps(data),
        headers=user)

    assert r.status_code == 200
    assert r.json['did'] == '3d313755-cbb4-4b08-899d-7bbac1f6e67d'

def test_index_create_with_invalid_did(client, user):
    data = {
        'did':'3d313755-cbb4-4b0fdfdfd8-899d-7bbac1f6e67dfdd',
        'form': 'object',
        'size': 123,
        'urls': ['s3://endpointurl/bucket/key'],
        'hashes': {'md5': '8b9942cf415384b27cadf1f4d2d682e5'}}

    assert client.post(
        '/index/',
        data=json.dumps(data),
        headers=user).status_code == 400

def test_index_create_with_prefix(client, user):
    data = {
        'did':'cdis:3d313755-cbb4-4b08-899d-7bbac1f6e67d',
        'form': 'object',
        'size': 123,
        'urls': ['s3://endpointurl/bucket/key'],
        'hashes': {'md5': '8b9942cf415384b27cadf1f4d2d682e5'}}

    r = client.post(
        '/index/',
        data=json.dumps(data),
        headers=user)

    assert r.status_code == 200
    assert r.json['did'] == 'cdis:3d313755-cbb4-4b08-899d-7bbac1f6e67d'

def test_index_create_with_duplicate_did(client, user):
    data = {
        'did':'3d313755-cbb4-4b0fdfdfd8-899d-7bbac1f6e67dfdd',
        'form': 'object',
        'size': 123,
        'urls': ['s3://endpointurl/bucket/key'],
        'hashes': {'md5': '8b9942cf415384b27cadf1f4d2d682e5'}}
    client.post(
        '/index/',
        data=json.dumps(data),
        headers=user)

    data2 = {
        'did':'3d313755-cbb4-4b0fdfdfd8-899d-7bbac1f6e67dfdd',
        'form': 'object',
        'size': 213,
        'urls': ['s3://endpointurl/bucket/key'],
        'hashes': {'md5': '469942cf415384b27cadf1f4d2d682e5'}}

    assert client.post(
        '/index/',
        data=json.dumps(data2),
        headers=user).status_code == 400

def test_index_create_with_file_name(client, user):
    data = {
        'form': 'object',
        'size': 123,
        'urls': ['s3://endpointurl/bucket/key'],
        'file_name': 'abc',
        'hashes': {'md5': '8b9942cf415384b27cadf1f4d2d682e5'}}

    r = client.post(
        '/index/',
        data=json.dumps(data),
        headers=user)
    r = client.get('/index/'+r.json['did'])
    assert r.json['file_name'] == 'abc'

def test_index_create_with_version(client, user):
    data = {
        'form': 'object',
        'size': 123,
        'urls': ['s3://endpointurl/bucket/key'],
        'file_name': 'abc',
        'version': 'ver_123',
        'hashes': {'md5': '8b9942cf415384b27cadf1f4d2d682e5'}}

    r = client.post(
        '/index/',
        data=json.dumps(data),
        headers=user)
    r = client.get('/index/'+r.json['did'])
    assert r.json['version'] == 'ver_123'


def test_index_create_with_metadata(client, user):
    data = {
        'form': 'object',
        'size': 123,
        'urls': ['s3://endpointurl/bucket/key'],
        'metadata': {
            'project_id': 'bpa-UChicago'
        },
        'hashes': {'md5': '8b9942cf415384b27cadf1f4d2d682e5'}}

    r = client.post(
        '/index/',
        data=json.dumps(data),
        headers=user)

    r = client.get('/index/'+r.json['did'])
    assert r.json['metadata'] == {
            'project_id': 'bpa-UChicago'
        }

def test_index_get_global_endpoint(client, user):
    data = {
        'form': 'object',
        'size': 123,
        'urls': ['s3://endpointurl/bucket/key'],
        'metadata': {
            'project_id': 'bpa-UChicago'
        },
        'hashes': {'md5': '8b9942cf415384b27cadf1f4d2d682e5'}}

    r = client.post(
        '/index/',
        data=json.dumps(data),
        headers=user)

    r = client.get(r.json['did'])
    assert r.status_code == 200
    assert r.json['metadata'] == {
            'project_id': 'bpa-UChicago'
        }
    assert r.json['form'] == 'object'
    assert r.json['size'] == 123
    assert r.json['urls'] == ['s3://endpointurl/bucket/key']
    assert r.json['hashes'] == {'md5': '8b9942cf415384b27cadf1f4d2d682e5'}


def test_index_update(client, user):
    data = {
        'form': 'object',
        'size': 123,
        'urls': ['s3://endpointurl/bucket/key'],
        'hashes': {'md5': '8b9942cf415384b27cadf1f4d2d682e5'}}

    r = client.post(
        '/index/',
        data=json.dumps(data),
        headers=user)
    assert r.status_code == 200
    assert 'did' in r.json
    did = r.json['did']
    assert 'rev' in r.json
    rev = r.json['rev']
    dataNew = {
        'rev': rev,
        'urls': ['s3://endpointurl/bucket/key'],
        'file_name': 'test',
        'version': 'ver123',
        }
    r2 = client.put(
        '/index/' + did + '?rev=' + rev,
        data=json.dumps(dataNew),
        headers=user)
    assert r2.status_code == 200
    assert 'rev' in r2.json
    assert r2.json['rev'] != rev

def test_index_update_prefix(client, user):
    data = {
        'did':'cdis:3d313755-cbb4-4b08-899d-7bbac1f6e67d',
        'form': 'object',
        'size': 123,
        'urls': ['s3://endpointurl/bucket/key'],
        'hashes': {'md5': '8b9942cf415384b27cadf1f4d2d682e5'}}

    r = client.post(
        '/index/',
        data=json.dumps(data),
        headers=user)
    assert r.status_code == 200
    assert 'did' in r.json
    did = r.json['did']
    assert 'rev' in r.json
    rev = r.json['rev']
    dataNew = {
        'rev': rev,
        'urls': ['s3://endpointurl/bucket/key'],
        'file_name': 'test',
        'version': 'ver123',
        }
    r2 = client.put(
        '/index/' + did + '?rev=' + rev,
        data=json.dumps(dataNew),
        headers=user)
    assert r2.status_code == 200
    assert 'rev' in r2.json
    assert r2.json['rev'] != rev

def test_index_delete(client, user):
    data = {
        'form': 'object',
        'size': 123,
        'urls': ['s3://endpointurl/bucket/key'],
        'hashes': {'md5': '8b9942cf415384b27cadf1f4d2d682e5'}}

    r = client.post(
        '/index/',
        data=json.dumps(data),
        headers=user)
    assert r.status_code == 200
    assert 'did' in r.json
    did = r.json['did']
    assert 'rev' in r.json
    rev = r.json['rev']
    r = client.get('/index/' + did)
    assert r.status_code == 200
    r = client.delete('/index/' + did + '?rev=' + rev, headers=user)
    assert r.status_code == 200
    r = client.get('/index/' + did)
    assert r.status_code == 404

def test_create_index_version(client, user):
    data = {
        'form': 'object',
        'size': 123,
        'urls': ['s3://endpointurl/bucket/key'],
        'hashes': {'md5': '8b9942cf415384b27cadf1f4d2d682e5'}}

    r = client.post(
        '/index/',
        data=json.dumps(data),
        headers=user)

    assert r.status_code == 200
    assert 'did' in r.json
    did = r.json['did']
    assert 'baseid' in r.json
    baseid = r.json['baseid']

    dataNew = {
        'form': 'object',
        'size': 244,
        'urls': ['s3://endpointurl/bucket2/key'],
        'hashes': {'md5': '8b9942cf415384b27cadf1f4d2d981f5'},
        }

    r2 = client.post(
        '/index/' + did,
        data=json.dumps(dataNew),
        headers=user)
    assert r2.status_code == 200
    assert 'baseid' in r2.json
    assert r2.json['baseid'] == baseid
    assert 'rev' in r2.json

def test_get_latest_version(client, user):
    data = {
        'form': 'object',
        'size': 123,
        'urls': ['s3://endpointurl/bucket/key'],
        'hashes': {'md5': '8b9942cf415384b27cadf1f4d2d682e5'}}

    r = client.post(
        '/index/',
        data=json.dumps(data),
        headers=user)

    assert r.status_code == 200
    assert 'did' in r.json
    did = r.json['did']

    r2 = client.get(
        '/index/' + did + '/latest',
        )

    assert r2.status_code == 200

def test_get_all_versions(client, user):
    data = {
        'form': 'object',
        'size': 123,
        'urls': ['s3://endpointurl/bucket/key'],
        'hashes': {'md5': '8b9942cf415384b27cadf1f4d2d682e5'}}

    r = client.post(
        '/index/',
        data=json.dumps(data),
        headers=user)

    assert r.status_code == 200
    assert 'did' in r.json
    did = r.json['did']

    r2 = client.get(
        '/index/' + did + '/versions',
        )

    assert r2.status_code == 200

def test_alias_list(client):
    assert client.get('/alias/').status_code == 200


def test_alias_create(client, user):
    data = {
        'size': 123,
        'hashes': {'md5': '8b9942cf415384b27cadf1f4d2d682e5'},
        'release': 'private',
        'keeper_authority': 'CRI', 'host_authorities': ['PDC'],
    }
    ark = 'ark:/31807/TEST-abc'

    r = client.put(
        '/alias/' + ark,
        data=json.dumps(data),
        headers=user)
    assert r.json['name'] == ark
    assert 'rev' in r.json

    aliases = client.get('/alias/').json['aliases']
    assert len(aliases) == 1
    assert aliases[0] == ark


def test_alias_get_global_endpoint(client, user):
    data = {
        'size': 123,
        'hashes': {'md5': '8b9942cf415384b27cadf1f4d2d682e5'},
        'release': 'private',
        'keeper_authority': 'CRI', 'host_authorities': ['PDC'],
    }
    ark = 'ark:/31807/TEST-abc'

    r = client.put(
        '/alias/' + ark,
        data=json.dumps(data),
        headers=user)
    r = client.get('/' + ark)
    assert r.status_code == 200
    assert r.json['size'] == 123

def test_alias_update(client, user):
    data = {
        'size': 123,
        'hashes': {'md5': '8b9942cf415384b27cadf1f4d2d682e5'},
        'release': 'private',
        'keeper_authority': 'CRI', 'host_authorities': ['PDC'],
    }
    ark = 'ark:/31807/TEST-abc'

    r = client.put(
        '/alias/' + ark,
        data=json.dumps(data),
        headers=user)
    assert r.status_code == 200
    assert 'rev' in r.json
    rev = r.json['rev']

    dataNew = {
        'size': 456,
        'hashes': {'md5': '8b9942cf415384b27cadf1f4d2d682e5'},
        'release': 'private',
        'keeper_authority': 'CRI', 'host_authorities': ['PDC'],
    }
    r = client.put(
        '/alias/' + ark + '?rev=' + rev,
        data=json.dumps(dataNew),
        headers=user)
    assert r.status_code == 200
    assert r.json['rev'] != rev

def test_alias_delete(client, user):
    data = {
        'size': 123,
        'hashes': {'md5': '8b9942cf415384b27cadf1f4d2d682e5'},
        'release': 'private',
        'keeper_authority': 'CRI', 'host_authorities': ['PDC'],
    }
    ark = 'ark:/31807/TEST-abc'

    r = client.put(
        '/alias/' + ark,
        data=json.dumps(data),
        headers=user)
    assert r.status_code == 200
    assert 'rev' in r.json
    rev = r.json['rev']

    r = client.delete(
        '/alias/' + ark + '?rev=' + rev,
        headers=user)
    assert r.status_code == 200

    r = client.get(
        '/alias/' + ark,
        headers=user)
    assert r.status_code == 404
