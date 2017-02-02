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
        'size': 456,
        'urls': ['s3://endpointurl/bucket/key'],
        'hashes': {'md5': '8b9942cf415384b27cadf1f4d2d682e5'}}
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

def test_alias_list(client):
    assert client.get('/alias/').status_code == 200


def test_alias_create(client, user):
    data = {
        'size': 123,
        'hashes': {'md5': '8b9942cf415384b27cadf1f4d2d682e5'},
        'release': 'private',
        'keeper_authority': 'CRI', 'host_authority': ['PDC'],
        'urls': [],
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
