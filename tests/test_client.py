import json

import pytest
from swagger_client.rest import ApiException

from indexd.index.blueprint import ACCEPTABLE_HASHES


def test_index_list(swg_index_client):
    r = swg_index_client.list_entries()
    assert r.ids == []


def test_index_list_with_params(client, user):
    data = {
        'form': 'object',
        'size': 123,
        'urls': ['s3://endpointurl/bucket/key'],
        'metadata': {
            'project_id': 'bpa-UChicago'
        },
        'hashes': {'md5': '8b9942cf415384b27cadf1f4d2d682e5'}}

    r_1 = client.post(
        '/index/',
        data=json.dumps(data),
        headers=user)

    data['metadata'] = {'project_id': 'other-project'}

    r_2 = client.post(
        '/index/',
        data=json.dumps(data),
        headers=user)

    r = client.get('/index/?metadata=project_id%3Abpa-UChicago')
    assert r_1.json['did'] in r.json['ids']

    r = client.get('/index/?metadata=project_id%3Aother-project')
    assert r_2.json['did'] in r.json['ids']

    r = client.get('/index/?hashes=md5%3A8b9942cf415384b27cadf1f4d2d682e5')
    assert r_1.json['did'] in r.json['ids']
    assert r_2.json['did'] in r.json['ids']


def test_index_create(swg_index_client):
    data = {
        'form': 'object',
        'size': 123,
        'urls': ['s3://endpointurl/bucket/key'],
        'hashes': {'md5': '8b9942cf415384b27cadf1f4d2d682e5'},
        'baseid': 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa',
    }

    result = swg_index_client.add_entry(data)
    assert result.did
    assert result.baseid == data['baseid']

def test_delete_and_recreate(swg_index_client):
    """
    Test that you can delete an IndexDocument and be able to
    recreate it with the same fields.
    """

    old_data = {
        'form': 'object',
        'size': 123,
        'urls': ['s3://endpointurl/bucket/key'],
        'hashes': {'md5': '8b9942cf415384b27cadf1f4d2d682e5'},
        'baseid': 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa',
    }
    new_data = {
        'did': None, # populated after one is assigned
        'form': 'object',
        'size': 321,
        'urls': ['s3://endpointurl/bucket/key2'],
        'hashes': {'md5': '11111111111111111111111111111111'},
        'baseid': 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa',
    }

    old_result = swg_index_client.add_entry(old_data)
    assert old_result.did
    assert old_result.baseid == old_data['baseid']

    # create a new doc with the same did
    new_data['did'] = old_result.did

    # delete the old doc
    swg_index_client.delete_entry(old_result.did, old_result.rev)
    with pytest.raises(ApiException):
        # make sure it's deleted
        swg_index_client.get_entry(old_result.did)

    # create new doc with the same baseid and did
    new_result = swg_index_client.add_entry(new_data)

    assert new_result.did
    # verify that they are the same
    assert new_result.baseid == new_data['baseid']
    assert new_result.did == old_result.did
    assert new_result.baseid == old_result.baseid

    # verify that new data is in the new node
    new_doc = swg_index_client.get_entry(new_result.did)
    assert new_data['baseid'] == new_doc.baseid
    assert new_data['urls'] == new_doc.urls
    assert new_data['hashes']['md5'] == new_doc.hashes.md5


def test_index_create_with_multiple_hashes(swg_index_client):
    data = {
        'form': 'object',
        'size': 123,
        'urls': ['s3://endpointurl/bucket/key'],
        'hashes': {
            'md5': '8b9942cf415384b27cadf1f4d2d682e5',
            'sha1': 'fdbbca63fbec1c2b0d4eb2494ce91520ec9f55f5'
        }
    }

    result = swg_index_client.add_entry(data)
    assert result.did


def test_index_create_with_valid_did(swg_index_client):
    data = {
        'did':'3d313755-cbb4-4b08-899d-7bbac1f6e67d',
        'form': 'object',
        'size': 123,
        'urls': ['s3://endpointurl/bucket/key'],
        'hashes': {'md5': '8b9942cf415384b27cadf1f4d2d682e5'}}

    result = swg_index_client.add_entry(data)
    assert result.did == '3d313755-cbb4-4b08-899d-7bbac1f6e67d'


def test_index_create_with_invalid_did(swg_index_client):
    data = {
        'did': '3d313755-cbb4-4b0fdfdfd8-899d-7bbac1f6e67dfdd',
        'form': 'object',
        'size': 123,
        'urls': ['s3://endpointurl/bucket/key'],
        'hashes': {'md5': '8b9942cf415384b27cadf1f4d2d682e5'}}

    with pytest.raises(ApiException) as e:
        swg_index_client.add_entry(data)
        assert e.status == 400


def test_index_create_with_prefix(swg_index_client):
    data = {
        'did': 'cdis:3d313755-cbb4-4b08-899d-7bbac1f6e67d',
        'form': 'object',
        'size': 123,
        'urls': ['s3://endpointurl/bucket/key'],
        'hashes': {'md5': '8b9942cf415384b27cadf1f4d2d682e5'}}

    r = swg_index_client.add_entry(data)
    assert r.did == 'cdis:3d313755-cbb4-4b08-899d-7bbac1f6e67d'


def test_index_create_with_duplicate_did(swg_index_client):
    data = {
        'did':'3d313755-cbb4-4b08-899d-7bbac1f6e67d',
        'form': 'object',
        'size': 123,
        'urls': ['s3://endpointurl/bucket/key'],
        'hashes': {'md5': '8b9942cf415384b27cadf1f4d2d682e5'}}

    swg_index_client.add_entry(data)

    data2 = {
        'did':'3d313755-cbb4-4b08-899d-7bbac1f6e67d',
        'form': 'object',
        'size': 213,
        'urls': ['s3://endpointurl/bucket/key'],
        'hashes': {'md5': '469942cf415384b27cadf1f4d2d682e5'}}

    with pytest.raises(ApiException) as e:
        swg_index_client.add_entry(data2)
        assert e.status == 400


def test_index_create_with_file_name(swg_index_client):
    data = {
        'form': 'object',
        'size': 123,
        'urls': ['s3://endpointurl/bucket/key'],
        'file_name': 'abc',
        'hashes': {'md5': '8b9942cf415384b27cadf1f4d2d682e5'}}

    r = swg_index_client.add_entry(data)
    r = swg_index_client.get_entry(r.did)
    assert r.file_name == 'abc'


def test_index_create_with_version(swg_index_client):
    data = {
        'form': 'object',
        'size': 123,
        'urls': ['s3://endpointurl/bucket/key'],
        'file_name': 'abc',
        'version': 'ver_123',
        'hashes': {'md5': '8b9942cf415384b27cadf1f4d2d682e5'}}

    r = swg_index_client.add_entry(data)
    r = swg_index_client.get_entry(r.did)
    assert r.version == data['version']


def test_index_create_with_metadata(swg_index_client):
    data = {
        'form': 'object',
        'size': 123,
        'urls': ['s3://endpointurl/bucket/key'],
        'metadata': {
            'project_id': 'bpa-UChicago'
        },
        'hashes': {'md5': '8b9942cf415384b27cadf1f4d2d682e5'}}

    r = swg_index_client.add_entry(data)
    r = swg_index_client.get_entry(r.did)
    assert r.metadata == {
            'project_id': 'bpa-UChicago'
        }


def test_index_get_global_endpoint(swg_global_client, swg_index_client):
    data = {
        'form': 'object',
        'size': 123,
        'urls': ['s3://endpointurl/bucket/key'],
        'metadata': {
            'project_id': 'bpa-UChicago'
        },
        'hashes': {'md5': '8b9942cf415384b27cadf1f4d2d682e5'}}

    r = swg_index_client.add_entry(data)
    r = swg_global_client.get_entry(r.did)

    assert r.metadata == {
            'project_id': 'bpa-UChicago'
        }
    assert r.form == 'object'
    assert r.size == 123
    assert r.urls == ['s3://endpointurl/bucket/key']
    assert r.hashes.md5 == '8b9942cf415384b27cadf1f4d2d682e5'


def test_index_update(swg_index_client):
    data = {
        'form': 'object',
        'size': 123,
        'urls': ['s3://endpointurl/bucket/key'],
        'hashes': {'md5': '8b9942cf415384b27cadf1f4d2d682e5'},
        'metadata': {'test': 'abc'}}

    r = swg_index_client.add_entry(data)
    assert r.did
    assert r.rev
    assert swg_index_client.get_entry(r.did).metadata == data['metadata']
    dataNew = {
        'urls': ['s3://endpointurl/bucket/key'],
        'file_name': 'test',
        'version': 'ver123',
        'metadata': {'test': 'abcd'},
    }
    r2 = swg_index_client.update_entry(r.did, rev=r.rev, body=dataNew)
    assert r2.rev != r.rev
    assert swg_index_client.get_entry(r.did).metadata == dataNew['metadata']

    data = {
        'did': 'cdis:3d313755-cbb4-4b08-899d-7bbac1f6e67d',
        'form': 'object',
        'size': 123,
        'urls': ['s3://endpointurl/bucket/key'],
        'hashes': {'md5': '8b9942cf415384b27cadf1f4d2d682e5'}}

    r = swg_index_client.add_entry(data)
    assert r.did
    assert r.rev
    dataNew = {
        'urls': ['s3://endpointurl/bucket/key'],
        'file_name': 'test',
        'version': 'ver123',
        }
    r2 = swg_index_client.update_entry(r.did, rev=r.rev, body=dataNew)
    assert r2.rev != r.rev


def test_index_delete(swg_index_client):
    data = {
        'form': 'object',
        'size': 123,
        'urls': ['s3://endpointurl/bucket/key'],
        'hashes': {'md5': '8b9942cf415384b27cadf1f4d2d682e5'}}

    r = swg_index_client.add_entry(data)
    assert r.did
    assert r.rev

    r = swg_index_client.get_entry(r.did)
    assert r.did

    swg_index_client.delete_entry(r.did, rev=r.rev)

    with pytest.raises(ApiException) as e:
        r = swg_index_client.get_entry(r.did)
        assert e.status == 400


def test_create_index_version(swg_index_client):
    data = {
        'form': 'object',
        'size': 123,
        'urls': ['s3://endpointurl/bucket/key'],
        'hashes': {'md5': '8b9942cf415384b27cadf1f4d2d682e5'}}

    r = swg_index_client.add_entry(data)
    assert r.did
    assert r.rev
    assert r.baseid

    dataNew = {
        'form': 'object',
        'size': 244,
        'urls': ['s3://endpointurl/bucket2/key'],
        'hashes': {'md5': '8b9942cf415384b27cadf1f4d2d981f5'},
        }

    r2 = swg_index_client.add_new_version(r.did, body=dataNew)
    assert r2.baseid == r.baseid


def test_get_latest_version(swg_index_client):
    data = {
        'form': 'object',
        'size': 123,
        'urls': ['s3://endpointurl/bucket/key'],
        'hashes': {'md5': '8b9942cf415384b27cadf1f4d2d682e5'}}

    r = swg_index_client.add_entry(data)
    assert r.did

    dataNew = {
        'form': 'object',
        'size': 244,
        'urls': ['s3://endpointurl/bucket2/key'],
        'hashes': {'md5': '8b9942cf415384b27cadf1f4d2d981f5'},
        }

    r2 = swg_index_client.add_new_version(r.did, body=dataNew)

    r3 = swg_index_client.get_latest_version(r.did)
    assert r3.did == r2.did


def test_get_all_versions(swg_index_client):
    data = {
        'form': 'object',
        'size': 123,
        'urls': ['s3://endpointurl/bucket/key'],
        'hashes': {'md5': '8b9942cf415384b27cadf1f4d2d682e5'}}

    r = swg_index_client.add_entry(data)
    assert r.did

    dataNew = {
        'form': 'object',
        'size': 244,
        'urls': ['s3://endpointurl/bucket2/key'],
        'hashes': {'md5': '8b9942cf415384b27cadf1f4d2d981f5'},
        }

    swg_index_client.add_new_version(r.did, body=dataNew)
    r3 = swg_index_client.get_all_versions(r.did)
    assert len(r3) == 2


def test_alias_list(swg_alias_client):
    assert swg_alias_client.list_entries().aliases == []


def test_alias_create(swg_alias_client):
    data = {
        'size': 123,
        'hashes': {'md5': '8b9942cf415384b27cadf1f4d2d682e5'},
        'release': 'private',
        'keeper_authority': 'CRI', 'host_authorities': ['PDC'],
    }
    ark = 'ark:/31807/TEST-abc'
    r = swg_alias_client.upsert_entry(ark, body=data)
    assert r.name == ark

    assert len(swg_alias_client.list_entries().aliases) == 1
    assert swg_alias_client.get_entry(r.name).name


def test_alias_get_global_endpoint(swg_alias_client, swg_global_client):
    data = {
        'size': 123,
        'hashes': {'md5': '8b9942cf415384b27cadf1f4d2d682e5'},
        'release': 'private',
        'keeper_authority': 'CRI', 'host_authorities': ['PDC'],
    }
    ark = 'ark:/31807/TEST-abc'

    swg_alias_client.upsert_entry(ark, body=data)

    assert swg_global_client.get_entry(ark).size == 123


def test_alias_update(swg_alias_client):
    data = {
        'size': 123,
        'hashes': {'md5': '8b9942cf415384b27cadf1f4d2d682e5'},
        'release': 'private',
        'keeper_authority': 'CRI', 'host_authorities': ['PDC'],
    }
    ark = 'ark:/31807/TEST-abc'

    r = swg_alias_client.upsert_entry(ark, body=data)
    assert r.rev

    dataNew = {
        'size': 456,
        'hashes': {'md5': '8b9942cf415384b27cadf1f4d2d682e5'},
        'release': 'private',
        'keeper_authority': 'CRI', 'host_authorities': ['PDC'],
    }
    r2 = swg_alias_client.upsert_entry(ark, rev=r.rev, body=dataNew)
    assert r2.rev != r.rev


def test_alias_delete(swg_alias_client):
    data = {
        'size': 123,
        'hashes': {'md5': '8b9942cf415384b27cadf1f4d2d682e5'},
        'release': 'private',
        'keeper_authority': 'CRI', 'host_authorities': ['PDC'],
    }
    ark = 'ark:/31807/TEST-abc'

    r = swg_alias_client.upsert_entry(ark, body=data)
    assert r.rev

    swg_alias_client.delete_entry(ark, rev=r.rev)

    assert len(swg_alias_client.list_entries().aliases) == 0


@pytest.mark.parametrize('typ,h', [
    ('md5', '8b9942cf415384b27cadf1f4d2d682e5'),
    ('etag', '8b9942cf415384b27cadf1f4d2d682e5'),
    ('etag', '8b9942cf415384b27cadf1f4d2d682e5-2311'),
    ('sha1', '1b64db0c5ef4fa349b5e37403c745e7ef4caa350'),
    ('sha256', '4ff2d1da9e33bb0c45f7b0e5faa1a5f5' +
        'e6250856090ff808e2c02be13b6b4258'),
    ('sha512', '65de2c01a38d2d88bd182526305' +
        '56ed443b56fd51474cb7c0930d0b62b608' +
        'a3c7d9e27d53269f9a356a2af9bd4c18d5' +
        '368e66dd9f2412b82e325de3c5a4c21b3'),
    ('crc', '997a6f5c'),
])
def test_good_hashes(client, user, typ, h):
    data = {
        'form': 'object',
        'size': 123,
        'urls': ['s3://endpointurl/bucket/key'],
        'file_name': 'abc',
        'version': 'ver_123',
        'hashes': {typ: h}
    }

    resp = client.post('/index/', data=json.dumps(data), headers=user)

    assert resp.status_code == 200
    json_resp = resp.json
    assert 'error' not in json_resp


@pytest.mark.parametrize('typ,h', [
    ('', ''),
    ('blah', 'aaa'),
    ('not_supported', '8b9942cf415384b27cadf1f4d2d682e5'),
    ('md5', 'not valid'),
    ('crc', 'not valid'),
    ('etag', ''),
    ('etag', '8b9942cf415384b27cadf1f4d2d682e5-'),
    ('etag', '8b9942cf415384b27cadf1f4d2d682e5-afffafb'),
    ('sha1', '8b9942cf415384b27cadf1f4d2d682e5'),
    ('sha256', 'not valid'),
    ('sha512', 'not valid'),
])
def test_bad_hashes(client, user, typ, h):
    data = {
        'form': 'object',
        'size': 123,
        'urls': ['s3://endpointurl/bucket/key'],
        'file_name': 'abc',
        'version': 'ver_123',
        'hashes': {typ: h}
    }

    resp = client.post('/index/', data=json.dumps(data), headers=user)

    assert resp.status_code == 400
    json_resp = resp.json
    assert 'error' in json_resp
    if typ not in ACCEPTABLE_HASHES:
        assert 'is not valid' in json_resp['error']
    else:
        assert 'does not match' in json_resp['error']
