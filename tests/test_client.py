import json

import pytest
from swagger_client.rest import ApiException

from indexd.index.blueprint import ACCEPTABLE_HASHES

def get_doc(
        has_metadata=True, has_baseid=False,
        has_urls_metadata=False, has_version=False):
    doc = {
        'form': 'object',
        'size': 123,
        'urls': ['s3://endpointurl/bucket/key'],
        'hashes': {'md5': '8b9942cf415384b27cadf1f4d2d682e5'}
    }
    if has_metadata:
        doc['metadata'] = {'project_id': 'bpa-UChicago'}
    if has_baseid:
        doc['baseid'] = 'e044a62c-fd60-4203-b1e5-a62d1005f027'
    if has_urls_metadata:
        doc['urls_metadata'] = {
            's3://endpointurl/bucket/key': {'state': 'uploaded'}}
    if has_version:
        doc['version'] = '1'
    return doc


def test_index_list(swg_index_client):
    r = swg_index_client.list_entries()
    assert r.records == []


def test_index_list_with_params(swg_index_client):
    data = get_doc()
    r_1 = swg_index_client.add_entry(data)

    data['metadata'] = {'project_id': 'other-project'}
    r_2 = swg_index_client.add_entry(data)

    r = swg_index_client.list_entries(metadata='project_id:bpa-UChicago')
    ids = [record.did for record in r.records]
    assert r_1.did in ids

    r = swg_index_client.list_entries(metadata='project_id:other-project')
    ids = [record.did for record in r.records]
    assert r_2.did in ids

    r = swg_index_client.list_entries(
        hash='md5:8b9942cf415384b27cadf1f4d2d682e5')
    ids = [record.did for record in r.records]
    assert r_1.did in ids
    assert r_2.did in ids

    r = swg_index_client.list_entries(
        ids=','.join(ids))

    ids = [record.did for record in r.records]
    assert r_1.did in ids
    assert r_2.did in ids


def test_urls_metadata(swg_index_client):
    data = get_doc(has_urls_metadata=True)
    result = swg_index_client.add_entry(data)

    doc = swg_index_client.get_entry(result.did)
    assert doc.urls_metadata == data['urls_metadata']

    updated = {'urls_metadata': {data['urls'][0]: {'test': 'b'}}}
    swg_index_client.update_entry(doc.did, rev=doc.rev, body=updated)

    doc = swg_index_client.get_entry(result.did)
    assert doc.urls_metadata == updated['urls_metadata']


def test_urls_metadata_partial_match(swg_index_client):
    data = get_doc(has_urls_metadata=True)
    r1 = swg_index_client.add_entry(data)

    data['urls'] = ['s3://endpointurl/bucket_2/key_2']
    data['urls_metadata'] = {'s3://endpointurl/bucket_2/key_2': {'state': 'uploaded'}}
    r2 = swg_index_client.add_entry(data)

    docs = swg_index_client.list_entries(
        urls_metadata=json.dumps({"s3://do_not_exist": {"test": "test"}})
    )
    assert len(docs.records) == 0

    with pytest.raises(ApiException) as e:
        swg_index_client.list_entries(
            urls_metadata="invalid json."
        )
        assert e.status == 400

    docs = swg_index_client.list_entries(
        urls_metadata=json.dumps({'s3://endpointurl/': {'state': 'uploaded'}})
    )
    assert len(docs.records) == 2

    ids = {record.did for record in docs.records}
    assert ids == {r1.did, r2.did}


def test_get_urls(swg_index_client, swg_global_client):
    data = get_doc(has_urls_metadata=True)
    result = swg_index_client.add_entry(data)

    result = swg_global_client.list_urls(ids=result.did)
    url = data['urls'][0]
    assert result.urls[0].url == url
    assert result.urls[0].metadata == data['urls_metadata'][url]


def test_index_create(swg_index_client):
    data = get_doc(has_baseid=True)

    result = swg_index_client.add_entry(data)
    assert result.did
    assert result.baseid == data['baseid']
    r = swg_index_client.get_entry(result.did)
    assert r.acl == []


def test_index_get(swg_index_client):
    data = get_doc(has_baseid=True)

    result = swg_index_client.add_entry(data)
    r = swg_index_client.get_entry(result.did)
    r2 = swg_index_client.get_entry(result.baseid)
    assert r.did == result.did
    assert r2.did == result.did

def test_index_get_with_baseid(swg_index_client):
    data1 = get_doc(has_baseid=True)
    swg_index_client.add_entry(data1)

    data2 = get_doc(has_baseid=True)
    r2 = swg_index_client.add_entry(data2)

    r = swg_index_client.get_entry(data1['baseid'])
    assert r.did == r2.did


def test_delete_and_recreate(swg_index_client):
    """
    Test that you can delete an IndexDocument and be able to
    recreate it with the same fields.
    """

    old_data = get_doc(has_baseid=True)
    new_data = get_doc(has_baseid=True)
    new_data['hashes'] = {'md5': '11111111111111111111111111111111'}

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
    data = get_doc()
    data['hashes'] = {
        'md5': '8b9942cf415384b27cadf1f4d2d682e5',
        'sha1': 'fdbbca63fbec1c2b0d4eb2494ce91520ec9f55f5'
    }

    result = swg_index_client.add_entry(data)
    assert result.did


def test_index_create_with_valid_did(swg_index_client):
    data = get_doc()
    data['did'] = '3d313755-cbb4-4b08-899d-7bbac1f6e67d'

    result = swg_index_client.add_entry(data)
    assert result.did == '3d313755-cbb4-4b08-899d-7bbac1f6e67d'


def test_index_create_with_acl(swg_index_client):
    data = {
        'acl': ['a', 'b'],
        'form': 'object',
        'size': 123,
        'urls': ['s3://endpointurl/bucket/key'],
        'hashes': {'md5': '8b9942cf415384b27cadf1f4d2d682e5'}}

    r = swg_index_client.add_entry(data)
    result = swg_index_client.get_entry(r.did)
    assert result.acl == ['a', 'b']


def test_index_create_with_invalid_did(swg_index_client):
    data = get_doc()

    data['did'] = '3d313755-cbb4-4b0fdfdfd8-899d-7bbac1f6e67dfdd'

    with pytest.raises(ApiException) as e:
        swg_index_client.add_entry(data)
        assert e.status == 400


def test_index_create_with_prefix(swg_index_client):
    data = get_doc()
    data['did'] = 'cdis:3d313755-cbb4-4b08-899d-7bbac1f6e67d'

    r = swg_index_client.add_entry(data)
    assert r.did == 'cdis:3d313755-cbb4-4b08-899d-7bbac1f6e67d'


def test_index_create_with_duplicate_did(swg_index_client):
    data = get_doc()
    data['did'] = '3d313755-cbb4-4b08-899d-7bbac1f6e67d'

    swg_index_client.add_entry(data)

    with pytest.raises(ApiException) as e:
        swg_index_client.add_entry(data)
        assert e.status == 400


def test_index_create_with_file_name(swg_index_client):
    data = get_doc()
    data['file_name'] = 'abc'

    r = swg_index_client.add_entry(data)
    r = swg_index_client.get_entry(r.did)
    assert r.file_name == 'abc'


def test_index_create_with_version(swg_index_client):
    data = get_doc()
    data['version'] = 'ver_123'

    r = swg_index_client.add_entry(data)
    r = swg_index_client.get_entry(r.did)
    assert r.version == data['version']


def test_index_get_global_endpoint(swg_global_client, swg_index_client):
    data = get_doc()

    r = swg_index_client.add_entry(data)
    r = swg_global_client.get_entry(r.did)

    assert r.metadata == data['metadata']
    assert r.form == 'object'
    assert r.size == data['size']
    assert r.urls == data['urls']
    assert r.hashes.md5 == data['hashes']['md5']

    r2 = swg_global_client.get_entry('testprefix:'+r.did)
    assert r2.did == r.did


def test_index_update(swg_index_client):
    data = get_doc()

    r = swg_index_client.add_entry(data)
    assert r.did
    assert r.rev
    assert swg_index_client.get_entry(r.did).metadata == data['metadata']
    dataNew = get_doc()
    del dataNew['hashes']
    del dataNew['size']
    del dataNew['form']
    dataNew['metadata'] = {'test': 'abcd'}
    dataNew['version'] = 'ver123'
    dataNew['acl'] = ['a', 'b']
    r2 = swg_index_client.update_entry(r.did, rev=r.rev, body=dataNew)
    assert r2.rev != r.rev
    result = swg_index_client.get_entry(r.did)
    assert result.metadata == dataNew['metadata']
    assert result.acl == dataNew['acl']

    data = get_doc()
    data['did'] = 'cdis:3d313755-cbb4-4b08-899d-7bbac1f6e67d'
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
    data = get_doc(has_metadata=False, has_baseid=False)

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
    data = get_doc(has_metadata=False, has_baseid=False)

    r = swg_index_client.add_entry(data)
    assert r.did
    assert r.rev
    assert r.baseid

    dataNew = {
        'did': 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa',
        'form': 'object',
        'size': 244,
        'urls': ['s3://endpointurl/bucket2/key'],
        'hashes': {'md5': '8b9942cf415384b27cadf1f4d2d981f5'},
        'acl': ['a'],
    }

    r2 = swg_index_client.add_new_version(r.did, body=dataNew)
    assert r2.baseid == r.baseid
    assert r2.did == dataNew['did']


def test_get_latest_version(swg_index_client):
    data = get_doc(has_metadata=False, has_baseid=False, has_version=True)
    r = swg_index_client.add_entry(data)
    assert r.did

    data = get_doc(has_metadata=False, has_baseid=False, has_version=False)
    r2 = swg_index_client.add_new_version(r.did, body=data)
    r3 = swg_index_client.get_latest_version(r.did)
    assert r3.did == r2.did

    r4 = swg_index_client.get_latest_version(r.baseid)
    assert r4.did == r2.did

    r5 = swg_index_client.get_latest_version(r.baseid, has_version=True)
    assert r5.did == r.did


def test_get_all_versions(swg_index_client):
    data = get_doc(has_metadata=False, has_baseid=False)
    r = swg_index_client.add_entry(data)
    assert r.did
    swg_index_client.add_new_version(r.did, body=data)
    r3 = swg_index_client.get_all_versions(r.did)
    assert len(r3) == 2
    r4 = swg_index_client.get_all_versions(r.baseid)
    assert len(r4) == 2


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
