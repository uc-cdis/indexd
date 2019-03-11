import uuid
from datetime import datetime

import pytest

from indexd.errors import UserError
from indexd.index.drivers.alchemy import IndexRecord
from indexd.index.errors import NoRecordFound, RevisionMismatch
from tests.util import make_sql_statement

#TODO check if pytest has utilities for meta-programming of tests


def test_driver_init_does_not_create_records(index_driver, database_conn):
    """
    Tests for creation of records after driver init.
    Tests driver init does not have unexpected side-effects.
    """
    count = database_conn.execute("""
        SELECT COUNT(*) FROM index_record
    """).fetchone()[0]

    assert count == 0, 'driver created records upon initilization'


def test_driver_init_does_not_create_record_urls(index_driver, database_conn):
    """
    Tests for creation of urls after driver init.
    Tests driver init does not have unexpected side-effects.
    """
    count = database_conn.execute("""
        SELECT COUNT(*) FROM index_record_url
    """).fetchone()[0]

    assert count == 0, 'driver created records urls upon initilization'


def test_driver_init_does_not_create_record_hashes(index_driver, database_conn):
    """
    Tests for creation of hashes after driver init.
    Tests driver init does not have unexpected side-effects.
    """

    count = database_conn.execute("""
        SELECT COUNT(*) FROM index_record_hash
    """).fetchone()[0]

    assert count == 0, 'driver created records hashes upon initilization'


def test_driver_add_object_record(index_driver, database_conn):
    """
    Tests creation of a record.
    """

    index_driver.add('object')

    count = database_conn.execute("""
        SELECT COUNT(*) FROM index_record
    """).fetchone()[0]

    assert count == 1, 'driver did not create record'

    record = database_conn.execute("""
        SELECT * FROM index_record
    """).fetchone()

    assert record[0], 'record id not populated'
    assert record[1], 'record baseid not populated'
    assert record[2], 'record rev not populated'
    assert record[3] == 'object', 'record form is not object'
    assert record[4] is None, 'record size non-null'


def test_driver_add_container_record(index_driver, database_conn):
    """
    Tests creation of a record.
    """

    index_driver.add('container')

    count = database_conn.execute("""
        SELECT COUNT(*) FROM index_record
    """).fetchone()[0]

    assert count == 1, 'driver did not create record'

    record = database_conn.execute("""
        SELECT * FROM index_record
    """).fetchone()

    assert record[0], 'record id not populated'
    assert record[1], 'record baseid not populated'
    assert record[2], 'record rev not populated'
    assert record[3] == 'container', 'record form is not container'
    assert record[4] is None, 'record size non-null'


def test_driver_add_multipart_record(index_driver, database_conn):
    """
    Tests creation of a record.
    """

    index_driver.add('multipart')

    count = database_conn.execute("""
        SELECT COUNT(*) FROM index_record
    """).fetchone()[0]

    assert count == 1, 'driver did not create record'

    record = database_conn.execute("""
        SELECT * FROM index_record
    """).fetchone()

    assert record[0], 'record id not populated'
    assert record[1], 'record baseid not populated'
    assert record[2], 'record rev not populated'
    assert record[3] == 'multipart', 'record form is not multipart'
    assert record[4] == None, 'record size non-null'


def test_driver_add_with_valid_did(index_driver):
    """
    Tests creation of a record with given valid did.
    """

    form = 'object'
    did = '3d313755-cbb4-4b08-899d-7bbac1f6e67d'
    index_driver.add(form, did=did)
    with index_driver.session as session:
        assert session.query(IndexRecord).first().did == did


def test_driver_add_with_duplicate_did(index_driver):
    """
    Tests creation of a record with duplicate did.
    """
    form = 'object'
    did = '3d313755-cbb4-4b08-899d-7bbac1f6e67d'
    index_driver.add(form, did=did)

    with pytest.raises(UserError):
        index_driver.add(form, did=did)


def test_driver_add_multiple_records(index_driver, database_conn):
    """
    Tests creation of a record.
    """

    index_driver.add('object')
    index_driver.add('object')
    index_driver.add('object')

    count = database_conn.execute("""
        SELECT COUNT(*) FROM index_record
    """).fetchone()[0]

    assert count == 3, 'driver did not create record(s)'

    records = database_conn.execute("""
        SELECT * FROM index_record
    """)

    for record in records:
        assert record[0], 'record id not populated'
        assert record[1], 'record baseid not populated'
        assert record[2], 'record rev not populated'
        assert record[3] == 'object', 'record form is not object'
        assert record[4] == None, 'record size non-null'


def test_driver_add_with_size(index_driver, database_conn):
    """
    Tests creation of a record with size.
    """

    form = 'object'
    size = 512

    index_driver.add(form, size=size)

    count = database_conn.execute("""
        SELECT COUNT(*) FROM index_record
    """).fetchone()[0]

    assert count == 1, 'driver did not create record'

    new_form, new_size = database_conn.execute("""
        SELECT form, size FROM index_record
    """).fetchone()

    assert form == new_form, 'record form mismatch'
    assert size == new_size, 'record size mismatch'


def test_driver_add_with_urls(index_driver, database_conn):
    """
    Tests creation of a record with urls.
    """
    form = 'object'
    urls_metadata = {'a': {'type': 'ok'}, 'b': {'type': 'ok'}, 'c': {'type': 'ok'}}
    # urls = ['a', 'b', 'c']

    index_driver.add(form, urls_metadata=urls_metadata)

    count = database_conn.execute("""
        SELECT COUNT(*) FROM index_record
    """).fetchone()[0]

    assert count == 1, 'driver did not create record'

    count = database_conn.execute("""
        SELECT COUNT(url) FROM index_record_url_metadata_jsonb
    """).fetchone()[0]

    assert count == 3, 'driver did not create url(s)'

    new_urls = sorted(url[0] for url in database_conn.execute("""
        SELECT url FROM index_record_url_metadata_jsonb
    """))

    assert sorted(urls_metadata.keys()) == new_urls, 'record urls mismatch'


def test_driver_add_with_filename(index_driver):
    """
    Tests creation of a record with filename.
    """

    form = 'object'
    file_name = 'abc'
    index_driver.add(form, file_name=file_name)
    with index_driver.session as s:
        assert s.query(IndexRecord).first().file_name == 'abc'


def test_driver_add_with_version(index_driver):
    """
    Tests creation of a record with version string.
    """
    form = 'object'
    version = 'ver_123'
    index_driver.add(form, version=version)
    with index_driver.session as s:
        assert s.query(IndexRecord).first().version == 'ver_123'


def test_driver_add_with_hashes(index_driver, database_conn):
    """
    Tests creation of a record with hashes.
    """

    form = 'object'
    hashes = {
        'a': '1',
        'b': '2',
        'c': '3',
    }

    index_driver.add(form, hashes=hashes)

    count = database_conn.execute("""
        SELECT COUNT(*) FROM index_record
    """).fetchone()[0]

    assert count == 1, 'driver did not create record'

    count = database_conn.execute("""
        SELECT COUNT(*) FROM index_record_hash
    """).fetchone()[0]

    assert count == 3, 'driver did not create hash(es)'

    new_hashes = {h:v for h, v in database_conn.execute("""
        SELECT hash_type, hash_value FROM index_record_hash
    """)}

    assert hashes == new_hashes, 'record hashes mismatch'


def test_driver_get_record(index_driver, database_conn):
    """
    Tests retrieval of a record.
    """
    did = str(uuid.uuid4())
    baseid = str(uuid.uuid4())
    rev = str(uuid.uuid4())[:8]
    size = 512
    form = 'object'
    baseid = str(uuid.uuid4())
    created_date = datetime.now()
    updated_date = datetime.now()

    database_conn.execute(make_sql_statement("""
        INSERT INTO index_record (did, baseid, rev, form, size, created_date, updated_date)
        VALUES (?,?,?,?,?,?,?)
    """, (did, baseid, rev, form, size, created_date, updated_date)))

    record = index_driver.get(did)

    assert record['did'] == did, 'record id does not match'
    assert record['baseid'] == baseid, 'record id does not match'
    assert record['rev'] == rev, 'record revision does not match'
    assert record['size'] == size, 'record size does not match'
    assert record['form'] == form, 'record form does not match'
    assert record['created_date'] == created_date.isoformat(), 'created date does not match'
    assert record['updated_date'] == updated_date.isoformat(), 'updated date does not match'


def test_driver_get_fails_with_no_records(index_driver):
    """
    Tests retrieval of a record fails if there are no records.
    """
    with pytest.raises(NoRecordFound):
        index_driver.get('some_record_that_does_not_exist')


def test_driver_get_latest_version(index_driver, database_conn):
    """
    Tests retrieval of the latest record version
    """
    baseid = str(uuid.uuid4())

    for _ in range(10):

        did = str(uuid.uuid4())
        rev = str(uuid.uuid4())[:8]
        size = 512
        form = 'object'
        baseid = str(uuid.uuid4())
        created_date = datetime.now()
        updated_date = datetime.now()

        database_conn.execute(make_sql_statement("""
            INSERT INTO index_record(did, baseid, rev, form, size, created_date, updated_date) VALUES (?,?,?,?,?,?,?)
        """, (did, baseid, rev, form, size, created_date, updated_date)))

    record = index_driver.get_latest_version(did)

    assert record['did'] == did, 'record id does not match'
    assert record['rev'] == rev, 'record revision does not match'
    assert record['size'] == size, 'record size does not match'
    assert record['form'] == form, 'record form does not match'
    assert record['created_date'] == created_date.isoformat(), 'created date does not match'
    assert record['updated_date'] == updated_date.isoformat(), 'updated date does not match'


def test_driver_get_latest_version_with_no_record(index_driver, database_conn):
    """
    Tests retrieval of the latest record version
    """
    for _ in range(10):

        did = str(uuid.uuid4())
        rev = str(uuid.uuid4())[:8]
        size = 512
        form = 'object'
        baseid = str(uuid.uuid4())
        dt = datetime.now()

        database_conn.execute(make_sql_statement("""
            INSERT INTO index_record(did, baseid, rev, form, size, created_date, updated_date) VALUES (?,?,?,?,?,?,?)
        """, (did, baseid, rev, form, size, dt, dt)))

    with pytest.raises(NoRecordFound):
        index_driver.get_latest_version('some base version')


def test_driver_get_all_version(index_driver, database_conn):
    """
    Tests retrieval of the latest record version
    """
    baseid = str(uuid.uuid4())

    NUMBER_OF_RECORD = 3

    given_records = {}

    for _ in range(NUMBER_OF_RECORD):

        did = str(uuid.uuid4())
        rev = str(uuid.uuid4())[:8]
        size = 512
        form = 'object'
        created_date = datetime.now()
        updated_date = created_date

        given_records[did] = {
            'rev': rev,
            'size': size,
            'form': form,
            'created_date': created_date,
            'updated_date': updated_date,
        }

        database_conn.execute(make_sql_statement(
            """
                INSERT INTO index_record(did, baseid, rev, form, size, created_date, updated_date)
                VALUES (?,?,?,?,?,?,?)""",
            (did, baseid, rev, form, size, created_date, updated_date)))

    records = index_driver.get_all_versions(did)
    assert len(records) == NUMBER_OF_RECORD, 'the number of records does not match'

    for i in range(NUMBER_OF_RECORD):
        record = records[i]
        given_record = given_records[record['did']]
        assert record['rev'] == given_record['rev'], 'record revision does not match'
        assert record['size'] == given_record['size'], 'record size does not match'
        assert record['form'] == given_record['form'], 'record form does not match'
        assert record['created_date'] == given_record['created_date'].isoformat(), 'created date does not match'
        assert record['updated_date'] == given_record['updated_date'].isoformat(), 'updated date does not match'


def test_driver_get_all_version_with_no_record(index_driver, database_conn):
    """
    Tests retrieval of the latest record version
    """
    baseid = str(uuid.uuid4())

    for _ in range(3):

        did = str(uuid.uuid4())
        rev = str(uuid.uuid4())[:8]
        size = 512
        form = 'object'

        database_conn.execute(make_sql_statement("""
            INSERT INTO index_record(did, baseid, rev, form, size) VALUES (?,?,?,?,?)
        """, (did, baseid, rev, form, size)))

    with pytest.raises(NoRecordFound):
        index_driver.get_all_versions('some baseid')


def test_driver_get_fails_with_invalid_id(index_driver, database_conn):
    """
    Tests retrieval of a record fails if the record id is not found.
    """

    did = str(uuid.uuid4())
    baseid = str(uuid.uuid4())
    rev = str(uuid.uuid4())[:8]
    form = 'object'

    database_conn.execute(make_sql_statement("""
        INSERT INTO index_record(did, baseid, rev, form, size) VALUES (?,?,?,?,?)
    """, (did, baseid, rev, form, None)))

    with pytest.raises(NoRecordFound):
        index_driver.get('some_record_that_does_not_exist')


def test_driver_update_record(index_driver, database_conn):
    """
    Tests updating of a record.
    """

    did = str(uuid.uuid4())
    baseid = str(uuid.uuid4())
    rev = str(uuid.uuid4())[:8]
    form = 'object'

    database_conn.execute(make_sql_statement("""
        INSERT INTO index_record(did, baseid, rev, form, size) VALUES (?,?,?,?,?)
    """, (did, baseid, rev, form, 1)))

    update_size = 256
    update_urls_metadata = {
        'a': {
            'type': 'ok',
            'not type': 'ok',
        }, 'b': {
            'state': 'not ok',
        }, 'c': {
            'not type': 'not ok',
        },
    }
    update_hashes = {
        'a': '1',
        'b': '2',
        'c': '3',
    }

    file_name = 'test'
    version = 'ver_123'

    changing_fields = {
        'urls_metadata': update_urls_metadata,
        'file_name': file_name,
        'version': version,
        'size': update_size,
        'hashes': update_hashes,
    }

    index_driver.update(did, rev, changing_fields)

    new_did, new_rev, new_file_name, new_size, new_version = database_conn.execute("""
        SELECT did, rev, file_name, size, version FROM index_record
    """).fetchone()

    query = database_conn.execute("""
        SELECT url, state, type, urls_metadata
        FROM index_record_url_metadata_jsonb
    """)
    new_urls_metadata = {}
    for row in query:
        new_urls_metadata[row.url] = row.urls_metadata
        # Don't add the key if there's no value.
        if row.type:
            new_urls_metadata[row.url]['type'] = row.type
        if row.state:
            new_urls_metadata[row.url]['state'] = row.state

    new_hashes = {h: v for h, v in database_conn.execute("""
        SELECT hash_type, hash_value FROM index_record_hash
    """)}

    assert did == new_did, 'record id does not match'
    assert rev != new_rev, 'record revision matches prior'
    assert update_urls_metadata == new_urls_metadata, 'record urls_metadata mismatch'
    assert file_name == new_file_name, 'file_name does not match'
    assert update_size == new_size, 'size does not match'
    assert version == new_version, 'version does not match'
    assert update_hashes == new_hashes, 'hashes do not match'


def test_driver_update_fails_with_no_records(index_driver):
    """
    Tests updating a record fails if there are no records.
    """
    with pytest.raises(NoRecordFound):
        index_driver.update('some_record_that_does_not_exist', 'some_base_version', 'some_revision')


def test_driver_update_fails_with_invalid_id(index_driver, database_conn):
    """
    Tests updating a record fails if the record id is not found.
    """
    did = str(uuid.uuid4())
    baseid = str(uuid.uuid4())
    rev = str(uuid.uuid4())[:8]
    form = 'object'

    database_conn.execute(make_sql_statement("""
        INSERT INTO index_record(did, baseid, rev, form, size) VALUES (?,?,?,?,?)
    """, (did, baseid, rev, form, None)))

    with pytest.raises(NoRecordFound):
        index_driver.update('some_record_that_does_not_exist','some_record_version', rev)


def test_driver_update_fails_with_invalid_rev(index_driver, database_conn):
    """
    Tests updating a record fails if the record rev is not invalid.
    """

    did = str(uuid.uuid4())
    baseid = str(uuid.uuid4())
    rev = str(uuid.uuid4())[:8]
    form = 'object'

    database_conn.execute(make_sql_statement("""
        INSERT INTO index_record(did, baseid, rev, form, size) VALUES (?,?,?,?,?)
    """, (did, baseid, rev, form, None)))

    with pytest.raises(RevisionMismatch):
        index_driver.update(did, baseid, 'some_revision')


def test_driver_delete_record(index_driver, database_conn):
    """
    Tests deletion of a record.
    """
    did = str(uuid.uuid4())
    baseid = str(uuid.uuid4())
    rev = str(uuid.uuid4())[:8]
    form = 'object'

    database_conn.execute(make_sql_statement("""
        INSERT INTO index_record(did, baseid, rev, form, size) VALUES (?,?,?,?,?)
    """, (did, baseid, rev, form, None)))

    index_driver.delete(did, rev)

    count = database_conn.execute("""
        SELECT COUNT(*) FROM index_record
    """).fetchone()[0]

    assert count == 0, 'records remain after deletion'


def test_driver_delete_fails_with_no_records(index_driver):
    """
    Tests deletion of a record fails if there are no records.
    """
    with pytest.raises(NoRecordFound):
        index_driver.delete('some_record_that_does_not_exist', 'some_revision')


def test_driver_delete_fails_with_invalid_id(index_driver, database_conn):
    """
    Tests deletion of a record fails if the record id is not found.
    """

    did = str(uuid.uuid4())
    baseid = str(uuid.uuid4())
    rev = str(uuid.uuid4())[:8]
    form = 'object'

    database_conn.execute(make_sql_statement("""
        INSERT INTO index_record(did, baseid, rev, form, size) VALUES (?,?,?,?,?)
    """, (did, baseid, rev, form, None)))

    with pytest.raises(NoRecordFound):
        index_driver.delete('some_record_that_does_not_exist', rev)


def test_driver_delete_fails_with_invalid_rev(index_driver, database_conn):
    """
    Tests deletion of a record fails if the record rev is not invalid.
    """
    did = str(uuid.uuid4())
    baseid = str(uuid.uuid4())
    rev = str(uuid.uuid4())[:8]
    form = 'object'

    database_conn.execute(make_sql_statement("""
        INSERT INTO index_record(did, baseid, rev, form, size) VALUES (?,?,?,?,?)
    """, (did, baseid, rev, form, None)))

    with pytest.raises(RevisionMismatch):
        index_driver.delete(did, 'some_revision')
