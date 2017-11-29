import uuid
import sqlite3

import pytest

import util
import indexd

from indexd.index.errors import NoRecordFound
from indexd.index.errors import MultipleRecordsFound
from indexd.index.errors import RevisionMismatch

from indexd.index.drivers.alchemy import SQLAlchemyIndexDriver

from datetime import datetime


# TODO check if pytest has utilities for meta-programming of tests

@util.removes('index.sq3')
def test_driver_init_does_not_create_records():
    '''
    Tests for creation of records after driver init.
    Tests driver init does not have unexpected side-effects.
    '''
    with sqlite3.connect('index.sq3') as conn:

        driver = SQLAlchemyIndexDriver('sqlite:///index.sq3')

        count = conn.execute('''
            SELECT COUNT(*) FROM index_record
        ''').fetchone()[0]

        assert count == 0, 'driver created records upon initilization'

@util.removes('index.sq3')
def test_driver_init_does_not_create_record_urls():
    '''
    Tests for creation of urls after driver init.
    Tests driver init does not have unexpected side-effects.
    '''
    with sqlite3.connect('index.sq3') as conn:

        driver = SQLAlchemyIndexDriver('sqlite:///index.sq3')

        count = conn.execute('''
            SELECT COUNT(*) FROM index_record_url
        ''').fetchone()[0]

        assert count == 0, 'driver created records urls upon initilization'

@util.removes('index.sq3')
def test_driver_init_does_not_create_record_hashes():
    '''
    Tests for creation of hashes after driver init.
    Tests driver init does not have unexpected side-effects.
    '''
    with sqlite3.connect('index.sq3') as conn:

        driver = SQLAlchemyIndexDriver('sqlite:///index.sq3')

        count = conn.execute('''
            SELECT COUNT(*) FROM index_record_hash
        ''').fetchone()[0]

        assert count == 0, 'driver created records hashes upon initilization'

@util.removes('index.sq3')
def test_driver_add_object_record():
    '''
    Tests creation of a record.
    '''
    with sqlite3.connect('index.sq3') as conn:

        driver = SQLAlchemyIndexDriver('sqlite:///index.sq3')

        driver.add('object')

        count = conn.execute('''
            SELECT COUNT(*) FROM index_record
        ''').fetchone()[0]

        assert count == 1, 'driver did not create record'

        record = conn.execute('''
            SELECT * FROM index_record
        ''').fetchone()

        assert record[0], 'record id not populated'
        assert record[1], 'record baseid not populated'
        assert record[2], 'record rev not populated'
        assert record[3] == 'object', 'record form is not object'
        assert record[4] == None, 'record size non-null'

@util.removes('index.sq3')
def test_driver_add_container_record():
    '''
    Tests creation of a record.
    '''
    with sqlite3.connect('index.sq3') as conn:

        driver = SQLAlchemyIndexDriver('sqlite:///index.sq3')

        driver.add('container')

        count = conn.execute('''
            SELECT COUNT(*) FROM index_record
        ''').fetchone()[0]

        assert count == 1, 'driver did not create record'

        record = conn.execute('''
            SELECT * FROM index_record
        ''').fetchone()

        assert record[0], 'record id not populated'
        assert record[1], 'record baseid not populated'
        assert record[2], 'record rev not populated'
        assert record[3] == 'container', 'record form is not container'
        assert record[4] == None, 'record size non-null'

@util.removes('index.sq3')
def test_driver_add_multipart_record():
    '''
    Tests creation of a record.
    '''
    with sqlite3.connect('index.sq3') as conn:

        driver = SQLAlchemyIndexDriver('sqlite:///index.sq3')

        driver.add('multipart')

        count = conn.execute('''
            SELECT COUNT(*) FROM index_record
        ''').fetchone()[0]

        assert count == 1, 'driver did not create record'

        record = conn.execute('''
            SELECT * FROM index_record
        ''').fetchone()

        assert record[0], 'record id not populated'
        assert record[1], 'record baseid not populated'
        assert record[2], 'record rev not populated'
        assert record[3] == 'multipart', 'record form is not multipart'
        assert record[4] == None, 'record size non-null'

@util.removes('index.sq3')
def test_driver_add_multiple_records():
    '''
    Tests creation of a record.
    '''
    with sqlite3.connect('index.sq3') as conn:

        driver = SQLAlchemyIndexDriver('sqlite:///index.sq3')

        driver.add('object')
        driver.add('object')
        driver.add('object')

        count = conn.execute('''
            SELECT COUNT(*) FROM index_record
        ''').fetchone()[0]

        assert count == 3, 'driver did not create record(s)'

        records = conn.execute('''
            SELECT * FROM index_record
        ''')

        for record in records:
            assert record[0], 'record id not populated'
            assert record[1], 'record baseid not populated'
            assert record[2], 'record rev not populated'
            assert record[3] == 'object', 'record form is not object'
            assert record[4] == None, 'record size non-null'

@util.removes('index.sq3')
def test_driver_add_with_size():
    '''
    Tests creation of a record with size.
    '''
    with sqlite3.connect('index.sq3') as conn:

        driver = SQLAlchemyIndexDriver('sqlite:///index.sq3')

        form = 'object'
        size = 512

        driver.add(form, size=size)

        count = conn.execute('''
            SELECT COUNT(*) FROM index_record
        ''').fetchone()[0]

        assert count == 1, 'driver did not create record'

        new_form, new_size = conn.execute('''
            SELECT form, size FROM index_record
        ''').fetchone()

        assert form == new_form, 'record form mismatch'
        assert size == new_size, 'record size mismatch'

@util.removes('index.sq3')
def test_driver_add_with_urls():
    '''
    Tests creation of a record with urls.
    '''
    with sqlite3.connect('index.sq3') as conn:

        driver = SQLAlchemyIndexDriver('sqlite:///index.sq3')

        form = 'object'
        urls = ['a', 'b', 'c']

        driver.add(form, urls=urls)

        count = conn.execute('''
            SELECT COUNT(*) FROM index_record
        ''').fetchone()[0]

        assert count == 1, 'driver did not create record'

        count = conn.execute('''
            SELECT COUNT(*) FROM index_record_url
        ''').fetchone()[0]

        assert count == 3, 'driver did not create url(s)'

        new_urls = sorted(url[0] for url in conn.execute('''
            SELECT url FROM index_record_url
        '''))

        assert urls == new_urls, 'record urls mismatch'

@util.removes('index.sq3')
def test_driver_add_with_hashes():
    '''
    Tests creation of a record with hashes.
    '''
    with sqlite3.connect('index.sq3') as conn:

        driver = SQLAlchemyIndexDriver('sqlite:///index.sq3')

        form = 'object'
        hashes = {
            'a': '1',
            'b': '2',
            'c': '3',
        }

        driver.add(form, hashes=hashes)

        count = conn.execute('''
            SELECT COUNT(*) FROM index_record
        ''').fetchone()[0]

        assert count == 1, 'driver did not create record'

        count = conn.execute('''
            SELECT COUNT(*) FROM index_record_hash
        ''').fetchone()[0]

        assert count == 3, 'driver did not create hash(es)'

        new_hashes = {h:v for h, v in conn.execute('''
            SELECT hash_type, hash_value FROM index_record_hash
        ''')}

        assert hashes == new_hashes, 'record hashes mismatch'

@util.removes('index.sq3')
def test_driver_get_record():
    '''
    Tests retrieval of a record.
    '''
    with sqlite3.connect('index.sq3') as conn:

        driver = SQLAlchemyIndexDriver('sqlite:///index.sq3')

        did = str(uuid.uuid4())
        baseid = str(uuid.uuid4())
        rev = str(uuid.uuid4())[:8]
        size = 512
        form = 'object'
        baseid = str(uuid.uuid4())
        dt = datetime.now()

        conn.execute('''
            INSERT INTO index_record(did, baseid, rev, form, size, last_updated) VALUES (?,?,?,?,?,?)
        ''', (did, baseid, rev, form, size,dt))

        conn.commit()

        record = driver.get(did)

        assert record['did'] == did, 'record id does not match'
        assert record['baseid'] == baseid, 'record id does not match'
        assert record['rev'] == rev, 'record revision does not match'
        assert record['size'] == size, 'record size does not match'
        assert record['form'] == form, 'record form does not match'
        assert record['last_updated'] == dt, 'record last_updated does not match'

@util.removes('index.sq3')
def test_driver_get_fails_with_no_records():
    '''
    Tests retrieval of a record fails if there are no records.
    '''
    driver = SQLAlchemyIndexDriver('sqlite:///index.sq3')

    with pytest.raises(NoRecordFound):
        driver.get('some_record_that_does_not_exist')

@util.removes('index.sq3')
def test_driver_get_latest_version():
    '''
    Tests retrieval of the lattest record version
    '''
    with sqlite3.connect('index.sq3') as conn:
        driver = SQLAlchemyIndexDriver('sqlite:///index.sq3')
        baseid = str(uuid.uuid4())

        for _ in range(1,10):

            did = str(uuid.uuid4())
            rev = str(uuid.uuid4())[:8]
            size = 512
            form = 'object'
            baseid = str(uuid.uuid4())
            dt = datetime.now()

            conn.execute('''
                INSERT INTO index_record(did, baseid, rev, form, size, last_updated) VALUES (?,?,?,?,?,?)
            ''', (did, baseid, rev, form, size, dt))

            conn.commit()

        record = driver.get_latest_version(baseid)

        assert record['did'] == did, 'record id does not match'
        assert record['rev'] == rev, 'record revision does not match'
        assert record['size'] == size, 'record size does not match'
        assert record['form'] == form, 'record form does not match'
        assert record['last_updated'] == dt, 'record form does not match'

@util.removes('index.sq3')
def test_driver_get_latest_version_with_no_record():
    '''
    Tests retrieval of the lattest record version
    '''
    with sqlite3.connect('index.sq3') as conn:
        driver = SQLAlchemyIndexDriver('sqlite:///index.sq3')

        for _ in range(1,10):

            did = str(uuid.uuid4())
            rev = str(uuid.uuid4())[:8]
            size = 512
            form = 'object'
            baseid = str(uuid.uuid4())
            dt = datetime.now()

            conn.execute('''
                INSERT INTO index_record(did, baseid, rev, form, size, last_updated) VALUES (?,?,?,?,?,?)
            ''', (did, baseid, rev, form, size, dt))

            conn.commit()

        with pytest.raises(NoRecordFound):
            driver.get_latest_version('some base version')


@util.removes('index.sq3')
def test_driver_get_all_version():
    '''
    Tests retrieval of the lattest record version
    '''
    with sqlite3.connect('index.sq3') as conn:
        driver = SQLAlchemyIndexDriver('sqlite:///index.sq3')
        baseid = str(uuid.uuid4())

        NUMBER_OF_RECORD = 3

        dids = []
        revs = []
        dts = []

        for i in xrange(NUMBER_OF_RECORD):

            did = str(uuid.uuid4())
            rev = str(uuid.uuid4())[:8]
            size = 512
            form = 'object'
            dt = datetime.now()

            dids.append(did)
            revs.append(rev)
            dts.append(dt)

            conn.execute('''
                INSERT INTO index_record(did, baseid, rev, form, size, last_updated) VALUES (?,?,?,?,?,?)
            ''', (did, baseid, rev, form, size, dt))

        conn.commit()

        records = driver.get_all_versions(baseid)
        assert len(records) == NUMBER_OF_RECORD, 'the number of records does not match'

        for i in xrange(0,NUMBER_OF_RECORD):
            record = records[i]
            assert record['did'] == dids[i], 'record id does not match'
            assert record['rev'] == revs[i], 'record revision does not match'
            assert record['size'] == size, 'record size does not match'
            assert record['form'] == form, 'record form does not match'
            assert record['last_updated'] == dts[i], 'record form does not match'

@util.removes('index.sq3')
def test_driver_get_all_version_with_no_record():
    '''
    Tests retrieval of the lattest record version
    '''
    with sqlite3.connect('index.sq3') as conn:
        driver = SQLAlchemyIndexDriver('sqlite:///index.sq3')
        baseid = str(uuid.uuid4())

        for _ in range(3):

            did = str(uuid.uuid4())
            rev = str(uuid.uuid4())[:8]
            size = 512
            form = 'object'

            conn.execute('''
                INSERT INTO index_record(did, baseid, rev, form, size) VALUES (?,?,?,?,?)
            ''', (did, baseid, rev, form, size))

        conn.commit()

        with pytest.raises(NoRecordFound):
            driver.get_all_versions('some baseid')

@util.removes('index.sq3')
def test_driver_get_fails_with_invalid_id():
    '''
    Tests retrieval of a record fails if the record id is not found.
    '''
    with sqlite3.connect('index.sq3') as conn:

        driver = SQLAlchemyIndexDriver('sqlite:///index.sq3')

        did = str(uuid.uuid4())
        baseid = str(uuid.uuid4())
        rev = str(uuid.uuid4())[:8]
        form = 'object'

        conn.execute('''
            INSERT INTO index_record(did, baseid, rev, form, size) VALUES (?,?,?,?,?)
        ''', (did, baseid, rev, form, None))

        conn.commit()

        with pytest.raises(NoRecordFound):
            driver.get('some_record_that_does_not_exist')

@util.removes('index.sq3')
def test_driver_update_record():
    '''
    Tests updating of a record.
    '''
    with sqlite3.connect('index.sq3') as conn:

        driver = SQLAlchemyIndexDriver('sqlite:///index.sq3')

        did = str(uuid.uuid4())
        baseid = str(uuid.uuid4())
        rev = str(uuid.uuid4())[:8]
        form = 'object'

        conn.execute('''
            INSERT INTO index_record(did, baseid, rev, form, size) VALUES (?,?,?,?,?)
        ''', (did, baseid, rev, form, None))

        conn.commit()

        update_size = 256
        update_urls = ['a', 'b', 'c']
        update_hashes = {
            'a': '1',
            'b': '2',
            'c': '3',
        }

        driver.update(did, rev,
            urls=update_urls
        )

        new_did, new_rev, new_form, new_size = conn.execute('''
            SELECT did, rev, form, size FROM index_record
        ''').fetchone()

        new_urls = sorted(url[0] for url in conn.execute('''
            SELECT url FROM index_record_url
        '''))

        new_hashes = {h:v for h,v in conn.execute('''
            SELECT hash_type, hash_value FROM index_record_hash
        ''')}

        assert did == new_did, 'record id does not match'
        assert rev != new_rev, 'record revision matches prior'
        assert update_urls == new_urls, 'record urls mismatch'

@util.removes('index.sq3')
def test_driver_update_fails_with_no_records():
    '''
    Tests updating a record fails if there are no records.
    '''
    driver = SQLAlchemyIndexDriver('sqlite:///index.sq3')

    with pytest.raises(NoRecordFound):
        driver.update('some_record_that_does_not_exist', 'some_base_version', 'some_revision')

@util.removes('index.sq3')
def test_driver_update_fails_with_invalid_id():
    '''
    Tests updating a record fails if the record id is not found.
    '''
    with sqlite3.connect('index.sq3') as conn:

        driver = SQLAlchemyIndexDriver('sqlite:///index.sq3')

        did = str(uuid.uuid4())
        baseid = str(uuid.uuid4())
        rev = str(uuid.uuid4())[:8]
        form = 'object'

        conn.execute('''
            INSERT INTO index_record(did, baseid, rev, form, size) VALUES (?,?,?,?,?)
        ''', (did, baseid, rev, form, None))

        conn.commit()

        with pytest.raises(NoRecordFound):
            driver.update('some_record_that_does_not_exist','some_record_version', rev)

@util.removes('index.sq3')
def test_driver_update_fails_with_invalid_rev():
    '''
    Tests updating a record fails if the record rev is not invalid.
    '''
    with sqlite3.connect('index.sq3') as conn:

        driver = SQLAlchemyIndexDriver('sqlite:///index.sq3')

        did = str(uuid.uuid4())
        baseid = str(uuid.uuid4())
        rev = str(uuid.uuid4())[:8]
        form = 'object'

        conn.execute('''
            INSERT INTO index_record(did, baseid, rev, form, size) VALUES (?,?,?,?,?)
        ''', (did, baseid, rev, form, None))

        conn.commit()

        with pytest.raises(RevisionMismatch):
            driver.update(did, baseid, 'some_revision')

@util.removes('index.sq3')
def test_driver_delete_record():
    '''
    Tests deletion of a record.
    '''
    with sqlite3.connect('index.sq3') as conn:

        driver = SQLAlchemyIndexDriver('sqlite:///index.sq3')

        did = str(uuid.uuid4())
        baseid = str(uuid.uuid4())
        rev = str(uuid.uuid4())[:8]
        form = 'object'

        conn.execute('''
            INSERT INTO index_record(did, baseid, rev, form, size) VALUES (?,?,?,?,?)
        ''', (did, baseid, rev, form, None))

        conn.commit()

        driver.delete(did, rev)

        count = conn.execute('''
            SELECT COUNT(*) FROM index_record
        ''').fetchone()[0]

        assert count == 0, 'records remain after deletion'

@util.removes('index.sq3')
def test_driver_delete_fails_with_no_records():
    '''
    Tests deletion of a record fails if there are no records.
    '''
    driver = SQLAlchemyIndexDriver('sqlite:///index.sq3')

    with pytest.raises(NoRecordFound):
        driver.delete('some_record_that_does_not_exist', 'some_revision')

@util.removes('index.sq3')
def test_driver_delete_fails_with_invalid_id():
    '''
    Tests deletion of a record fails if the record id is not found.
    '''
    with sqlite3.connect('index.sq3') as conn:

        driver = SQLAlchemyIndexDriver('sqlite:///index.sq3')

        did = str(uuid.uuid4())
        baseid = str(uuid.uuid4())
        rev = str(uuid.uuid4())[:8]
        form = 'object'

        conn.execute('''
            INSERT INTO index_record(did, baseid, rev, form, size) VALUES (?,?,?,?,?)
        ''', (did, baseid, rev, form, None))

        conn.commit()

        with pytest.raises(NoRecordFound):
            driver.delete('some_record_that_does_not_exist', rev)

@util.removes('index.sq3')
def test_driver_delete_fails_with_invalid_rev():
    '''
    Tests deletion of a record fails if the record rev is not invalid.
    '''
    with sqlite3.connect('index.sq3') as conn:

        driver = SQLAlchemyIndexDriver('sqlite:///index.sq3')

        did = str(uuid.uuid4())
        baseid = str(uuid.uuid4())
        rev = str(uuid.uuid4())[:8]
        form = 'object'

        conn.execute('''
            INSERT INTO index_record(did, baseid, rev, form, size) VALUES (?,?,?,?,?)
        ''', (did, baseid, rev, form, None))

        conn.commit()

        with pytest.raises(RevisionMismatch):
            driver.delete(did, 'some_revision')
