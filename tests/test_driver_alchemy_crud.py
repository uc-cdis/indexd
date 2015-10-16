import uuid
import sqlite3

import pytest

import util
import indexd

from indexd.index.errors import NoRecordFound
from indexd.index.errors import MultipleRecordsFound
from indexd.index.errors import RevisionMismatch

from indexd.index.drivers.alchemy import SQLAlchemyIndexDriver


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
        ''').next()[0]
        
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
        ''').next()[0]
        
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
        ''').next()[0]
        
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
        ''').next()[0]
        
        assert count == 1, 'driver did not create record'
        
        record = conn.execute('''
            SELECT * FROM index_record
        ''').next()
        
        assert record[0], 'record id not populated'
        assert record[1], 'record rev not populated'
        assert record[2] == 'object', 'record form is not object'
        assert record[3] == None, 'record size non-null'

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
        ''').next()[0]
        
        assert count == 1, 'driver did not create record'
        
        record = conn.execute('''
            SELECT * FROM index_record
        ''').next()
        
        assert record[0], 'record id not populated'
        assert record[1], 'record rev not populated'
        assert record[2] == 'container', 'record form is not container'
        assert record[3] == None, 'record size non-null'

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
        ''').next()[0]
        
        assert count == 1, 'driver did not create record'
        
        record = conn.execute('''
            SELECT * FROM index_record
        ''').next()
        
        assert record[0], 'record id not populated'
        assert record[1], 'record rev not populated'
        assert record[2] == 'multipart', 'record form is not multipart'
        assert record[3] == None, 'record size non-null'

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
        ''').next()[0]
        
        assert count == 3, 'driver did not create record(s)'
        
        records = conn.execute('''
            SELECT * FROM index_record
        ''')
        
        for record in records:
            assert record[0], 'record id not populated'
            assert record[1], 'record rev not populated'
            assert record[2] == 'object', 'record form is not object'
            assert record[3] == None, 'record size non-null'

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
        ''').next()[0]
        
        assert count == 1, 'driver did not create record'
        
        new_form, new_size = conn.execute('''
            SELECT form, size FROM index_record
        ''').next()
        
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
        ''').next()[0]
        
        assert count == 1, 'driver did not create record'
        
        count = conn.execute('''
            SELECT COUNT(*) FROM index_record_url
        ''').next()[0]
        
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
        ''').next()[0]
        
        assert count == 1, 'driver did not create record'
        
        count = conn.execute('''
            SELECT COUNT(*) FROM index_record_hash
        ''').next()[0]
        
        assert count == 3, 'driver did not create hash(es)'
        
        new_hashes = {h:v for h, v in conn.execute('''
            SELECT hash_type, hash_value FROM index_record_hash
        ''')}
        
        assert hashes == new_hashes, 'record hashes mismatch'

@util.removes('index.sq3')
def test_driver_add_fails_with_invalid_forms():
    '''
    Tests creation of a record with invalid form will fail.
    '''
    driver = SQLAlchemyIndexDriver('sqlite:///index.sq3')

    with pytest.raises(ValueError):
        driver.add('this_is_not_a_valid_form')

@util.removes('index.sq3')
def test_driver_add_fails_with_invalid_sizes():
    '''
    Tests creation of a record with invalid size will fail.
    '''
    driver = SQLAlchemyIndexDriver('sqlite:///index.sq3')

    with pytest.raises(ValueError):
        driver.add('object', size=-1)

@util.removes('index.sq3')
def test_driver_get_record():
    '''
    Tests retrieval of a record.
    '''
    with sqlite3.connect('index.sq3') as conn:
        
        driver = SQLAlchemyIndexDriver('sqlite:///index.sq3')
        
        did = str(uuid.uuid4())
        rev = str(uuid.uuid4())[:8]
        size = 512
        form = 'object'
        
        conn.execute('''
            INSERT INTO index_record VALUES (?,?,?,?)
        ''', (did, rev, form, size))
        
        conn.commit()
        
        record = driver.get(did)
        
        assert record['did'] == did, 'record id does not match'
        assert record['rev'] == rev, 'record revision does not match'
        assert record['size'] == size, 'record size does not match'
        assert record['form'] == form, 'record form does not match'

@util.removes('index.sq3')
def test_driver_get_fails_with_no_records():
    '''
    Tests retrieval of a record fails if there are no records.
    '''
    driver = SQLAlchemyIndexDriver('sqlite:///index.sq3')

    with pytest.raises(NoRecordFound):
        driver.get('some_record_that_does_not_exist')

@util.removes('index.sq3')
def test_driver_get_fails_with_invalid_id():
    '''
    Tests retrieval of a record fails if the record id is not found.
    '''
    with sqlite3.connect('index.sq3') as conn:
        
        driver = SQLAlchemyIndexDriver('sqlite:///index.sq3')
        
        did = str(uuid.uuid4())
        rev = str(uuid.uuid4())[:8]
        form = 'object'
        
        conn.execute('''
            INSERT INTO index_record VALUES (?,?,?,?)
        ''', (did, rev, form, None))
        
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
        rev = str(uuid.uuid4())[:8]
        form = 'object'
        
        conn.execute('''
            INSERT INTO index_record VALUES (?,?,?,?)
        ''', (did, rev, form, None))
        
        conn.commit()
        
        update_size = 256
        update_urls = ['a', 'b', 'c']
        update_hashes = {
            'a': '1',
            'b': '2',
            'c': '3',
        }
        
        driver.update(did, rev,
            size=update_size,
            urls=update_urls,
            hashes=update_hashes,
        )
        
        new_did, new_rev, new_form, new_size = conn.execute('''
            SELECT did, rev, form, size FROM index_record
        ''').next()
        
        new_urls = sorted(url[0] for url in conn.execute('''
            SELECT url FROM index_record_url
        '''))
        
        new_hashes = {h:v for h,v in conn.execute('''
            SELECT hash_type, hash_value FROM index_record_hash
        ''')}
        
        assert did == new_did, 'record id does not match'
        assert rev != new_rev, 'record revision matches prior'
        assert form == new_form, 'record form does not match'
        assert update_size == new_size, 'record size mismatch'
        assert update_urls == new_urls, 'record urls mismatch'
        assert update_hashes == new_hashes, 'record hashes mismatch'

@util.removes('index.sq3')
def test_driver_update_fails_with_no_records():
    '''
    Tests updating a record fails if there are no records.
    '''
    driver = SQLAlchemyIndexDriver('sqlite:///index.sq3')

    with pytest.raises(NoRecordFound):
        driver.update('some_record_that_does_not_exist', 'some_revision')

@util.removes('index.sq3')
def test_driver_update_fails_with_invalid_id():
    '''
    Tests updating a record fails if the record id is not found.
    '''
    with sqlite3.connect('index.sq3') as conn:
        
        driver = SQLAlchemyIndexDriver('sqlite:///index.sq3')
        
        did = str(uuid.uuid4())
        rev = str(uuid.uuid4())[:8]
        form = 'object'
        
        conn.execute('''
            INSERT INTO index_record VALUES (?,?,?,?)
        ''', (did, rev, form, None))
        
        conn.commit()
        
        with pytest.raises(NoRecordFound):
            driver.update('some_record_that_does_not_exist', rev)

@util.removes('index.sq3')
def test_driver_update_fails_with_invalid_rev():
    '''
    Tests updating a record fails if the record rev is not invalid.
    '''
    with sqlite3.connect('index.sq3') as conn:
        
        driver = SQLAlchemyIndexDriver('sqlite:///index.sq3')
        
        did = str(uuid.uuid4())
        rev = str(uuid.uuid4())[:8]
        form = 'object'
        
        conn.execute('''
            INSERT INTO index_record VALUES (?,?,?,?)
        ''', (did, rev, form, None))
        
        conn.commit()
        
        with pytest.raises(RevisionMismatch):
            driver.update(did, 'some_revision')

@util.removes('index.sq3')
def test_driver_delete_record():
    '''
    Tests deletion of a record.
    '''
    with sqlite3.connect('index.sq3') as conn:
        
        driver = SQLAlchemyIndexDriver('sqlite:///index.sq3')
        
        did = str(uuid.uuid4())
        rev = str(uuid.uuid4())[:8]
        form = 'object'
        
        conn.execute('''
            INSERT INTO index_record VALUES (?,?,?,?)
        ''', (did, rev, form, None))
        
        conn.commit()
        
        driver.delete(did, rev)
        
        count = conn.execute('''
            SELECT COUNT(*) FROM index_record
        ''').next()[0]
        
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
        rev = str(uuid.uuid4())[:8]
        form = 'object'
        
        conn.execute('''
            INSERT INTO index_record VALUES (?,?,?,?)
        ''', (did, rev, form, None))
        
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
        rev = str(uuid.uuid4())[:8]
        form = 'object'
        
        conn.execute('''
            INSERT INTO index_record VALUES (?,?,?,?)
        ''', (did, rev, form, None))
        
        conn.commit()
        
        with pytest.raises(RevisionMismatch):
            driver.delete(did, 'some_revision')
