import json
import uuid
from datetime import datetime

import pytest
import sqlalchemy

from indexd.errors import UserError
from indexd.index.drivers.alchemy import IndexRecord
from indexd.index.errors import NoRecordFoundError, RevisionMismatchError
from tests.integration.util import make_sql_statement

# TODO check if pytest has utilities for meta-programming of tests


@pytest.fixture(params=([], [1, 2, 3], [20, 10000000000000000]))
def records_with_size(request, index_driver):
    with index_driver.session as sxn:
        for s in request.param:
            record = IndexRecord(
                did=str(uuid.uuid4()),
                baseid=str(uuid.uuid4()),
                rev=str(uuid.uuid4())[:8],
                size=s,
            )
            sxn.add(record)

    return request.param


def insert_base_data(database_conn):
    did = str(uuid.uuid4())
    baseid = str(uuid.uuid4())
    rev = str(uuid.uuid4())[:8]
    form = "object"

    database_conn.execute(
        make_sql_statement(
            """
        INSERT INTO index_record(did, baseid, rev, form, size) VALUES (?,?,?,?,?)
    """,
            (did, baseid, rev, form, 1),
        )
    )
    database_conn.commit()
    return did, baseid, rev, form


def test_get_total_bytes(index_driver, records_with_size):
    """Test that totalbytes return expected results"""
    assert sum(records_with_size) == index_driver.totalbytes()


def test_driver_init_does_not_create_records(index_driver, database_conn):
    """
    Tests for creation of records after driver init.
    Tests driver init does not have unexpected side-effects.
    """
    count = database_conn.execute(
        sqlalchemy.text("""
        SELECT COUNT(*) FROM index_record
    """)
    ).fetchone()[0]

    assert count == 0, "driver created records upon initilization"


def test_driver_init_does_not_create_record_urls(index_driver, database_conn):
    """
    Tests for creation of urls after driver init.
    Tests driver init does not have unexpected side-effects.
    """
    count = database_conn.execute(
        sqlalchemy.text("""
        SELECT COUNT(*) FROM index_record_url
    """)
    ).fetchone()[0]

    assert count == 0, "driver created records urls upon initialization"


def test_driver_init_does_not_create_record_hashes(index_driver, database_conn):
    """
    Tests for creation of hashes after driver init.
    Tests driver init does not have unexpected side-effects.
    """

    count = database_conn.execute(
        sqlalchemy.text("""
        SELECT COUNT(*) FROM index_record_hash
    """)
    ).fetchone()[0]

    assert count == 0, "driver created records hashes upon initialization"


def test_driver_add_object_record(index_driver, database_conn):
    """
    Tests creation of a record.
    """

    index_driver.add("object")

    count = database_conn.execute(
        sqlalchemy.text("""
        SELECT COUNT(*) FROM index_record
    """)
    ).fetchone()[0]

    assert count == 1, "driver did not create record"

    record = database_conn.execute(
        sqlalchemy.text("""
        SELECT * FROM index_record
    """)
    ).fetchone()

    assert record[0], "record id not populated"
    assert record[1], "record baseid not populated"
    assert record[2], "record rev not populated"
    assert record[3] == "object", "record form is not object"
    assert record[4] is None, "record size non-null"


def test_driver_add_container_record(index_driver, database_conn):
    """
    Tests creation of a record.
    """

    index_driver.add("container")

    count = database_conn.execute(
        sqlalchemy.text("""
        SELECT COUNT(*) FROM index_record
    """)
    ).fetchone()[0]

    assert count == 1, "driver did not create record"

    record = database_conn.execute(
        sqlalchemy.text("""
        SELECT * FROM index_record
    """)
    ).fetchone()

    assert record[0], "record id not populated"
    assert record[1], "record baseid not populated"
    assert record[2], "record rev not populated"
    assert record[3] == "container", "record form is not container"
    assert record[4] is None, "record size non-null"


def test_driver_add_multipart_record(index_driver, database_conn):
    """
    Tests creation of a record.
    """

    index_driver.add("multipart")

    count = database_conn.execute(
        sqlalchemy.text("""
        SELECT COUNT(*) FROM index_record
    """)
    ).fetchone()[0]

    assert count == 1, "driver did not create record"

    record = database_conn.execute(
        sqlalchemy.text("""
        SELECT * FROM index_record
    """)
    ).fetchone()

    assert record[0], "record id not populated"
    assert record[1], "record baseid not populated"
    assert record[2], "record rev not populated"
    assert record[3] == "multipart", "record form is not multipart"
    assert record[4] is None, "record size non-null"


def test_driver_add_with_valid_did(index_driver):
    """
    Tests creation of a record with given valid did.
    """

    form = "object"
    did = "3d313755-cbb4-4b08-899d-7bbac1f6e67d"
    index_driver.add(form, did=did)
    with index_driver.session as session:
        assert session.query(IndexRecord).first().did == did


def test_driver_add_with_duplicate_did(index_driver):
    """
    Tests creation of a record with duplicate did.
    """
    form = "object"
    did = "3d313755-cbb4-4b08-899d-7bbac1f6e67d"
    index_driver.add(form, did=did)

    with pytest.raises(UserError):
        index_driver.add(form, did=did)


def test_driver_add_multiple_records(index_driver, database_conn):
    """
    Tests creation of a record.
    """

    index_driver.add("object")
    index_driver.add("object")
    index_driver.add("object")

    count = database_conn.execute(
        sqlalchemy.text("""
        SELECT COUNT(*) FROM index_record
    """)
    ).fetchone()[0]

    assert count == 3, "driver did not create record(s)"

    records = database_conn.execute(
        sqlalchemy.text("""
        SELECT * FROM index_record
    """)
    )

    for record in records:
        assert record[0], "record id not populated"
        assert record[1], "record baseid not populated"
        assert record[2], "record rev not populated"
        assert record[3] == "object", "record form is not object"
        assert record[4] is None, "record size non-null"


def test_driver_add_with_size(index_driver, database_conn):
    """
    Tests creation of a record with size.
    """

    form = "object"
    size = 512

    index_driver.add(form, size=size)

    count = database_conn.execute(
        sqlalchemy.text("""
        SELECT COUNT(*) FROM index_record
    """)
    ).fetchone()[0]

    assert count == 1, "driver did not create record"

    new_form, new_size = database_conn.execute(
        sqlalchemy.text("""
        SELECT form, size FROM index_record
    """)
    ).fetchone()

    assert form == new_form, "record form mismatch"
    assert size == new_size, "record size mismatch"


def test_driver_add_with_urls(index_driver, database_conn):
    """
    Tests creation of a record with urls.
    """
    form = "object"
    urls_metadata = {"a": {"type": "ok"}, "b": {"type": "ok"}, "c": {"type": "ok"}}
    # urls = ['a', 'b', 'c']

    index_driver.add(form, urls_metadata=urls_metadata)

    count = database_conn.execute(
        sqlalchemy.text("""
        SELECT COUNT(*) FROM index_record
    """)
    ).fetchone()[0]

    assert count == 1, "driver did not create record"

    count = database_conn.execute(
        sqlalchemy.text("""
        SELECT COUNT(url) FROM index_record_url_metadata_jsonb
    """)
    ).fetchone()[0]

    assert count == 3, "driver did not create url(s)"

    new_urls = sorted(
        url[0]
        for url in database_conn.execute(
            sqlalchemy.text("""
        SELECT url FROM index_record_url_metadata_jsonb
    """)
        )
    )

    assert sorted(urls_metadata.keys()) == new_urls, "record urls mismatch"


def test_driver_add_with_filename(index_driver):
    """
    Tests creation of a record with filename.
    """

    form = "object"
    file_name = "abc"
    index_driver.add(form, file_name=file_name)
    with index_driver.session as s:
        assert s.query(IndexRecord).first().file_name == "abc"


def test_driver_add_with_version(index_driver):
    """
    Tests creation of a record with version string.
    """
    form = "object"
    version = "ver_123"
    index_driver.add(form, version=version)
    with index_driver.session as s:
        assert s.query(IndexRecord).first().version == "ver_123"


def test_driver_add_with_hashes(index_driver, database_conn):
    """
    Tests creation of a record with hashes.
    """

    form = "object"
    hashes = {
        "a": "1",
        "b": "2",
        "c": "3",
    }

    index_driver.add(form, hashes=hashes)

    count = database_conn.execute(
        sqlalchemy.text("""
        SELECT COUNT(*) FROM index_record
    """)
    ).fetchone()[0]

    assert count == 1, "driver did not create record"

    count = database_conn.execute(
        sqlalchemy.text("""
        SELECT COUNT(*) FROM index_record_hash
    """)
    ).fetchone()[0]

    assert count == 3, "driver did not create hash(es)"

    new_hashes = {
        h: v
        for h, v in database_conn.execute(
            sqlalchemy.text("""
        SELECT hash_type, hash_value FROM index_record_hash
    """)
        )
    }

    assert hashes == new_hashes, "record hashes mismatch"


def test_driver_get_record(index_driver, database_conn):
    """
    Tests retrieval of a record.
    """
    did = str(uuid.uuid4())
    baseid = str(uuid.uuid4())
    rev = str(uuid.uuid4())[:8]
    size = 512
    form = "object"
    baseid = str(uuid.uuid4())
    created_date = datetime.now()
    updated_date = datetime.now()

    database_conn.execute(
        make_sql_statement(
            """
        INSERT INTO index_record (did, baseid, rev, form, size, created_date, updated_date)
        VALUES (?,?,?,?,?,?,?)
    """,
            (did, baseid, rev, form, size, created_date, updated_date),
        )
    )
    database_conn.commit()

    record = index_driver.get(did)

    assert record["did"] == did, "record id does not match"
    assert record["baseid"] == baseid, "record id does not match"
    assert record["rev"] == rev, "record revision does not match"
    assert record["size"] == size, "record size does not match"
    assert record["form"] == form, "record form does not match"
    assert record["created_date"] == created_date.isoformat(), (
        "created date does not match"
    )
    assert record["updated_date"] == updated_date.isoformat(), (
        "updated date does not match"
    )


def test_driver_get_fails_with_no_records(index_driver):
    """
    Tests retrieval of a record fails if there are no records.
    """
    with pytest.raises(NoRecordFoundError):
        index_driver.get("some_record_that_does_not_exist")


def test_driver_get_latest_version(index_driver, database_conn):
    """
    Tests retrieval of the latest record version
    """
    baseid = str(uuid.uuid4())

    for _ in range(10):
        did = str(uuid.uuid4())
        rev = str(uuid.uuid4())[:8]
        size = 512
        form = "object"
        baseid = str(uuid.uuid4())
        created_date = datetime.now()
        updated_date = datetime.now()

        database_conn.execute(
            make_sql_statement(
                """
            INSERT INTO index_record(did, baseid, rev, form, size, created_date, updated_date) VALUES (?,?,?,?,?,?,?)
        """,
                (did, baseid, rev, form, size, created_date, updated_date),
            )
        )
    database_conn.commit()
    record = index_driver.get_latest_version(did)

    assert record["did"] == did, "record id does not match"
    assert record["rev"] == rev, "record revision does not match"
    assert record["size"] == size, "record size does not match"
    assert record["form"] == form, "record form does not match"
    assert record["created_date"] == created_date.isoformat(), (
        "created date does not match"
    )
    assert record["updated_date"] == updated_date.isoformat(), (
        "updated date does not match"
    )


def test_driver_get_latest_version_with_no_record(index_driver, database_conn):
    """
    Tests retrieval of the latest record version
    """
    for _ in range(10):
        did = str(uuid.uuid4())
        rev = str(uuid.uuid4())[:8]
        size = 512
        form = "object"
        baseid = str(uuid.uuid4())
        dt = datetime.now()

        database_conn.execute(
            make_sql_statement(
                """
            INSERT INTO index_record(did, baseid, rev, form, size, created_date, updated_date) VALUES (?,?,?,?,?,?,?)
        """,
                (did, baseid, rev, form, size, dt, dt),
            )
        )
        database_conn.commit()

    with pytest.raises(NoRecordFoundError):
        index_driver.get_latest_version("some base version")


def test_driver_get_latest_version_exclude_deleted(index_driver, database_conn):
    """
    Tests retrieval of the latest record version not flagged as deleted in index_metadata
    """
    baseid = str(uuid.uuid4())

    is_first_record = True
    for _ in range(10):
        did = str(uuid.uuid4())
        rev = str(uuid.uuid4())[:8]
        size = 512
        form = "object"
        created_date = datetime.now()
        updated_date = datetime.now()

        if is_first_record:
            is_first_record = False
            index_metadata = None
            non_deleted_did = did
        else:
            index_metadata = json.dumps({"deleted": "true"})

        database_conn.execute(
            make_sql_statement(
                """
            INSERT INTO index_record(did, baseid, rev, form, size, created_date, updated_date, index_metadata)
            VALUES (?,?,?,?,?,?,?,?)
        """,
                (
                    did,
                    baseid,
                    rev,
                    form,
                    size,
                    created_date,
                    updated_date,
                    index_metadata,
                ),
            )
        )
    database_conn.commit()

    record = index_driver.get_latest_version(did, exclude_deleted=True)

    assert record["baseid"] == baseid, "record baseid does not match"
    assert record["did"] != did, "record did matches deleted record"
    assert record["did"] == non_deleted_did, (
        "record id does not match non-deleted record"
    )


def test_driver_get_all_version(index_driver, database_conn):
    """
    Tests retrieval of the latest record version
    """
    baseid = str(uuid.uuid4())

    number_of_record = 3

    given_records = {}

    for _ in range(number_of_record):
        did = str(uuid.uuid4())
        rev = str(uuid.uuid4())[:8]
        size = 512
        form = "object"
        created_date = datetime.now()
        updated_date = created_date

        given_records[did] = {
            "rev": rev,
            "size": size,
            "form": form,
            "created_date": created_date,
            "updated_date": updated_date,
        }

        database_conn.execute(
            make_sql_statement(
                """
            INSERT INTO index_record(did, baseid, rev, form, size, created_date, updated_date)
            VALUES (?,?,?,?,?,?,?)
        """,
                (did, baseid, rev, form, size, created_date, updated_date),
            )
        )
    database_conn.commit()

    records = index_driver.get_all_versions(did)
    assert len(records) == number_of_record, "the number of records does not match"

    for i in range(number_of_record):
        record = records[i]
        given_record = given_records[record["did"]]
        assert record["rev"] == given_record["rev"], "record revision does not match"
        assert record["size"] == given_record["size"], "record size does not match"
        assert record["form"] == given_record["form"], "record form does not match"
        assert record["created_date"] == given_record["created_date"].isoformat(), (
            "created date does not match"
        )
        assert record["updated_date"] == given_record["updated_date"].isoformat(), (
            "updated date does not match"
        )


def test_driver_get_all_version_with_no_record(index_driver, database_conn):
    """
    Tests retrieval of the latest record version
    """
    baseid = str(uuid.uuid4())

    for _ in range(3):
        did = str(uuid.uuid4())
        rev = str(uuid.uuid4())[:8]
        size = 512
        form = "object"

        database_conn.execute(
            make_sql_statement(
                """
            INSERT INTO index_record(did, baseid, rev, form, size) VALUES (?,?,?,?,?)
        """,
                (did, baseid, rev, form, size),
            )
        )
    database_conn.commit()

    with pytest.raises(NoRecordFoundError):
        index_driver.get_all_versions("some baseid")


def test_driver_get_all_version_exclude_deleted(index_driver, database_conn):
    """
    Tests retrieval of all versions of a document not flagged as deleted
    """
    baseid = str(uuid.uuid4())

    non_deleted_dids = []
    deleted_dids = []

    for i in range(10):
        did = str(uuid.uuid4())
        rev = str(uuid.uuid4())[:8]
        size = 512
        form = "object"
        created_date = datetime.now()
        updated_date = created_date

        if i % 2 == 0:
            non_deleted_dids.append(did)
            index_metadata = None
        else:
            deleted_dids.append(did)
            index_metadata = json.dumps({"deleted": "true"})

        database_conn.execute(
            make_sql_statement(
                """
            INSERT INTO index_record(did, baseid, rev, form, size, created_date, updated_date, index_metadata)
            VALUES (?,?,?,?,?,?,?,?)
        """,
                (
                    did,
                    baseid,
                    rev,
                    form,
                    size,
                    created_date,
                    updated_date,
                    index_metadata,
                ),
            )
        )
    database_conn.commit()

    records = index_driver.get_all_versions(did, exclude_deleted=True)
    assert len(records) == len(non_deleted_dids), "the number of records does not match"
    assert all([doc["baseid"] == baseid for doc in records.values()]), (
        "record baseid does not match"
    )
    assert {doc["did"] for doc in records.values()} == set(non_deleted_dids), (
        "record did does not match"
    )


def test_driver_get_fails_with_invalid_id(index_driver, database_conn):
    """
    Tests retrieval of a record fails if the record id is not found.
    """

    insert_base_data(database_conn)
    with pytest.raises(NoRecordFoundError):
        index_driver.get("some_record_that_does_not_exist")


def test_driver_update_record_simple_data(index_driver, database_conn):
    did, baseid, rev, form = insert_base_data(database_conn)

    update_size = 256
    file_name = "test"
    version = "ver_123"
    changing_fields = {
        "file_name": file_name,
        "version": version,
        "size": update_size,
    }
    index_driver.update(did, rev, changing_fields)

    new_did, new_rev, new_file_name, new_size, new_version = database_conn.execute(
        sqlalchemy.text("""
        SELECT did, rev, file_name, size, version FROM index_record
    """)
    ).fetchone()

    assert did == new_did, "record id does not match"
    assert rev != new_rev, "record revision matches prior"
    assert file_name == new_file_name, "file_name does not match"
    assert update_size == new_size, "size does not match"
    assert version == new_version, "version does not match"


def test_driver_update_record_hashes(index_driver, database_conn):
    did, _, rev, _ = insert_base_data(database_conn)
    update_hashes = {
        "a": "1",
        "b": "2",
        "c": "3",
    }
    changing_fields = {
        "hashes": update_hashes,
    }
    index_driver.update(did, rev, changing_fields)

    new_hashes = {
        h: v
        for h, v in database_conn.execute(
            sqlalchemy.text("""
        SELECT hash_type, hash_value FROM index_record_hash
    """)
        )
    }
    assert update_hashes == new_hashes, "hashes do not match"


def test_driver_update_record_metadata(index_driver, database_conn):
    did, _, rev, _ = insert_base_data(database_conn)
    update_metadata = {
        "a": "A",
        "b": "B",
    }
    changing_fields = {
        "metadata": update_metadata,
    }
    index_driver.update(did, rev, changing_fields)

    new_metadata = database_conn.execute(
        sqlalchemy.text("""
        SELECT index_metadata FROM index_record
    """)
    ).fetchone()[0]

    assert update_metadata == new_metadata, "metadata does not match"


def test_driver_update_record_release_number_separate(index_driver, database_conn):
    did, _, rev, _ = insert_base_data(database_conn)
    update_release_number = "1.0"
    changing_fields = {
        "release_number": update_release_number,
    }
    index_driver.update(did, rev, changing_fields)

    new_release_number = database_conn.execute(
        sqlalchemy.text("""
        SELECT release_number FROM index_record
    """)
    ).fetchone()[0]

    count = database_conn.execute(
        sqlalchemy.text("""
        SELECT count(did)
        FROM index_record
        WHERE index_metadata ? 'release_number'
    """)
    ).fetchone()[0]

    assert count == 0, "release number should not be in metadata jsonb"
    assert update_release_number == new_release_number, "metadata does not match"


def test_driver_update_record_release_number_metadata(index_driver, database_conn):
    did, _, rev, _ = insert_base_data(database_conn)
    update_release_number = "1.0"
    changing_fields = {
        "metadata": {"release_number": update_release_number},
    }
    index_driver.update(did, rev, changing_fields)

    new_release_number = database_conn.execute(
        sqlalchemy.text("""
        SELECT release_number FROM index_record
    """)
    ).fetchone()[0]

    count = database_conn.execute(
        sqlalchemy.text("""
        SELECT count(did)
        FROM index_record
        WHERE index_metadata ? 'release_number'
    """)
    ).fetchone()[0]

    assert update_release_number == new_release_number, "metadata does not match"
    assert count == 0, "release number should not be in metadata jsonb"


def test_driver_update_record_urls_metadata(index_driver, database_conn):
    """
    Tests updating of a record.
    """
    did, baseid, rev, form = insert_base_data(database_conn)

    update_urls_metadata = {
        "a": {
            "type": "ok",
            "not type": "ok",
        },
        "b": {
            "state": "not ok",
        },
        "c": {
            "not type": "not ok",
        },
    }

    changing_fields = {
        "urls_metadata": update_urls_metadata,
    }

    index_driver.update(did, rev, changing_fields)

    query = database_conn.execute(
        sqlalchemy.text("""
        SELECT url, state, type, urls_metadata
        FROM index_record_url_metadata_jsonb
    """)
    )
    new_urls_metadata = {}
    for row in query:
        new_urls_metadata[row.url] = row.urls_metadata
        # Don't add the key if there's no value.
        if row.type:
            new_urls_metadata[row.url]["type"] = row.type
        if row.state:
            new_urls_metadata[row.url]["state"] = row.state

    assert update_urls_metadata == new_urls_metadata, "record urls_metadata mismatch"


def test_driver_update_fails_with_no_records(index_driver):
    """
    Tests updating a record fails if there are no records.
    """
    with pytest.raises(NoRecordFoundError):
        index_driver.update(
            "some_record_that_does_not_exist", "some_base_version", "some_revision"
        )


def test_driver_update_fails_with_invalid_id(index_driver, database_conn):
    """
    Tests updating a record fails if the record id is not found.
    """
    did = str(uuid.uuid4())
    baseid = str(uuid.uuid4())
    rev = str(uuid.uuid4())[:8]
    form = "object"

    database_conn.execute(
        make_sql_statement(
            """
        INSERT INTO index_record(did, baseid, rev, form, size) VALUES (?,?,?,?,?)
    """,
            (did, baseid, rev, form, None),
        )
    )
    database_conn.commit()

    with pytest.raises(NoRecordFoundError):
        index_driver.update(
            "some_record_that_does_not_exist", "some_record_version", rev
        )


def test_driver_update_fails_with_invalid_rev(index_driver, database_conn):
    """
    Tests updating a record fails if the record rev is not invalid.
    """

    did = str(uuid.uuid4())
    baseid = str(uuid.uuid4())
    rev = str(uuid.uuid4())[:8]
    form = "object"

    database_conn.execute(
        make_sql_statement(
            """
        INSERT INTO index_record(did, baseid, rev, form, size) VALUES (?,?,?,?,?)
    """,
            (did, baseid, rev, form, None),
        )
    )
    database_conn.commit()

    with pytest.raises(RevisionMismatchError):
        index_driver.update(did, baseid, "some_revision")


def test_driver_delete_record(index_driver, database_conn):
    """
    Tests deletion of a record.
    """
    did = str(uuid.uuid4())
    baseid = str(uuid.uuid4())
    rev = str(uuid.uuid4())[:8]
    form = "object"

    database_conn.execute(
        make_sql_statement(
            """
        INSERT INTO index_record(did, baseid, rev, form, size) VALUES (?,?,?,?,?)
    """,
            (did, baseid, rev, form, None),
        )
    )
    database_conn.commit()

    index_driver.delete(did, rev)

    count = database_conn.execute(
        sqlalchemy.text("""
        SELECT COUNT(*) FROM index_record
    """)
    ).fetchone()[0]

    assert count == 0, "records remain after deletion"


def test_driver_delete_fails_with_no_records(index_driver):
    """
    Tests deletion of a record fails if there are no records.
    """
    with pytest.raises(NoRecordFoundError):
        index_driver.delete("some_record_that_does_not_exist", "some_revision")


def test_driver_delete_fails_with_invalid_id(index_driver, database_conn):
    """
    Tests deletion of a record fails if the record id is not found.
    """

    did = str(uuid.uuid4())
    baseid = str(uuid.uuid4())
    rev = str(uuid.uuid4())[:8]
    form = "object"

    database_conn.execute(
        make_sql_statement(
            """
        INSERT INTO index_record(did, baseid, rev, form, size) VALUES (?,?,?,?,?)
    """,
            (did, baseid, rev, form, None),
        )
    )
    database_conn.commit()

    with pytest.raises(NoRecordFoundError):
        index_driver.delete("some_record_that_does_not_exist", rev)


def test_driver_delete_fails_with_invalid_rev(index_driver, database_conn):
    """
    Tests deletion of a record fails if the record rev is not invalid.
    """
    did = str(uuid.uuid4())
    baseid = str(uuid.uuid4())
    rev = str(uuid.uuid4())[:8]
    form = "object"

    database_conn.execute(
        make_sql_statement(
            """
        INSERT INTO index_record(did, baseid, rev, form, size) VALUES (?,?,?,?,?)
    """,
            (did, baseid, rev, form, None),
        )
    )
    database_conn.commit()

    with pytest.raises(RevisionMismatchError):
        index_driver.delete(did, "some_revision")


def test_driver_bulk_get_latest_versions_exclude_deleted(index_driver, database_conn):
    """
    Tests bulk retrieval of the latest record version not flagged as deleted for each document in a list of dids
    """
    # generate baseids
    baseids = {}
    for _ in range(5):
        baseids[str(uuid.uuid4())] = {}

    # create set of deleted and non-deleted documents for each baseid
    for baseid in baseids.keys():
        is_first_record = True
        for _ in range(10):
            did = str(uuid.uuid4())
            rev = str(uuid.uuid4())[:8]
            size = 512
            form = "object"
            created_date = datetime.now()
            updated_date = datetime.now()

            if is_first_record:
                is_first_record = False
                index_metadata = None
                baseids[baseid]["non_deleted_did"] = did
            else:
                index_metadata = json.dumps({"deleted": "true"})
                baseids[baseid]["deleted_did"] = did

            database_conn.execute(
                make_sql_statement(
                    """
                INSERT INTO index_record(did, baseid, rev, form, size, created_date, updated_date, index_metadata)
                VALUES (?,?,?,?,?,?,?,?)
            """,
                    (
                        did,
                        baseid,
                        rev,
                        form,
                        size,
                        created_date,
                        updated_date,
                        index_metadata,
                    ),
                )
            )
    database_conn.commit()
    non_deleted_dids = [baseids[baseid]["non_deleted_did"] for baseid in baseids.keys()]
    deleted_dids = [baseids[baseid]["deleted_did"] for baseid in baseids.keys()]

    # get latest non-deleted records from the list of deleted dids
    records = index_driver.bulk_get_latest_versions(deleted_dids, exclude_deleted=True)
    assert len(records) == len(non_deleted_dids), "the number of records does not match"
    assert {record["baseid"] for record in records} == set(baseids.keys()), (
        "one ore more baseid does not match"
    )
    assert {record["did"] for record in records} == set(non_deleted_dids), (
        "one or more non-deleted record missing"
    )
    assert all(record["did"] not in deleted_dids for record in records), (
        "one or more deleted record returned"
    )
