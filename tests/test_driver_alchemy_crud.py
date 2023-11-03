import uuid
import sqlite3

import pytest

import tests.util as util

from indexd.index.errors import NoRecordFound
from indexd.index.errors import RevisionMismatch

from indexd.index.errors import MultipleRecordsFound

from indexd.index.drivers.alchemy import SQLAlchemyIndexDriver, IndexRecord

from datetime import datetime


# TODO check if pytest has utilities for meta-programming of tests


@util.removes("index.sq3")
def test_driver_init_does_not_create_records():
    """
    Tests for creation of records after driver init.
    Tests driver init does not have unexpected side-effects.
    """
    with sqlite3.connect("index.sq3") as conn:
        driver = SQLAlchemyIndexDriver("sqlite:///index.sq3")

        count = conn.execute(
            """
            SELECT COUNT(*) FROM index_record
        """
        ).fetchone()[0]

        assert count == 0, "driver created records upon initilization"


@util.removes("index.sq3")
def test_driver_init_does_not_create_record_urls():
    """
    Tests for creation of urls after driver init.
    Tests driver init does not have unexpected side-effects.
    """
    with sqlite3.connect("index.sq3") as conn:
        driver = SQLAlchemyIndexDriver("sqlite:///index.sq3")

        count = conn.execute(
            """
            SELECT COUNT(*) FROM index_record_url
        """
        ).fetchone()[0]

        assert count == 0, "driver created records urls upon initilization"


@util.removes("index.sq3")
def test_driver_init_does_not_create_record_hashes():
    """
    Tests for creation of hashes after driver init.
    Tests driver init does not have unexpected side-effects.
    """
    with sqlite3.connect("index.sq3") as conn:
        driver = SQLAlchemyIndexDriver(
            "sqlite:///index.sq3"
        )  # pylint: disable=unused-variable

        count = conn.execute(
            """
            SELECT COUNT(*) FROM index_record_hash
        """
        ).fetchone()[0]

        assert count == 0, "driver created records hashes upon initilization"


@util.removes("index.sq3")
def test_driver_add_object_record():
    """
    Tests creation of a record.
    """
    with sqlite3.connect("index.sq3") as conn:
        driver = SQLAlchemyIndexDriver("sqlite:///index.sq3")

        driver.add("object")

        count = conn.execute(
            """
            SELECT COUNT(*) FROM index_record
        """
        ).fetchone()[0]

        assert count == 1, "driver did not create record"

        record = conn.execute(
            """
            SELECT * FROM index_record
        """
        ).fetchone()

        assert record[0], "record id not populated"
        assert record[1], "record baseid not populated"
        assert record[2], "record rev not populated"
        assert record[3] == "object", "record form is not object"
        assert record[4] is None, "record size non-null"


@util.removes("index.sq3")
def test_driver_add_bundle_record():
    """
    Tests creation of a record.
    """
    with sqlite3.connect("index.sq3") as conn:
        driver = SQLAlchemyIndexDriver("sqlite:///index.sq3")

        driver.add_blank_bundle()

        count = conn.execute(
            """
            SELECT COUNT(*) FROM drs_bundle_record
        """
        ).fetchone()[0]

        assert count == 1, "driver did not create record"

        record = conn.execute(
            """
            SELECT * FROM drs_bundle_record
        """
        ).fetchone()

        assert record != None
        assert len(record) == 10


@util.removes("index.sq3")
def test_driver_add_container_record():
    """
    Tests creation of a record.
    """
    with sqlite3.connect("index.sq3") as conn:
        driver = SQLAlchemyIndexDriver("sqlite:///index.sq3")

        driver.add("container")

        count = conn.execute(
            """
            SELECT COUNT(*) FROM index_record
        """
        ).fetchone()[0]

        assert count == 1, "driver did not create record"

        record = conn.execute(
            """
            SELECT * FROM index_record
        """
        ).fetchone()

        assert record[0], "record id not populated"
        assert record[1], "record baseid not populated"
        assert record[2], "record rev not populated"
        assert record[3] == "container", "record form is not container"
        assert record[4] == None, "record size non-null"


@util.removes("index.sq3")
def test_driver_add_bundles_record():
    """
    Tests creation of a record.
    """
    with sqlite3.connect("index.sq3") as conn:
        driver = SQLAlchemyIndexDriver("sqlite:///index.sq3")

        driver.add_bundle(name="bundle")

        count = conn.execute(
            """
            SELECT COUNT(*) FROM drs_bundle_record
        """
        ).fetchone()[0]

        assert count == 1, "driver did not create record"

        record = conn.execute(
            """
            SELECT * FROM drs_bundle_record
        """
        ).fetchone()
        assert record[0], "record id not populated"
        assert record[1], "record name not populated"
        assert record[1] == "bundle", "record name is not bundle"
        assert record[2], "record created date not populated"
        assert record[3], "record updated date not populated"


@util.removes("index.sq3")
def test_driver_add_multipart_record():
    """
    Tests creation of a record.
    """
    with sqlite3.connect("index.sq3") as conn:
        driver = SQLAlchemyIndexDriver("sqlite:///index.sq3")

        driver.add("multipart")

        count = conn.execute(
            """
            SELECT COUNT(*) FROM index_record
        """
        ).fetchone()[0]

        assert count == 1, "driver did not create record"

        record = conn.execute(
            """
            SELECT * FROM index_record
        """
        ).fetchone()

        assert record[0], "record id not populated"
        assert record[1], "record baseid not populated"
        assert record[2], "record rev not populated"
        assert record[3] == "multipart", "record form is not multipart"
        assert record[4] == None, "record size non-null"


@util.removes("index.sq3")
def test_driver_add_with_valid_did():
    """
    Tests creation of a record with given valid did.
    """
    driver = SQLAlchemyIndexDriver("sqlite:///index.sq3")

    form = "object"
    did = "3d313755-cbb4-4b08-899d-7bbac1f6e67d"
    driver.add(form, did=did)
    with driver.session as s:
        assert s.query(IndexRecord).first().did == did


@util.removes("index.sq3")
def test_driver_add_with_duplicate_did():
    """
    Tests creation of a record with duplicate did.
    """
    driver = SQLAlchemyIndexDriver("sqlite:///index.sq3")

    form = "object"
    did = "3d313755-cbb4-4b08-899d-7bbac1f6e67d"
    driver.add(form, did=did)

    with pytest.raises(MultipleRecordsFound):
        driver.add(form, did=did)


@util.removes("index.sq3")
def test_driver_add_multiple_records():
    """
    Tests creation of a record.
    """
    with sqlite3.connect("index.sq3") as conn:
        driver = SQLAlchemyIndexDriver("sqlite:///index.sq3")

        driver.add("object")
        driver.add("object")
        driver.add("object")

        count = conn.execute(
            """
            SELECT COUNT(*) FROM index_record
        """
        ).fetchone()[0]

        assert count == 3, "driver did not create record(s)"

        records = conn.execute(
            """
            SELECT * FROM index_record
        """
        )

        for record in records:
            assert record[0], "record id not populated"
            assert record[1], "record baseid not populated"
            assert record[2], "record rev not populated"
            assert record[3] == "object", "record form is not object"
            assert record[4] == None, "record size non-null"


@util.removes("index.sq3")
def test_driver_add_with_size():
    """
    Tests creation of a record with size.
    """
    with sqlite3.connect("index.sq3") as conn:
        driver = SQLAlchemyIndexDriver("sqlite:///index.sq3")

        form = "object"
        size = 512

        driver.add(form, size=size)

        count = conn.execute(
            """
            SELECT COUNT(*) FROM index_record
        """
        ).fetchone()[0]

        assert count == 1, "driver did not create record"

        new_form, new_size = conn.execute(
            """
            SELECT form, size FROM index_record
        """
        ).fetchone()

        assert form == new_form, "record form mismatch"
        assert size == new_size, "record size mismatch"


@util.removes("index.sq3")
def test_driver_add_with_urls():
    """
    Tests creation of a record with urls.
    """
    with sqlite3.connect("index.sq3") as conn:
        driver = SQLAlchemyIndexDriver("sqlite:///index.sq3")

        form = "object"
        urls = ["a", "b", "c"]

        driver.add(form, urls=urls)

        count = conn.execute(
            """
            SELECT COUNT(*) FROM index_record
        """
        ).fetchone()[0]

        assert count == 1, "driver did not create record"

        count = conn.execute(
            """
            SELECT COUNT(*) FROM index_record_url
        """
        ).fetchone()[0]

        assert count == 3, "driver did not create url(s)"

        new_urls = sorted(
            url[0]
            for url in conn.execute(
                """
            SELECT url FROM index_record_url
        """
            )
        )

        assert urls == new_urls, "record urls mismatch"


@util.removes("index.sq3")
def test_driver_add_with_filename():
    """
    Tests creation of a record with filename.
    """
    driver = SQLAlchemyIndexDriver("sqlite:///index.sq3")

    form = "object"
    file_name = "abc"
    driver.add(form, file_name=file_name)
    with driver.session as s:
        assert s.query(IndexRecord).first().file_name == "abc"


@util.removes("index.sq3")
def test_driver_add_with_version():
    """
    Tests creation of a record with version string.
    """
    driver = SQLAlchemyIndexDriver("sqlite:///index.sq3")

    form = "object"
    version = "ver_123"
    driver.add(form, version=version)
    with driver.session as s:
        assert s.query(IndexRecord).first().version == "ver_123"


@util.removes("index.sq3")
def test_driver_add_with_hashes():
    """
    Tests creation of a record with hashes.
    """
    with sqlite3.connect("index.sq3") as conn:
        driver = SQLAlchemyIndexDriver("sqlite:///index.sq3")

        form = "object"
        hashes = {"a": "1", "b": "2", "c": "3"}

        driver.add(form, hashes=hashes)

        count = conn.execute(
            """
            SELECT COUNT(*) FROM index_record
        """
        ).fetchone()[0]

        assert count == 1, "driver did not create record"

        count = conn.execute(
            """
            SELECT COUNT(*) FROM index_record_hash
        """
        ).fetchone()[0]

        assert count == 3, "driver did not create hash(es)"

        new_hashes = {
            h: v
            for h, v in conn.execute(
                """
            SELECT hash_type, hash_value FROM index_record_hash
        """
            )
        }

        assert hashes == new_hashes, "record hashes mismatch"


@util.removes("index.sq3")
def test_driver_get_record():
    """
    Tests retrieval of a record.
    """
    with sqlite3.connect("index.sq3") as conn:
        driver = SQLAlchemyIndexDriver("sqlite:///index.sq3")

        did = str(uuid.uuid4())
        baseid = str(uuid.uuid4())
        rev = str(uuid.uuid4())[:8]
        size = 512
        form = "object"
        baseid = str(uuid.uuid4())
        created_date = datetime.now()
        updated_date = datetime.now()
        description = "a description"
        content_created_date = datetime.now()
        content_updated_date = datetime.now()

        conn.execute(
            """
            INSERT INTO index_record(did, baseid, rev, form, size, created_date, updated_date, content_created_date, content_updated_date, description) VALUES (?,?,?,?,?,?,?,?,?,?)
        """,
            (
                did,
                baseid,
                rev,
                form,
                size,
                created_date,
                updated_date,
                content_created_date,
                content_updated_date,
                description,
            ),
        )

        conn.commit()

        record = driver.get(did)

        assert record["did"] == did, "record id does not match"
        assert record["baseid"] == baseid, "record id does not match"
        assert record["rev"] == rev, "record revision does not match"
        assert record["size"] == size, "record size does not match"
        assert record["form"] == form, "record form does not match"
        assert (
            record["created_date"] == created_date.isoformat()
        ), "created date does not match"
        assert (
            record["updated_date"] == updated_date.isoformat()
        ), "updated date does not match"


@util.removes("index.sq3")
def test_driver_get_fails_with_no_records():
    """
    Tests retrieval of a record fails if there are no records.
    """
    driver = SQLAlchemyIndexDriver("sqlite:///index.sq3")

    with pytest.raises(NoRecordFound):
        driver.get("some_record_that_does_not_exist")


@util.removes("index.sq3")
def test_driver_nonstrict_get_without_prefix():
    """
    Tests retrieval of a record when a default prefix is set, but no prefix is supplied by the request.
    """
    with sqlite3.connect("index.sq3") as conn:
        driver = SQLAlchemyIndexDriver(
            "sqlite:///index.sq3",
            index_config={
                "DEFAULT_PREFIX": "testprefix/",
                "PREPEND_PREFIX": True,
                "ADD_PREFIX_ALIAS": False,
            },
        )

        did = str(uuid.uuid4())
        baseid = str(uuid.uuid4())
        rev = str(uuid.uuid4())[:8]
        size = 512
        form = "object"
        baseid = str(uuid.uuid4())
        created_date = datetime.now()
        updated_date = datetime.now()
        content_created_date = datetime.now()
        content_updated_date = datetime.now()
        description = "a description"
        conn.execute(
            """
            INSERT INTO index_record(did, baseid, rev, form, size, created_date, updated_date, content_created_date, content_updated_date, description) VALUES (?,?,?,?,?,?,?,?,?,?)
        """,
            (
                "testprefix/" + did,
                baseid,
                rev,
                form,
                size,
                created_date,
                updated_date,
                content_created_date,
                content_updated_date,
                description,
            ),
        )

        conn.commit()

        record = driver.get_with_nonstrict_prefix(did)

        assert record["did"] == "testprefix/" + did, "record id does not match"
        assert record["baseid"] == baseid, "record baseid does not match"
        assert record["rev"] == rev, "record revision does not match"
        assert record["size"] == size, "record size does not match"
        assert record["form"] == form, "record form does not match"
        assert (
            record["created_date"] == created_date.isoformat()
        ), "created date does not match"
        assert (
            record["updated_date"] == updated_date.isoformat()
        ), "updated date does not match"


@util.removes("index.sq3")
def test_driver_nonstrict_get_with_prefix():
    """
    Tests retrieval of a record when a default prefix is set and supplied by the request,
    but records are stored without prefixes.
    """
    with sqlite3.connect("index.sq3") as conn:
        driver = SQLAlchemyIndexDriver(
            "sqlite:///index.sq3",
            index_config={
                "DEFAULT_PREFIX": "testprefix/",
                "PREPEND_PREFIX": False,
                "ADD_PREFIX_ALIAS": True,
            },
        )

        did = str(uuid.uuid4())
        baseid = str(uuid.uuid4())
        rev = str(uuid.uuid4())[:8]
        size = 512
        form = "object"
        baseid = str(uuid.uuid4())
        created_date = datetime.now()
        updated_date = datetime.now()
        description = "a description"
        content_created_date = datetime.now()
        content_updated_date = datetime.now()
        conn.execute(
            """
            INSERT INTO index_record(did, baseid, rev, form, size, created_date, updated_date, content_created_date, content_updated_date, description) VALUES (?,?,?,?,?,?,?,?,?,?)
        """,
            (
                did,
                baseid,
                rev,
                form,
                size,
                created_date,
                updated_date,
                content_created_date,
                content_updated_date,
                description,
            ),
        )

        conn.commit()

        record = driver.get_with_nonstrict_prefix("testprefix/" + did)

        assert record["did"] == did, "record id does not match"
        assert record["baseid"] == baseid, "record baseid does not match"
        assert record["rev"] == rev, "record revision does not match"
        assert record["size"] == size, "record size does not match"
        assert record["form"] == form, "record form does not match"
        assert (
            record["created_date"] == created_date.isoformat()
        ), "created date does not match"
        assert (
            record["updated_date"] == updated_date.isoformat()
        ), "updated date does not match"


@util.removes("index.sq3")
def test_driver_nonstrict_get_with_incorrect_prefix():
    """
    Tests retrieval of a record fails if default prefix is set and request uses a different prefix with same uuid
    """
    with sqlite3.connect("index.sq3") as conn:
        driver = SQLAlchemyIndexDriver(
            "sqlite:///index.sq3",
            index_config={
                "DEFAULT_PREFIX": "testprefix/",
                "PREPEND_PREFIX": True,
                "ADD_PREFIX_ALIAS": False,
            },
        )

        did = str(uuid.uuid4())
        baseid = str(uuid.uuid4())
        rev = str(uuid.uuid4())[:8]
        size = 512
        form = "object"
        baseid = str(uuid.uuid4())
        created_date = datetime.now()
        updated_date = datetime.now()

        conn.execute(
            """
            INSERT INTO index_record(did, baseid, rev, form, size, created_date, updated_date) VALUES (?,?,?,?,?,?,?)
        """,
            ("testprefix/" + did, baseid, rev, form, size, created_date, updated_date),
        )

        conn.commit()

        with pytest.raises(NoRecordFound):
            driver.get_with_nonstrict_prefix("wrongprefix/" + did)


@util.removes("index.sq3")
def test_driver_nonstrict_get_with_no_default_prefix():
    """
    Tests retrieval of a record fails as expected if no default prefix is set
    """
    driver = SQLAlchemyIndexDriver(
        "sqlite:///index.sq3",
        index_config={
            "DEFAULT_PREFIX": None,
            "PREPEND_PREFIX": False,
            "ADD_PREFIX_ALIAS": False,
        },
    )

    with pytest.raises(NoRecordFound):
        driver.get_with_nonstrict_prefix("fake_id_without_prefix")


@util.removes("index.sq3")
def test_driver_get_latest_version():
    """
    Tests retrieval of the lattest record version
    """
    with sqlite3.connect("index.sq3") as conn:
        driver = SQLAlchemyIndexDriver("sqlite:///index.sq3")
        baseid = str(uuid.uuid4())

        for _ in range(10):
            did = str(uuid.uuid4())
            rev = str(uuid.uuid4())[:8]
            size = 512
            form = "object"
            baseid = str(uuid.uuid4())
            created_date = datetime.now()
            updated_date = datetime.now()
            description = "a description"
            content_created_date = datetime.now()
            content_updated_date = datetime.now()
            conn.execute(
                """
                INSERT INTO index_record(did, baseid, rev, form, size, created_date, updated_date, content_created_date, content_updated_date, description) VALUES (?,?,?,?,?,?,?,?,?,?)
            """,
                (
                    did,
                    baseid,
                    rev,
                    form,
                    size,
                    created_date,
                    updated_date,
                    content_created_date,
                    content_updated_date,
                    description,
                ),
            )

            conn.commit()

        record = driver.get_latest_version(did)

        assert record["did"] == did, "record id does not match"
        assert record["rev"] == rev, "record revision does not match"
        assert record["size"] == size, "record size does not match"
        assert record["form"] == form, "record form does not match"
        assert (
            record["created_date"] == created_date.isoformat()
        ), "created date does not match"
        assert (
            record["updated_date"] == updated_date.isoformat()
        ), "updated date does not match"


@util.removes("index.sq3")
def test_driver_get_latest_version_with_no_record():
    """
    Tests retrieval of the lattest record version
    """
    with sqlite3.connect("index.sq3") as conn:
        driver = SQLAlchemyIndexDriver("sqlite:///index.sq3")

        for _ in range(10):
            did = str(uuid.uuid4())
            rev = str(uuid.uuid4())[:8]
            size = 512
            form = "object"
            baseid = str(uuid.uuid4())
            dt = datetime.now()

            conn.execute(
                """
                INSERT INTO index_record(did, baseid, rev, form, size, created_date, updated_date) VALUES (?,?,?,?,?,?,?)
            """,
                (did, baseid, rev, form, size, dt, dt),
            )

            conn.commit()

        with pytest.raises(NoRecordFound):
            driver.get_latest_version("some base version")


@util.removes("index.sq3")
def test_driver_get_all_versions():
    """
    Tests retrieval of the lattest record version
    """
    with sqlite3.connect("index.sq3") as conn:
        driver = SQLAlchemyIndexDriver("sqlite:///index.sq3")
        baseid = str(uuid.uuid4())

        NUMBER_OF_RECORD = 3

        dids = []
        revs = []
        created_dates = []
        updated_dates = []
        content_created_dates = []
        content_updated_dates = []
        descriptions = []
        for _ in range(NUMBER_OF_RECORD):
            did = str(uuid.uuid4())
            rev = str(uuid.uuid4())[:8]
            size = 512
            form = "object"
            created_date = datetime.now()
            updated_date = created_date
            content_created_date = datetime.now()
            content_updated_date = created_date
            description = f"description for {did}"
            dids.append(did)
            revs.append(rev)
            created_dates.append(created_date)
            updated_dates.append(updated_date)
            content_created_dates.append(content_created_date)
            descriptions.append(description)
            conn.execute(
                """
                INSERT INTO index_record(did, baseid, rev, form, size, created_date, updated_date, content_created_date, content_updated_date, description) \
                    VALUES (?,?,?,?,?,?,?,?,?,?)
            """,
                (
                    did,
                    baseid,
                    rev,
                    form,
                    size,
                    created_date,
                    updated_date,
                    content_created_date,
                    content_updated_date,
                    description,
                ),
            )

        conn.commit()

        records = driver.get_all_versions(did)
        assert len(records) == NUMBER_OF_RECORD, "the number of records does not match"

        # make sure records are returned in creation date order
        for i, record in records.items():
            assert record["did"] == dids[i], "record id does not match"
            assert record["rev"] == revs[i], "record revision does not match"
            assert record["size"] == size, "record size does not match"
            assert record["form"] == form, "record form does not match"
            assert (
                record["created_date"] == created_dates[i].isoformat()
            ), "created date does not match"
            assert (
                record["updated_date"] == updated_dates[i].isoformat()
            ), "updated date does not match"


@util.removes("index.sq3")
def test_driver_get_all_versions_with_no_record():
    """
    Tests retrieval of the lattest record version
    """
    with sqlite3.connect("index.sq3") as conn:
        driver = SQLAlchemyIndexDriver("sqlite:///index.sq3")
        baseid = str(uuid.uuid4())

        for _ in range(3):
            did = str(uuid.uuid4())
            rev = str(uuid.uuid4())[:8]
            size = 512
            form = "object"

            conn.execute(
                """
                INSERT INTO index_record(did, baseid, rev, form, size) VALUES (?,?,?,?,?)
            """,
                (did, baseid, rev, form, size),
            )

        conn.commit()

        with pytest.raises(NoRecordFound):
            driver.get_all_versions("some baseid")


@util.removes("index.sq3")
def test_driver_get_fails_with_invalid_id():
    """
    Tests retrieval of a record fails if the record id is not found.
    """
    with sqlite3.connect("index.sq3") as conn:
        driver = SQLAlchemyIndexDriver("sqlite:///index.sq3")

        did = str(uuid.uuid4())
        baseid = str(uuid.uuid4())
        rev = str(uuid.uuid4())[:8]
        form = "object"

        conn.execute(
            """
            INSERT INTO index_record(did, baseid, rev, form, size) VALUES (?,?,?,?,?)
        """,
            (did, baseid, rev, form, None),
        )

        conn.commit()

        with pytest.raises(NoRecordFound):
            driver.get("some_record_that_does_not_exist")


def test_driver_update_record(skip_authz):
    _test_driver_update_record()


@util.removes("index.sq3")
def _test_driver_update_record():
    """
    Tests updating of a record.
    """
    with sqlite3.connect("index.sq3") as conn:
        driver = SQLAlchemyIndexDriver("sqlite:///index.sq3")

        did = str(uuid.uuid4())
        baseid = str(uuid.uuid4())
        rev = str(uuid.uuid4())[:8]
        form = "object"

        conn.execute(
            """
            INSERT INTO index_record(did, baseid, rev, form, size) VALUES (?,?,?,?,?)
        """,
            (did, baseid, rev, form, None),
        )

        conn.commit()

        # update_size = 256
        update_urls = ["a", "b", "c"]
        # update_hashes = {"a": "1", "b": "2", "c": "3"}

        file_name = "test"
        version = "ver_123"

        changing_fields = {
            "urls": update_urls,
            "file_name": file_name,
            "version": version,
        }

        driver.update(did, rev, changing_fields)

        new_did, new_rev, new_file_name, new_version = conn.execute(
            """
            SELECT did, rev, file_name, version FROM index_record
        """
        ).fetchone()

        new_urls = sorted(
            url[0]
            for url in conn.execute(
                """
            SELECT url FROM index_record_url
        """
            )
        )

        # new_hashes = {
        #     h: v
        #     for h, v in conn.execute(
        #         """
        #     SELECT hash_type, hash_value FROM index_record_hash
        # """
        #     )
        # }

        assert did == new_did, "record id does not match"
        assert rev != new_rev, "record revision matches prior"
        assert update_urls == new_urls, "record urls mismatch"
        assert file_name == new_file_name, "file_name does not match"
        assert version == new_version, "version does not match"


@util.removes("index.sq3")
def test_driver_update_fails_with_no_records():
    """
    Tests updating a record fails if there are no records.
    """
    driver = SQLAlchemyIndexDriver("sqlite:///index.sq3")

    with pytest.raises(NoRecordFound):
        driver.update(
            "some_record_that_does_not_exist", "some_base_version", "some_revision"
        )


@util.removes("index.sq3")
def test_driver_update_fails_with_invalid_id():
    """
    Tests updating a record fails if the record id is not found.
    """
    with sqlite3.connect("index.sq3") as conn:
        driver = SQLAlchemyIndexDriver("sqlite:///index.sq3")

        did = str(uuid.uuid4())
        baseid = str(uuid.uuid4())
        rev = str(uuid.uuid4())[:8]
        form = "object"

        conn.execute(
            """
            INSERT INTO index_record(did, baseid, rev, form, size) VALUES (?,?,?,?,?)
        """,
            (did, baseid, rev, form, None),
        )

        conn.commit()

        with pytest.raises(NoRecordFound):
            driver.update("some_record_that_does_not_exist", "some_record_version", rev)


@util.removes("index.sq3")
def test_driver_update_fails_with_invalid_rev():
    """
    Tests updating a record fails if the record rev is not invalid.
    """
    with sqlite3.connect("index.sq3") as conn:
        driver = SQLAlchemyIndexDriver("sqlite:///index.sq3")

        did = str(uuid.uuid4())
        baseid = str(uuid.uuid4())
        rev = str(uuid.uuid4())[:8]
        form = "object"

        conn.execute(
            """
            INSERT INTO index_record(did, baseid, rev, form, size) VALUES (?,?,?,?,?)
        """,
            (did, baseid, rev, form, None),
        )

        conn.commit()

        with pytest.raises(RevisionMismatch):
            driver.update(did, baseid, "some_revision")


def test_driver_delete_record(skip_authz):
    _test_driver_delete_record()


@util.removes("index.sq3")
def _test_driver_delete_record():
    """
    Tests deletion of a record.
    """
    with sqlite3.connect("index.sq3") as conn:
        driver = SQLAlchemyIndexDriver("sqlite:///index.sq3")

        did = str(uuid.uuid4())
        baseid = str(uuid.uuid4())
        rev = str(uuid.uuid4())[:8]
        form = "object"

        conn.execute(
            """
            INSERT INTO index_record(did, baseid, rev, form, size) VALUES (?,?,?,?,?)
        """,
            (did, baseid, rev, form, None),
        )

        conn.commit()

        driver.delete(did, rev)

        count = conn.execute(
            """
            SELECT COUNT(*) FROM index_record
        """
        ).fetchone()[0]

        assert count == 0, "records remain after deletion"


@util.removes("index.sq3")
def test_driver_delete_fails_with_no_records():
    """
    Tests deletion of a record fails if there are no records.
    """
    driver = SQLAlchemyIndexDriver("sqlite:///index.sq3")

    with pytest.raises(NoRecordFound):
        driver.delete("some_record_that_does_not_exist", "some_revision")


@util.removes("index.sq3")
def test_driver_delete_fails_with_invalid_id():
    """
    Tests deletion of a record fails if the record id is not found.
    """
    with sqlite3.connect("index.sq3") as conn:
        driver = SQLAlchemyIndexDriver("sqlite:///index.sq3")

        did = str(uuid.uuid4())
        baseid = str(uuid.uuid4())
        rev = str(uuid.uuid4())[:8]
        form = "object"

        conn.execute(
            """
            INSERT INTO index_record(did, baseid, rev, form, size) VALUES (?,?,?,?,?)
        """,
            (did, baseid, rev, form, None),
        )

        conn.commit()

        with pytest.raises(NoRecordFound):
            driver.delete("some_record_that_does_not_exist", rev)


@util.removes("index.sq3")
def test_driver_delete_fails_with_invalid_rev():
    """
    Tests deletion of a record fails if the record rev is not invalid.
    """
    with sqlite3.connect("index.sq3") as conn:
        driver = SQLAlchemyIndexDriver("sqlite:///index.sq3")

        did = str(uuid.uuid4())
        baseid = str(uuid.uuid4())
        rev = str(uuid.uuid4())[:8]
        form = "object"

        conn.execute(
            """
            INSERT INTO index_record(did, baseid, rev, form, size) VALUES (?,?,?,?,?)
        """,
            (did, baseid, rev, form, None),
        )

        conn.commit()

        with pytest.raises(RevisionMismatch):
            driver.delete(did, "some_revision")


@util.removes("index.sq3")
def test_driver_get_bundle():
    """
    Tests retrieval of a record.
    """
    with sqlite3.connect("index.sq3") as conn:
        driver = SQLAlchemyIndexDriver("sqlite:///index.sq3")

        bundle_id = str(uuid.uuid4())
        checksum = "iuhd91h9ufh928jidsoajh9du328"
        size = 512
        name = "object"
        created_time = updated_time = datetime.now()
        bundle_data = "{'bundle_data': [{'access_methods': [{'access_id': 's3', 'access_url': {'url': 's3://endpointurl/bucket/key'}, 'region': '', 'type': 's3'}], 'aliases': [], 'checksums': [{'checksum': '8b9942cf415384b27cadf1f4d2d682e5', 'type': 'md5'}], 'contents': [], 'created_time': '2020-04-23T21:42:36.506404', 'description': '', 'id': 'testprefix:7e677693-9da3-455a-b51c-03467d5498b0', 'mime_type': 'application/json', 'name': None, 'self_uri': 'drs://fictitious-commons.io/testprefix:7e677693-9da3-455a-b51c-03467d5498b0', 'size': 123, 'updated_time': '2020-04-23T21:42:36.506410', 'version': '3c995667'}], 'bundle_id': '1ff381ef-55c7-42b9-b33f-81ac0689d131', 'checksum': '65b464c1aea98176ef2fa38e8b6b9fc7', 'created_time': '2020-04-23T21:42:36.564808', 'name': 'test_bundle', 'size': 123, 'updated_time': '2020-04-23T21:42:36.564819'}"
        conn.execute(
            """
            INSERT INTO drs_bundle_record(bundle_id, name, checksum, size, bundle_data, created_time, updated_time) VALUES (?,?,?,?,?,?,?)
        """,
            (bundle_id, name, checksum, size, bundle_data, created_time, updated_time),
        )

        conn.commit()

        record = driver.get_bundle(bundle_id)

        assert record["id"] == bundle_id, "record id does not match"
        assert record["checksum"] == checksum, "record revision does not match"
        assert record["size"] == size, "record size does not match"
        assert record["name"] == name, "record name does not match"
        assert (
            record["created_time"] == created_time.isoformat()
        ), "created date does not match"
        assert (
            record["updated_time"] == updated_time.isoformat()
        ), "created date does not match"
