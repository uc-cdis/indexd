import uuid

import pytest
from sqlalchemy import create_engine

import tests.util as util

from indexd.index.errors import NoRecordFound
from indexd.index.errors import RevisionMismatch

from indexd.index.errors import MultipleRecordsFound

from indexd.index.drivers.alchemy import SQLAlchemyIndexDriver, IndexRecord

from datetime import datetime


# TODO check if pytest has utilities for meta-programming of tests

POSTGRES_CONNECTION = "postgres://postgres:postgres@localhost:5432/indexd_tests"  # pragma: allowlist secret


def test_driver_init_does_not_create_records(
    combined_default_and_single_table_settings,
):
    """
    Tests for creation of records after driver init.
    Tests driver init does not have unexpected side-effects.
    """

    engine = create_engine(POSTGRES_CONNECTION)
    driver = SQLAlchemyIndexDriver(POSTGRES_CONNECTION)

    with engine.connect() as conn:
        result = conn.execute("SELECT COUNT(*) FROM index_record")
        count = result.scalar()

        assert count == 0, "driver created records upon initialization"


def test_driver_init_does_not_create_record_urls(
    combined_default_and_single_table_settings,
):
    """
    Tests for creation of urls after driver init.
    Tests driver init does not have unexpected side-effects.
    """

    engine = create_engine(POSTGRES_CONNECTION)
    driver = SQLAlchemyIndexDriver(POSTGRES_CONNECTION)

    with engine.connect() as conn:
        result = conn.execute("SELECT COUNT(*) FROM index_record_url")
        count = result.scalar()

        assert count == 0, "driver created records urls upon initilization"


def test_driver_init_does_not_create_record_hashes(
    combined_default_and_single_table_settings,
):
    """
    Tests for creation of hashes after driver init.
    Tests driver init does not have unexpected side-effects.
    """

    engine = create_engine(POSTGRES_CONNECTION)
    driver = SQLAlchemyIndexDriver(POSTGRES_CONNECTION)

    with engine.connect() as conn:
        result = conn.execute("SELECT COUNT(*) FROM index_record_hash")
        count = result.scalar()

        assert count == 0, "driver created records hashes upon initilization"


def test_driver_add_object_record(combined_default_and_single_table_settings):
    """
    Tests creation of a record.
    """

    engine = create_engine(POSTGRES_CONNECTION)
    driver = SQLAlchemyIndexDriver(POSTGRES_CONNECTION)

    with engine.connect() as conn:
        driver.add("object")

        result = conn.execute("SELECT COUNT(*) FROM index_record")
        count = result.scalar()

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


def test_driver_add_bundle_record(combined_default_and_single_table_settings):
    """
    Tests creation of a record.
    """

    engine = create_engine(POSTGRES_CONNECTION)
    driver = SQLAlchemyIndexDriver(POSTGRES_CONNECTION)

    with engine.connect() as conn:
        driver.add_blank_bundle()

        result = conn.execute("SELECT COUNT(*) FROM drs_bundle_record")
        count = result.scalar()

        assert count == 1, "driver did not create record"

        result = conn.execute("SELECT * FROM drs_bundle_record").fetchone()

        assert result != None
        assert len(result) == 10


def test_driver_add_container_record(combined_default_and_single_table_settings):
    """
    Tests creation of a record.
    """

    engine = create_engine(POSTGRES_CONNECTION)
    driver = SQLAlchemyIndexDriver(POSTGRES_CONNECTION)

    with engine.connect() as conn:
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


def test_driver_add_bundles_record(combined_default_and_single_table_settings):
    """
    Tests creation of a record.
    """

    engine = create_engine(POSTGRES_CONNECTION)
    driver = SQLAlchemyIndexDriver(POSTGRES_CONNECTION)
    with engine.connect() as conn:
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


def test_driver_add_multipart_record(combined_default_and_single_table_settings):
    """
    Tests creation of a record.
    """

    engine = create_engine(POSTGRES_CONNECTION)
    driver = SQLAlchemyIndexDriver(POSTGRES_CONNECTION)

    with engine.connect() as conn:
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


def test_driver_add_with_valid_did(combined_default_and_single_table_settings):
    """
    Tests creation of a record with given valid did.
    """
    driver = SQLAlchemyIndexDriver(POSTGRES_CONNECTION)

    form = "object"
    did = "3d313755-cbb4-4b08-899d-7bbac1f6e67d"
    driver.add(form, did=did)
    with driver.session as s:
        assert s.query(IndexRecord).first().did == did


def test_driver_add_with_duplicate_did(combined_default_and_single_table_settings):
    """
    Tests creation of a record with duplicate did.
    """
    driver = SQLAlchemyIndexDriver(POSTGRES_CONNECTION)

    form = "object"
    did = "3d313755-cbb4-4b08-899d-7bbac1f6e67d"
    driver.add(form, did=did)

    with pytest.raises(MultipleRecordsFound):
        driver.add(form, did=did)


def test_driver_add_multiple_records(combined_default_and_single_table_settings):
    """
    Tests creation of a record.
    """
    engine = create_engine(POSTGRES_CONNECTION)
    driver = SQLAlchemyIndexDriver(POSTGRES_CONNECTION)

    with engine.connect() as conn:
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


def test_driver_add_with_size(combined_default_and_single_table_settings):
    """
    Tests creation of a record with size.
    """
    engine = create_engine(POSTGRES_CONNECTION)
    driver = SQLAlchemyIndexDriver(POSTGRES_CONNECTION)

    with engine.connect() as conn:
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


def test_driver_add_with_urls(combined_default_and_single_table_settings):
    """
    Tests creation of a record with urls.
    """
    engine = create_engine(POSTGRES_CONNECTION)
    driver = SQLAlchemyIndexDriver(POSTGRES_CONNECTION)

    with engine.connect() as conn:
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


def test_driver_add_with_filename(combined_default_and_single_table_settings):
    """
    Tests creation of a record with filename.
    """
    driver = SQLAlchemyIndexDriver(POSTGRES_CONNECTION)

    form = "object"
    file_name = "abc"
    driver.add(form, file_name=file_name)
    with driver.session as s:
        assert s.query(IndexRecord).first().file_name == "abc"


def test_driver_add_with_version(combined_default_and_single_table_settings):
    """
    Tests creation of a record with version string.
    """
    driver = SQLAlchemyIndexDriver(POSTGRES_CONNECTION)

    form = "object"
    version = "ver_123"
    driver.add(form, version=version)
    with driver.session as s:
        assert s.query(IndexRecord).first().version == "ver_123"


def test_driver_add_with_hashes(combined_default_and_single_table_settings):
    """
    Tests creation of a record with hashes.
    """

    engine = create_engine(POSTGRES_CONNECTION)
    driver = SQLAlchemyIndexDriver(POSTGRES_CONNECTION)

    with engine.connect() as conn:
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


def test_driver_get_record(combined_default_and_single_table_settings):
    """
    Tests retrieval of a record.
    """

    engine = create_engine(POSTGRES_CONNECTION)
    driver = SQLAlchemyIndexDriver(POSTGRES_CONNECTION)

    with engine.connect() as conn:
        did = str(uuid.uuid4())
        baseid = str(uuid.uuid4())
        rev = str(uuid.uuid4())[:8]
        size = 512
        form = "object"
        created_date = datetime.now()
        updated_date = datetime.now()
        description = "a description"
        content_created_date = datetime.now()
        content_updated_date = datetime.now()

        conn.execute("INSERT INTO base_version(baseid) VALUES ('{}')".format(baseid))

        conn.execute(
            "INSERT INTO index_record(did, baseid, rev, form, size, created_date, updated_date, content_created_date, content_updated_date, description) VALUES ('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}')".format(
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
            )
        )

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


def test_driver_get_fails_with_no_records(combined_default_and_single_table_settings):
    """
    Tests retrieval of a record fails if there are no records.
    """
    driver = SQLAlchemyIndexDriver(POSTGRES_CONNECTION)

    with pytest.raises(NoRecordFound):
        driver.get("some_record_that_does_not_exist")


def test_driver_nonstrict_get_without_prefix(
    combined_default_and_single_table_settings,
):
    """
    Tests retrieval of a record when a default prefix is set, but no prefix is supplied by the request.
    """

    engine = create_engine(POSTGRES_CONNECTION)
    driver = SQLAlchemyIndexDriver(
        POSTGRES_CONNECTION,
        index_config={
            "DEFAULT_PREFIX": "testprefix/",
            "PREPEND_PREFIX": True,
            "ADD_PREFIX_ALIAS": False,
        },
    )

    with engine.connect() as conn:
        did = str(uuid.uuid4())
        baseid = str(uuid.uuid4())
        rev = str(uuid.uuid4())[:8]
        size = 512
        form = "object"
        created_date = datetime.now()
        updated_date = datetime.now()
        content_created_date = datetime.now()
        content_updated_date = datetime.now()
        description = "a description"

        conn.execute("INSERT INTO base_version(baseid) VALUES ('{}')".format(baseid))

        conn.execute(
            """
            INSERT INTO index_record(did, baseid, rev, form, size, created_date, updated_date, content_created_date, content_updated_date, description) VALUES ('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}')
        """.format(
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


def test_driver_nonstrict_get_with_prefix(combined_default_and_single_table_settings):
    """
    Tests retrieval of a record when a default prefix is set and supplied by the request,
    but records are stored without prefixes.
    """

    engine = create_engine(POSTGRES_CONNECTION)
    driver = SQLAlchemyIndexDriver(
        POSTGRES_CONNECTION,
        index_config={
            "DEFAULT_PREFIX": "testprefix/",
            "PREPEND_PREFIX": False,
            "ADD_PREFIX_ALIAS": True,
        },
    )
    with engine.connect() as conn:
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

        conn.execute("INSERT INTO base_version(baseid) VALUES ('{}')".format(baseid))

        conn.execute(
            """
            INSERT INTO index_record(did, baseid, rev, form, size, created_date, updated_date, content_created_date, content_updated_date, description) VALUES ('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}')
        """.format(
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


def test_driver_nonstrict_get_with_incorrect_prefix(
    combined_default_and_single_table_settings,
):
    """
    Tests retrieval of a record fails if default prefix is set and request uses a different prefix with same uuid
    """
    engine = create_engine(POSTGRES_CONNECTION)
    driver = SQLAlchemyIndexDriver(
        POSTGRES_CONNECTION,
        index_config={
            "DEFAULT_PREFIX": "testprefix/",
            "PREPEND_PREFIX": True,
            "ADD_PREFIX_ALIAS": False,
        },
    )
    with engine.connect() as conn:
        did = str(uuid.uuid4())
        baseid = str(uuid.uuid4())
        rev = str(uuid.uuid4())[:8]
        size = 512
        form = "object"
        baseid = str(uuid.uuid4())
        created_date = datetime.now()
        updated_date = datetime.now()

        conn.execute("INSERT INTO base_version(baseid) VALUES ('{}')".format(baseid))

        conn.execute(
            """
            INSERT INTO index_record(did, baseid, rev, form, size, created_date, updated_date) VALUES ('{}','{}','{}','{}','{}','{}','{}')
        """.format(
                "testprefix/" + did, baseid, rev, form, size, created_date, updated_date
            ),
        )

        with pytest.raises(NoRecordFound):
            driver.get_with_nonstrict_prefix("wrongprefix/" + did)


def test_driver_nonstrict_get_with_no_default_prefix(
    combined_default_and_single_table_settings,
):
    """
    Tests retrieval of a record fails as expected if no default prefix is set
    """
    driver = SQLAlchemyIndexDriver(
        POSTGRES_CONNECTION,
        index_config={
            "DEFAULT_PREFIX": None,
            "PREPEND_PREFIX": False,
            "ADD_PREFIX_ALIAS": False,
        },
    )

    with pytest.raises(NoRecordFound):
        driver.get_with_nonstrict_prefix("fake_id_without_prefix")


def test_driver_get_latest_version(combined_default_and_single_table_settings):
    """
    Tests retrieval of the lattest record version
    """
    engine = create_engine(POSTGRES_CONNECTION)
    driver = SQLAlchemyIndexDriver(POSTGRES_CONNECTION)

    with engine.connect() as conn:
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
                "INSERT INTO base_version(baseid) VALUES ('{}')".format(baseid)
            )

            conn.execute(
                """
                INSERT INTO index_record(did, baseid, rev, form, size, created_date, updated_date, content_created_date, content_updated_date, description) VALUES ('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}')
            """.format(
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


def test_driver_get_latest_version_with_no_record(
    combined_default_and_single_table_settings,
):
    """
    Tests retrieval of the lattest record version
    """
    engine = create_engine(POSTGRES_CONNECTION)
    driver = SQLAlchemyIndexDriver(POSTGRES_CONNECTION)

    with engine.connect() as conn:
        for _ in range(10):
            did = str(uuid.uuid4())
            rev = str(uuid.uuid4())[:8]
            size = 512
            form = "object"
            baseid = str(uuid.uuid4())
            dt = datetime.now()

            conn.execute(
                "INSERT INTO base_version(baseid) VALUES ('{}')".format(baseid)
            )

            conn.execute(
                """
                INSERT INTO index_record(did, baseid, rev, form, size, created_date, updated_date) VALUES ('{}','{}','{}','{}','{}','{}','{}')
            """.format(
                    did, baseid, rev, form, size, dt, dt
                ),
            )

        with pytest.raises(NoRecordFound):
            driver.get_latest_version("some base version")


def test_driver_get_all_versions(combined_default_and_single_table_settings):
    """
    Tests retrieval of the lattest record version
    """
    engine = create_engine(POSTGRES_CONNECTION)
    driver = SQLAlchemyIndexDriver(POSTGRES_CONNECTION)

    with engine.connect() as conn:
        baseid = str(uuid.uuid4())

        NUMBER_OF_RECORD = 3

        dids = []
        revs = []
        created_dates = []
        updated_dates = []
        content_created_dates = []
        content_updated_dates = []
        descriptions = []

        conn.execute("INSERT INTO base_version(baseid) VALUES ('{}')".format(baseid))

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
                    VALUES ('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}')
            """.format(
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


def test_driver_get_all_versions_with_no_record(
    combined_default_and_single_table_settings,
):
    """
    Tests retrieval of the lattest record version
    """
    engine = create_engine(POSTGRES_CONNECTION)
    driver = SQLAlchemyIndexDriver(POSTGRES_CONNECTION)

    with engine.connect() as conn:
        baseid = str(uuid.uuid4())

        conn.execute("INSERT INTO base_version(baseid) VALUES ('{}')".format(baseid))

        for _ in range(3):
            did = str(uuid.uuid4())
            rev = str(uuid.uuid4())[:8]
            size = 512
            form = "object"

            conn.execute(
                """
                INSERT INTO index_record(did, baseid, rev, form, size) VALUES ('{}','{}','{}','{}','{}')
            """.format(
                    did, baseid, rev, form, size
                ),
            )

        with pytest.raises(NoRecordFound):
            driver.get_all_versions("some baseid")


def test_driver_get_fails_with_invalid_id(combined_default_and_single_table_settings):
    """
    Tests retrieval of a record fails if the record id is not found.
    """
    engine = create_engine(POSTGRES_CONNECTION)
    driver = SQLAlchemyIndexDriver(POSTGRES_CONNECTION)

    with engine.connect() as conn:
        did = str(uuid.uuid4())
        baseid = str(uuid.uuid4())
        rev = str(uuid.uuid4())[:8]
        form = "object"
        size = 512

        conn.execute("INSERT INTO base_version(baseid) VALUES ('{}')".format(baseid))

        conn.execute(
            """
            INSERT INTO index_record(did, baseid, rev, form, size) VALUES ('{}','{}','{}','{}','{}')
        """.format(
                did, baseid, rev, form, size
            ),
        )

        with pytest.raises(NoRecordFound):
            driver.get("some_record_that_does_not_exist")


def test_driver_update_record(skip_authz, combined_default_and_single_table_settings):
    _test_driver_update_record()


def _test_driver_update_record():
    """
    Tests updating of a record.
    """
    engine = create_engine(POSTGRES_CONNECTION)
    driver = SQLAlchemyIndexDriver(POSTGRES_CONNECTION)

    with engine.connect() as conn:
        did = str(uuid.uuid4())
        baseid = str(uuid.uuid4())
        rev = str(uuid.uuid4())[:8]
        form = "object"
        size = 512

        conn.execute("INSERT INTO base_version(baseid) VALUES ('{}')".format(baseid))

        conn.execute(
            """
            INSERT INTO index_record(did, baseid, rev, form, size) VALUES ('{}','{}','{}','{}','{}')
        """.format(
                did, baseid, rev, form, size
            ),
        )

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


def test_driver_update_fails_with_no_records(
    combined_default_and_single_table_settings,
):
    """
    Tests updating a record fails if there are no records.
    """
    driver = SQLAlchemyIndexDriver(POSTGRES_CONNECTION)

    with pytest.raises(NoRecordFound):
        driver.update(
            "some_record_that_does_not_exist", "some_base_version", "some_revision"
        )


def test_driver_update_fails_with_invalid_id(
    combined_default_and_single_table_settings,
):
    """
    Tests updating a record fails if the record id is not found.
    """
    engine = create_engine(POSTGRES_CONNECTION)
    driver = SQLAlchemyIndexDriver(POSTGRES_CONNECTION)

    with engine.connect() as conn:
        did = str(uuid.uuid4())
        baseid = str(uuid.uuid4())
        rev = str(uuid.uuid4())[:8]
        form = "object"
        size = 512

        conn.execute("INSERT INTO base_version(baseid) VALUES ('{}')".format(baseid))

        conn.execute(
            """
            INSERT INTO index_record(did, baseid, rev, form, size) VALUES ('{}','{}','{}','{}','{}')
        """.format(
                did, baseid, rev, form, size
            ),
        )

        with pytest.raises(NoRecordFound):
            driver.update("some_record_that_does_not_exist", "some_record_version", rev)


def test_driver_update_fails_with_invalid_rev(
    combined_default_and_single_table_settings,
):
    """
    Tests updating a record fails if the record rev is not invalid.
    """
    engine = create_engine(POSTGRES_CONNECTION)
    driver = SQLAlchemyIndexDriver(POSTGRES_CONNECTION)

    with engine.connect() as conn:
        did = str(uuid.uuid4())
        baseid = str(uuid.uuid4())
        rev = str(uuid.uuid4())[:8]
        form = "object"
        size = 512

        conn.execute("INSERT INTO base_version(baseid) VALUES ('{}')".format(baseid))

        conn.execute(
            """
            INSERT INTO index_record(did, baseid, rev, form, size) VALUES ('{}','{}','{}','{}','{}')
        """.format(
                did, baseid, rev, form, size
            ),
        )

        with pytest.raises(RevisionMismatch):
            driver.update(did, baseid, "some_revision")


def test_driver_delete_record(skip_authz, combined_default_and_single_table_settings):
    _test_driver_delete_record()


def _test_driver_delete_record():
    """
    Tests deletion of a record.
    """
    engine = create_engine(POSTGRES_CONNECTION)
    driver = SQLAlchemyIndexDriver(POSTGRES_CONNECTION)

    with engine.connect() as conn:
        did = str(uuid.uuid4())
        baseid = str(uuid.uuid4())
        rev = str(uuid.uuid4())[:8]
        form = "object"
        size = 512

        conn.execute("INSERT INTO base_version(baseid) VALUES ('{}')".format(baseid))

        conn.execute(
            """
            INSERT INTO index_record(did, baseid, rev, form, size) VALUES ('{}','{}','{}','{}','{}')
        """.format(
                did, baseid, rev, form, size
            ),
        )

        driver.delete(did, rev)

        count = conn.execute(
            """
            SELECT COUNT(*) FROM index_record
        """
        ).fetchone()[0]

        assert count == 0, "records remain after deletion"


def test_driver_delete_fails_with_no_records(
    combined_default_and_single_table_settings,
):
    """
    Tests deletion of a record fails if there are no records.
    """
    driver = SQLAlchemyIndexDriver(POSTGRES_CONNECTION)

    with pytest.raises(NoRecordFound):
        driver.delete("some_record_that_does_not_exist", "some_revision")


def test_driver_delete_fails_with_invalid_id(
    combined_default_and_single_table_settings,
):
    """
    Tests deletion of a record fails if the record id is not found.
    """
    engine = create_engine(POSTGRES_CONNECTION)
    driver = SQLAlchemyIndexDriver(POSTGRES_CONNECTION)

    with engine.connect() as conn:
        did = str(uuid.uuid4())
        baseid = str(uuid.uuid4())
        rev = str(uuid.uuid4())[:8]
        form = "object"
        size = 512

        conn.execute("INSERT INTO base_version(baseid) VALUES ('{}')".format(baseid))

        conn.execute(
            """
            INSERT INTO index_record(did, baseid, rev, form, size) VALUES ('{}','{}','{}','{}','{}')
        """.format(
                did, baseid, rev, form, size
            ),
        )

        with pytest.raises(NoRecordFound):
            driver.delete("some_record_that_does_not_exist", rev)


def test_driver_delete_fails_with_invalid_rev(
    combined_default_and_single_table_settings,
):
    """
    Tests deletion of a record fails if the record rev is not invalid.
    """
    engine = create_engine(POSTGRES_CONNECTION)
    driver = SQLAlchemyIndexDriver(POSTGRES_CONNECTION)

    with engine.connect() as conn:
        did = str(uuid.uuid4())
        baseid = str(uuid.uuid4())
        rev = str(uuid.uuid4())[:8]
        form = "object"
        size = 512

        conn.execute("INSERT INTO base_version(baseid) VALUES ('{}')".format(baseid))

        conn.execute(
            """
            INSERT INTO index_record(did, baseid, rev, form, size) VALUES ('{}','{}','{}','{}','{}')
        """.format(
                did, baseid, rev, form, size
            ),
        )

        with pytest.raises(RevisionMismatch):
            driver.delete(did, "some_revision")


def test_driver_get_bundle(combined_default_and_single_table_settings):
    """
    Tests retrieval of a record.
    """
    engine = create_engine(POSTGRES_CONNECTION)
    driver = SQLAlchemyIndexDriver(POSTGRES_CONNECTION)

    with engine.connect() as conn:
        bundle_id = str(uuid.uuid4())
        checksum = "iuhd91h9ufh928jidsoajh9du328"
        size = 512
        name = "object"
        created_time = updated_time = datetime.now()
        bundle_data = '{"bundle_data": [{"access_methods": [{"access_id": "s3", "access_url": {"url": "s3://endpointurl/bucket/key"}, "region": "", "type": "s3"}], "aliases": [], "checksums": [{"checksum": "8b9942cf415384b27cadf1f4d2d682e5", "type": "md5"}], "contents": [], "created_time": "2020-04-23T21:42:36.506404", "description": "", "id": "testprefix:7e677693-9da3-455a-b51c-03467d5498b0", "mime_type": "application/json", "name": None, "self_uri": "drs://fictitious-commons.io/testprefix:7e677693-9da3-455a-b51c-03467d5498b0", "size": 123, "updated_time": "2020-04-23T21:42:36.506410", "version": "3c995667"}], "bundle_id": "1ff381ef-55c7-42b9-b33f-81ac0689d131", "checksum": "65b464c1aea98176ef2fa38e8b6b9fc7", "created_time": "2020-04-23T21:42:36.564808", "name": "test_bundle", "size": 123, "updated_time": "2020-04-23T21:42:36.564819"}'

        conn.execute(
            """
            INSERT INTO drs_bundle_record(bundle_id, name, checksum, size, bundle_data, created_time, updated_time) VALUES ('{}','{}','{}','{}','{}','{}','{}')
        """.format(
                bundle_id, name, checksum, size, bundle_data, created_time, updated_time
            ),
        )

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
