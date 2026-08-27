import uuid
import pytest
from datetime import datetime

from sqlalchemy import text, select
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

import tests.util as util

from indexd.index.errors import NoRecordFound
from indexd.index.errors import RevisionMismatch
from indexd.index.errors import MultipleRecordsFound

from indexd.index.drivers.alchemy import SQLAlchemyIndexDriver, IndexRecord

POSTGRES_CONNECTION = "postgresql+asyncpg://postgres:postgres@localhost:5432/indexd_tests"  # pragma: allowlist secret


@pytest.mark.asyncio
async def test_driver_init_does_not_create_records():
    """
    Tests for creation of records after driver init.
    Tests driver init does not have unexpected side-effects.
    """
    engine = create_async_engine(POSTGRES_CONNECTION, poolclass=NullPool)

    async with engine.begin() as conn:
        result = await conn.execute(text("SELECT COUNT(*) FROM index_record"))
        count = result.scalar()

        assert count == 0, "driver created records upon initialization"

    await engine.dispose()


@pytest.mark.asyncio
async def test_driver_init_does_not_create_record_urls():
    """
    Tests for creation of urls after driver init.
    Tests driver init does not have unexpected side-effects.
    """
    engine = create_async_engine(POSTGRES_CONNECTION, poolclass=NullPool)

    async with engine.begin() as conn:
        result = await conn.execute(text("SELECT COUNT(*) FROM index_record_url"))
        count = result.scalar()

        assert count == 0, "driver created records urls upon initilization"

    await engine.dispose()


@pytest.mark.asyncio
async def test_driver_init_does_not_create_record_hashes():
    """
    Tests for creation of hashes after driver init.
    Tests driver init does not have unexpected side-effects.
    """
    engine = create_async_engine(POSTGRES_CONNECTION, poolclass=NullPool)

    async with engine.begin() as conn:
        result = await conn.execute(text("SELECT COUNT(*) FROM index_record_hash"))
        count = result.scalar()

        assert count == 0, "driver created records hashes upon initilization"

    await engine.dispose()


@pytest.mark.asyncio
async def test_driver_add_object_record():
    """
    Tests creation of a record.
    """
    engine = create_async_engine(POSTGRES_CONNECTION, poolclass=NullPool)
    driver = SQLAlchemyIndexDriver(POSTGRES_CONNECTION, poolclass=NullPool)

    async with engine.begin() as conn:
        await driver.add("object")

        result = await conn.execute(text("SELECT COUNT(*) FROM index_record"))
        count = result.scalar()

        assert count == 1, "driver did not create record"

        record = (await conn.execute(text("SELECT * FROM index_record"))).fetchone()

        assert record[0], "record id not populated"
        assert record[1], "record baseid not populated"
        assert record[2], "record rev not populated"
        assert record[3] == "object", "record form is not object"
        assert record[4] is None, "record size non-null"

    await engine.dispose()


@pytest.mark.asyncio
async def test_driver_add_bundle_record():
    """
    Tests creation of a record.
    """
    engine = create_async_engine(POSTGRES_CONNECTION, poolclass=NullPool)
    driver = SQLAlchemyIndexDriver(POSTGRES_CONNECTION, poolclass=NullPool)

    async with engine.begin() as conn:
        await driver.add_blank_bundle()

        result = await conn.execute(text("SELECT COUNT(*) FROM drs_bundle_record"))
        count = result.scalar()

        assert count == 1, "driver did not create record"

        result = (
            await conn.execute(text("SELECT * FROM drs_bundle_record"))
        ).fetchone()

        assert result != None
        assert len(result) == 10

    await engine.dispose()


@pytest.mark.asyncio
async def test_driver_add_container_record():
    """
    Tests creation of a record.
    """
    engine = create_async_engine(POSTGRES_CONNECTION, poolclass=NullPool)
    driver = SQLAlchemyIndexDriver(POSTGRES_CONNECTION, poolclass=NullPool)

    async with engine.begin() as conn:
        await driver.add("container")

        count = (await conn.execute(text("SELECT COUNT(*) FROM index_record"))).scalar()

        assert count == 1, "driver did not create record"

        record = (await conn.execute(text("SELECT * FROM index_record"))).fetchone()

        assert record[0], "record id not populated"
        assert record[1], "record baseid not populated"
        assert record[2], "record rev not populated"
        assert record[3] == "container", "record form is not container"
        assert record[4] == None, "record size non-null"

    await engine.dispose()


@pytest.mark.asyncio
async def test_driver_add_bundles_record():
    """
    Tests creation of a record.
    """
    engine = create_async_engine(POSTGRES_CONNECTION, poolclass=NullPool)
    driver = SQLAlchemyIndexDriver(POSTGRES_CONNECTION, poolclass=NullPool)

    async with engine.begin() as conn:
        await driver.add_bundle(name="bundle")

        count = (
            await conn.execute(text("SELECT COUNT(*) FROM drs_bundle_record"))
        ).scalar()

        assert count == 1, "driver did not create record"

        record = (
            await conn.execute(text("SELECT * FROM drs_bundle_record"))
        ).fetchone()

        assert record[0], "record id not populated"
        assert record[1], "record name not populated"
        assert record[1] == "bundle", "record name is not bundle"
        assert record[2], "record created date not populated"
        assert record[3], "record updated date not populated"

    await engine.dispose()


@pytest.mark.asyncio
async def test_driver_add_multipart_record():
    """
    Tests creation of a record.
    """
    engine = create_async_engine(POSTGRES_CONNECTION, poolclass=NullPool)
    driver = SQLAlchemyIndexDriver(POSTGRES_CONNECTION, poolclass=NullPool)

    async with engine.begin() as conn:
        await driver.add("multipart")

        count = (await conn.execute(text("SELECT COUNT(*) FROM index_record"))).scalar()

        assert count == 1, "driver did not create record"

        record = (await conn.execute(text("SELECT * FROM index_record"))).fetchone()

        assert record[0], "record id not populated"
        assert record[1], "record baseid not populated"
        assert record[2], "record rev not populated"
        assert record[3] == "multipart", "record form is not multipart"
        assert record[4] == None, "record size non-null"

    await engine.dispose()


@pytest.mark.asyncio
async def test_driver_add_with_valid_did():
    """
    Tests creation of a record with given valid did.
    """
    driver = SQLAlchemyIndexDriver(POSTGRES_CONNECTION, poolclass=NullPool)

    form = "object"
    did = "3d313755-cbb4-4b08-899d-7bbac1f6e67d"

    await driver.add(form, did=did)

    async with driver.session as s:
        result = await s.execute(select(IndexRecord))
        record = result.scalars().first()
        assert record.did == did


@pytest.mark.asyncio
async def test_driver_add_with_duplicate_did():
    """
    Tests creation of a record with duplicate did.
    """
    driver = SQLAlchemyIndexDriver(POSTGRES_CONNECTION, poolclass=NullPool)

    form = "object"
    did = "3d313755-cbb4-4b08-899d-7bbac1f6e67d"

    await driver.add(form, did=did)

    with pytest.raises(MultipleRecordsFound):
        await driver.add(form, did=did)


@pytest.mark.asyncio
async def test_driver_add_multiple_records():
    """
    Tests creation of a record.
    """
    engine = create_async_engine(POSTGRES_CONNECTION, poolclass=NullPool)
    driver = SQLAlchemyIndexDriver(POSTGRES_CONNECTION, poolclass=NullPool)

    async with engine.begin() as conn:
        await driver.add("object")
        await driver.add("object")
        await driver.add("object")

        count = (await conn.execute(text("SELECT COUNT(*) FROM index_record"))).scalar()

        assert count == 3, "driver did not create record(s)"

        result = await conn.execute(text("SELECT * FROM index_record"))

        for record in result.fetchall():
            assert record[0], "record id not populated"
            assert record[1], "record baseid not populated"
            assert record[2], "record rev not populated"
            assert record[3] == "object", "record form is not object"
            assert record[4] == None, "record size non-null"

    await engine.dispose()


@pytest.mark.asyncio
async def test_driver_add_with_size():
    """
    Tests creation of a record with size.
    """
    engine = create_async_engine(POSTGRES_CONNECTION, poolclass=NullPool)
    driver = SQLAlchemyIndexDriver(POSTGRES_CONNECTION, poolclass=NullPool)

    async with engine.begin() as conn:
        form = "object"
        size = 512

        await driver.add(form, size=size)

        count = (await conn.execute(text("SELECT COUNT(*) FROM index_record"))).scalar()

        assert count == 1, "driver did not create record"

        new_form, new_size = (
            await conn.execute(text("SELECT form, size FROM index_record"))
        ).fetchone()

        assert form == new_form, "record form mismatch"
        assert size == new_size, "record size mismatch"

    await engine.dispose()


@pytest.mark.asyncio
async def test_driver_add_with_urls():
    """
    Tests creation of a record with urls.
    """
    engine = create_async_engine(POSTGRES_CONNECTION, poolclass=NullPool)
    driver = SQLAlchemyIndexDriver(POSTGRES_CONNECTION, poolclass=NullPool)

    async with engine.begin() as conn:
        form = "object"
        urls = ["a", "b", "c"]

        await driver.add(form, urls=urls)

        count = (await conn.execute(text("SELECT COUNT(*) FROM index_record"))).scalar()

        assert count == 1, "driver did not create record"

        count = (
            await conn.execute(text("SELECT COUNT(*) FROM index_record_url"))
        ).scalar()

        assert count == 3, "driver did not create url(s)"

        result = await conn.execute(text("SELECT url FROM index_record_url"))
        new_urls = sorted(url[0] for url in result.fetchall())

        assert urls == new_urls, "record urls mismatch"

    await engine.dispose()


@pytest.mark.asyncio
async def test_driver_add_with_filename():
    """
    Tests creation of a record with filename.
    """
    driver = SQLAlchemyIndexDriver(POSTGRES_CONNECTION, poolclass=NullPool)

    form = "object"
    file_name = "abc"

    await driver.add(form, file_name=file_name)
    async with driver.session as s:
        result = await s.execute(select(IndexRecord))
        assert result.scalars().first().file_name == "abc"


@pytest.mark.asyncio
async def test_driver_add_with_version():
    """
    Tests creation of a record with version string.
    """
    driver = SQLAlchemyIndexDriver(POSTGRES_CONNECTION, poolclass=NullPool)

    form = "object"
    version = "ver_123"

    await driver.add(form, version=version)
    async with driver.session as s:
        result = await s.execute(select(IndexRecord))
        assert result.scalars().first().version == "ver_123"


@pytest.mark.asyncio
async def test_driver_add_with_hashes():
    """
    Tests creation of a record with hashes.
    """
    engine = create_async_engine(POSTGRES_CONNECTION, poolclass=NullPool)
    driver = SQLAlchemyIndexDriver(POSTGRES_CONNECTION, poolclass=NullPool)

    async with engine.begin() as conn:
        form = "object"
        hashes = {"a": "1", "b": "2", "c": "3"}

        await driver.add(form, hashes=hashes)

        count = (await conn.execute(text("SELECT COUNT(*) FROM index_record"))).scalar()
        assert count == 1, "driver did not create record"

        count = (
            await conn.execute(text("SELECT COUNT(*) FROM index_record_hash"))
        ).scalar()
        assert count == 3, "driver did not create hash(es)"

        result = await conn.execute(
            text("SELECT hash_type, hash_value FROM index_record_hash")
        )
        new_hashes = {h: v for h, v in result.fetchall()}

        assert hashes == new_hashes, "record hashes mismatch"

    await engine.dispose()


@pytest.mark.asyncio
async def test_driver_get_record():
    """
    Tests retrieval of a record.
    """
    engine = create_async_engine(POSTGRES_CONNECTION, poolclass=NullPool)
    driver = SQLAlchemyIndexDriver(POSTGRES_CONNECTION, poolclass=NullPool)

    async with engine.begin() as conn:
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

        await conn.execute(
            text("INSERT INTO base_version(baseid) VALUES ('{}')".format(baseid))
        )

        await conn.execute(
            text(
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
        )

    record = await driver.get(did)

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

    await engine.dispose()


@pytest.mark.asyncio
async def test_driver_get_fails_with_no_records():
    """
    Tests retrieval of a record fails if there are no records.
    """
    driver = SQLAlchemyIndexDriver(POSTGRES_CONNECTION, poolclass=NullPool)

    with pytest.raises(NoRecordFound):
        await driver.get("some_record_that_does_not_exist")


@pytest.mark.asyncio
async def test_driver_nonstrict_get_without_prefix():
    """
    Tests retrieval of a record when a default prefix is set, but no prefix is supplied by the request.
    """
    engine = create_async_engine(POSTGRES_CONNECTION, poolclass=NullPool)
    driver = SQLAlchemyIndexDriver(
        POSTGRES_CONNECTION,
        index_config={
            "DEFAULT_PREFIX": "testprefix/",
            "PREPEND_PREFIX": True,
            "ADD_PREFIX_ALIAS": False,
        },
        poolclass=NullPool,
    )

    async with engine.begin() as conn:
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

        await conn.execute(
            text("INSERT INTO base_version(baseid) VALUES ('{}')".format(baseid))
        )

        await conn.execute(
            text(
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
                )
            )
        )

    record = await driver.get_with_nonstrict_prefix(did)

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

    await engine.dispose()


@pytest.mark.asyncio
async def test_driver_nonstrict_get_with_prefix():
    """
    Tests retrieval of a record when a default prefix is set and supplied by the request,
    but records are stored without prefixes.
    """
    engine = create_async_engine(POSTGRES_CONNECTION, poolclass=NullPool)
    driver = SQLAlchemyIndexDriver(
        POSTGRES_CONNECTION,
        index_config={
            "DEFAULT_PREFIX": "testprefix/",
            "PREPEND_PREFIX": False,
            "ADD_PREFIX_ALIAS": True,
        },
        poolclass=NullPool,
    )

    async with engine.begin() as conn:
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

        await conn.execute(
            text("INSERT INTO base_version(baseid) VALUES ('{}')".format(baseid))
        )

        await conn.execute(
            text(
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
                )
            )
        )

    record = await driver.get_with_nonstrict_prefix("testprefix/" + did)

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

    await engine.dispose()


@pytest.mark.asyncio
async def test_driver_nonstrict_get_with_incorrect_prefix():
    """
    Tests retrieval of a record fails if default prefix is set and request uses a different prefix with same uuid
    """
    engine = create_async_engine(POSTGRES_CONNECTION, poolclass=NullPool)
    driver = SQLAlchemyIndexDriver(
        POSTGRES_CONNECTION,
        index_config={
            "DEFAULT_PREFIX": "testprefix/",
            "PREPEND_PREFIX": True,
            "ADD_PREFIX_ALIAS": False,
        },
        poolclass=NullPool,
    )

    async with engine.begin() as conn:
        did = str(uuid.uuid4())
        baseid = str(uuid.uuid4())
        rev = str(uuid.uuid4())[:8]
        size = 512
        form = "object"
        baseid = str(uuid.uuid4())
        created_date = datetime.now()
        updated_date = datetime.now()

        await conn.execute(
            text("INSERT INTO base_version(baseid) VALUES ('{}')".format(baseid))
        )

        await conn.execute(
            text(
                """
                INSERT INTO index_record(did, baseid, rev, form, size, created_date, updated_date) VALUES ('{}','{}','{}','{}','{}','{}','{}')
            """.format(
                    "testprefix/" + did,
                    baseid,
                    rev,
                    form,
                    size,
                    created_date,
                    updated_date,
                )
            )
        )

    with pytest.raises(NoRecordFound):
        await driver.get_with_nonstrict_prefix("wrongprefix/" + did)

    await engine.dispose()


@pytest.mark.asyncio
async def test_driver_nonstrict_get_with_no_default_prefix():
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
        poolclass=NullPool,
    )

    with pytest.raises(NoRecordFound):
        await driver.get_with_nonstrict_prefix("fake_id_without_prefix")


@pytest.mark.asyncio
async def test_driver_get_latest_version():
    """
    Tests retrieval of the lattest record version
    """
    engine = create_async_engine(POSTGRES_CONNECTION, poolclass=NullPool)
    driver = SQLAlchemyIndexDriver(POSTGRES_CONNECTION, poolclass=NullPool)

    async with engine.begin() as conn:
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

            await conn.execute(
                text("INSERT INTO base_version(baseid) VALUES ('{}')".format(baseid))
            )

            await conn.execute(
                text(
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
                    )
                )
            )

    record = await driver.get_latest_version(did)

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

    await engine.dispose()


@pytest.mark.asyncio
async def test_driver_get_latest_version_with_no_record():
    """
    Tests retrieval of the lattest record version
    """
    engine = create_async_engine(POSTGRES_CONNECTION, poolclass=NullPool)
    driver = SQLAlchemyIndexDriver(POSTGRES_CONNECTION, poolclass=NullPool)

    async with engine.begin() as conn:
        for _ in range(10):
            did = str(uuid.uuid4())
            rev = str(uuid.uuid4())[:8]
            size = 512
            form = "object"
            baseid = str(uuid.uuid4())
            dt = datetime.now()

            await conn.execute(
                text("INSERT INTO base_version(baseid) VALUES ('{}')".format(baseid))
            )

            await conn.execute(
                text(
                    """
                    INSERT INTO index_record(did, baseid, rev, form, size, created_date, updated_date) VALUES ('{}','{}','{}','{}','{}','{}','{}')
                """.format(
                        did, baseid, rev, form, size, dt, dt
                    )
                )
            )

    with pytest.raises(NoRecordFound):
        await driver.get_latest_version("some base version")

    await engine.dispose()


@pytest.mark.asyncio
async def test_driver_get_all_versions():
    """
    Tests retrieval of the lattest record version
    """
    engine = create_async_engine(POSTGRES_CONNECTION, poolclass=NullPool)
    driver = SQLAlchemyIndexDriver(POSTGRES_CONNECTION, poolclass=NullPool)

    async with engine.begin() as conn:
        baseid = str(uuid.uuid4())

        NUMBER_OF_RECORD = 3

        dids = []
        revs = []
        created_dates = []
        updated_dates = []
        content_created_dates = []
        descriptions = []

        await conn.execute(
            text("INSERT INTO base_version(baseid) VALUES ('{}')".format(baseid))
        )

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

            await conn.execute(
                text(
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
                    )
                )
            )

    records = await driver.get_all_versions(did)
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

    await engine.dispose()


@pytest.mark.asyncio
async def test_driver_get_all_versions_with_no_record():
    """
    Tests retrieval of the lattest record version
    """
    engine = create_async_engine(POSTGRES_CONNECTION, poolclass=NullPool)
    driver = SQLAlchemyIndexDriver(POSTGRES_CONNECTION, poolclass=NullPool)

    async with engine.begin() as conn:
        baseid = str(uuid.uuid4())

        await conn.execute(
            text("INSERT INTO base_version(baseid) VALUES ('{}')".format(baseid))
        )

        for _ in range(3):
            did = str(uuid.uuid4())
            rev = str(uuid.uuid4())[:8]
            size = 512
            form = "object"

            await conn.execute(
                text(
                    """
                    INSERT INTO index_record(did, baseid, rev, form, size) VALUES ('{}','{}','{}','{}','{}')
                """.format(
                        did, baseid, rev, form, size
                    )
                )
            )

    with pytest.raises(NoRecordFound):
        await driver.get_all_versions("some baseid")

    await engine.dispose()


@pytest.mark.asyncio
async def test_driver_get_fails_with_invalid_id():
    """
    Tests retrieval of a record fails if the record id is not found.
    """
    engine = create_async_engine(POSTGRES_CONNECTION, poolclass=NullPool)
    driver = SQLAlchemyIndexDriver(POSTGRES_CONNECTION, poolclass=NullPool)

    async with engine.begin() as conn:
        did = str(uuid.uuid4())
        baseid = str(uuid.uuid4())
        rev = str(uuid.uuid4())[:8]
        form = "object"
        size = 512

        await conn.execute(
            text("INSERT INTO base_version(baseid) VALUES ('{}')".format(baseid))
        )

        await conn.execute(
            text(
                """
                INSERT INTO index_record(did, baseid, rev, form, size) VALUES ('{}','{}','{}','{}','{}')
            """.format(
                    did, baseid, rev, form, size
                )
            )
        )

    with pytest.raises(NoRecordFound):
        await driver.get("some_record_that_does_not_exist")

    await engine.dispose()


@pytest.mark.asyncio
async def test_driver_update_record(skip_authz):
    await _test_driver_update_record()


async def _test_driver_update_record():
    """
    Tests updating of a record.
    """
    engine = create_async_engine(POSTGRES_CONNECTION, poolclass=NullPool)
    driver = SQLAlchemyIndexDriver(POSTGRES_CONNECTION, poolclass=NullPool)

    async with engine.begin() as conn:
        did = str(uuid.uuid4())
        baseid = str(uuid.uuid4())
        rev = str(uuid.uuid4())[:8]
        form = "object"
        size = 512

        await conn.execute(
            text("INSERT INTO base_version(baseid) VALUES ('{}')".format(baseid))
        )

        await conn.execute(
            text(
                """
                INSERT INTO index_record(did, baseid, rev, form, size) VALUES ('{}','{}','{}','{}','{}')
                """.format(
                    did, baseid, rev, form, size
                )
            )
        )

    update_urls = ["a", "b", "c"]
    file_name = "test"
    version = "ver_123"

    changing_fields = {
        "urls": update_urls,
        "file_name": file_name,
        "version": version,
    }

    await driver.update(None, did, rev, changing_fields)

    async with engine.connect() as conn:
        result = await conn.execute(
            text(
                """
                SELECT did, rev, file_name, version FROM index_record
                """
            )
        )
        new_did, new_rev, new_file_name, new_version = result.fetchone()

        result_urls = await conn.execute(
            text(
                """
                SELECT url FROM index_record_url
                """
            )
        )
        new_urls = sorted(url[0] for url in result_urls.fetchall())

    assert did == new_did, "record id does not match"
    assert rev != new_rev, "record revision matches prior"
    assert update_urls == new_urls, "record urls mismatch"
    assert file_name == new_file_name, "file_name does not match"
    assert version == new_version, "version does not match"

    await engine.dispose()


@pytest.mark.asyncio
async def test_driver_update_fails_with_no_records():
    """
    Tests updating a record fails if there are no records.
    """
    driver = SQLAlchemyIndexDriver(POSTGRES_CONNECTION, poolclass=NullPool)

    with pytest.raises(NoRecordFound):
        await driver.update(
            "some_request",
            "some_record_that_does_not_exist",
            "some_base_version",
            "some_revision",
        )


@pytest.mark.asyncio
async def test_driver_update_fails_with_invalid_id():
    """
    Tests updating a record fails if the record id is not found.
    """
    engine = create_async_engine(POSTGRES_CONNECTION, poolclass=NullPool)
    driver = SQLAlchemyIndexDriver(POSTGRES_CONNECTION, poolclass=NullPool)

    async with engine.begin() as conn:
        did = str(uuid.uuid4())
        baseid = str(uuid.uuid4())
        rev = str(uuid.uuid4())[:8]
        form = "object"
        size = 512

        await conn.execute(
            text("INSERT INTO base_version(baseid) VALUES ('{}')".format(baseid))
        )

        await conn.execute(
            text(
                """
                INSERT INTO index_record(did, baseid, rev, form, size) VALUES ('{}','{}','{}','{}','{}')
            """.format(
                    did, baseid, rev, form, size
                )
            )
        )

    with pytest.raises(NoRecordFound):
        await driver.update(
            None, "some_record_that_does_not_exist", "some_record_version", rev
        )

    await engine.dispose()


@pytest.mark.asyncio
async def test_driver_update_fails_with_invalid_rev():
    """
    Tests updating a record fails if the record rev is not invalid.
    """
    engine = create_async_engine(POSTGRES_CONNECTION, poolclass=NullPool)
    driver = SQLAlchemyIndexDriver(POSTGRES_CONNECTION, poolclass=NullPool)

    async with engine.begin() as conn:
        did = str(uuid.uuid4())
        baseid = str(uuid.uuid4())
        rev = str(uuid.uuid4())[:8]
        form = "object"
        size = 512

        await conn.execute(
            text("INSERT INTO base_version(baseid) VALUES ('{}')".format(baseid))
        )

        await conn.execute(
            text(
                """
                INSERT INTO index_record(did, baseid, rev, form, size) VALUES ('{}','{}','{}','{}','{}')
            """.format(
                    did, baseid, rev, form, size
                )
            )
        )

    with pytest.raises(RevisionMismatch):
        await driver.update(None, did, baseid, "some_revision")

    await engine.dispose()


@pytest.mark.asyncio
async def test_driver_delete_record(skip_authz):
    await _test_driver_delete_record()


async def _test_driver_delete_record():
    """
    Tests deletion of a record.
    """
    engine = create_async_engine(POSTGRES_CONNECTION, poolclass=NullPool)
    driver = SQLAlchemyIndexDriver(POSTGRES_CONNECTION, poolclass=NullPool)

    async with engine.begin() as conn:
        did = str(uuid.uuid4())
        baseid = str(uuid.uuid4())
        rev = str(uuid.uuid4())[:8]
        form = "object"
        size = 512

        await conn.execute(
            text("INSERT INTO base_version(baseid) VALUES ('{}')".format(baseid))
        )

        await conn.execute(
            text(
                """
                INSERT INTO index_record(did, baseid, rev, form, size) VALUES ('{}','{}','{}','{}','{}')
            """.format(
                    did, baseid, rev, form, size
                )
            )
        )

    await driver.delete(None, did, rev)

    async with engine.begin() as conn:
        count = (
            await conn.execute(
                text(
                    """
                SELECT COUNT(*) FROM index_record
            """
                )
            )
        ).scalar()

        assert count == 0, "records remain after deletion"

    await engine.dispose()


@pytest.mark.asyncio
async def test_driver_delete_fails_with_no_records():
    """
    Tests deletion of a record fails if there are no records.
    """
    driver = SQLAlchemyIndexDriver(POSTGRES_CONNECTION, poolclass=NullPool)

    with pytest.raises(NoRecordFound):
        await driver.delete(None, "some_record_that_does_not_exist", "some_revision")


@pytest.mark.asyncio
async def test_driver_delete_fails_with_invalid_id():
    """
    Tests deletion of a record fails if the record id is not found.
    """
    engine = create_async_engine(POSTGRES_CONNECTION, poolclass=NullPool)
    driver = SQLAlchemyIndexDriver(POSTGRES_CONNECTION, poolclass=NullPool)

    async with engine.begin() as conn:
        did = str(uuid.uuid4())
        baseid = str(uuid.uuid4())
        rev = str(uuid.uuid4())[:8]
        form = "object"
        size = 512

        await conn.execute(
            text("INSERT INTO base_version(baseid) VALUES ('{}')".format(baseid))
        )

        await conn.execute(
            text(
                """
                INSERT INTO index_record(did, baseid, rev, form, size) VALUES ('{}','{}','{}','{}','{}')
            """.format(
                    did, baseid, rev, form, size
                )
            )
        )

    with pytest.raises(NoRecordFound):
        await driver.delete(None, "some_record_that_does_not_exist", rev)

    await engine.dispose()


@pytest.mark.asyncio
async def test_driver_delete_fails_with_invalid_rev():
    """
    Tests deletion of a record fails if the record rev is not invalid.
    """
    engine = create_async_engine(POSTGRES_CONNECTION, poolclass=NullPool)
    driver = SQLAlchemyIndexDriver(POSTGRES_CONNECTION, poolclass=NullPool)

    async with engine.begin() as conn:
        did = str(uuid.uuid4())
        baseid = str(uuid.uuid4())
        rev = str(uuid.uuid4())[:8]
        form = "object"
        size = 512

        await conn.execute(
            text("INSERT INTO base_version(baseid) VALUES ('{}')".format(baseid))
        )

        await conn.execute(
            text(
                """
                INSERT INTO index_record(did, baseid, rev, form, size) VALUES ('{}','{}','{}','{}','{}')
            """.format(
                    did, baseid, rev, form, size
                )
            )
        )

    with pytest.raises(RevisionMismatch):
        await driver.delete(None, did, "some_revision")

    await engine.dispose()


@pytest.mark.asyncio
async def test_driver_get_bundle():
    """
    Tests retrieval of a record.
    """
    engine = create_async_engine(POSTGRES_CONNECTION, poolclass=NullPool)
    driver = SQLAlchemyIndexDriver(POSTGRES_CONNECTION, poolclass=NullPool)

    async with engine.begin() as conn:
        bundle_id = str(uuid.uuid4())
        checksum = "iuhd91h9ufh928jidsoajh9du328"
        size = 512
        name = "object"
        created_time = updated_time = datetime.now()
        bundle_data = '{"bundle_data": [{"access_methods": [{"access_id": "s3", "access_url": {"url": "s3://endpointurl/bucket/key"}, "region": "", "type": "s3"}], "aliases": [], "checksums": [{"checksum": "8b9942cf415384b27cadf1f4d2d682e5", "type": "md5"}], "contents": [], "created_time": "2020-04-23T21:42:36.506404", "description": "", "id": "testprefix/7e677693-9da3-455a-b51c-03467d5498b0", "mime_type": "application/json", "name": None, "self_uri": "drs://fictitious-commons.io/testprefix/7e677693-9da3-455a-b51c-03467d5498b0", "size": 123, "updated_time": "2020-04-23T21:42:36.506410", "version": "3c995667"}], "bundle_id": "1ff381ef-55c7-42b9-b33f-81ac0689d131", "checksum": "65b464c1aea98176ef2fa38e8b6b9fc7", "created_time": "2020-04-23T21:42:36.564808", "name": "test_bundle", "size": 123, "updated_time": "2020-04-23T21:42:36.564819"}'

        await conn.execute(
            text(
                """
                INSERT INTO drs_bundle_record(bundle_id, name, checksum, size, bundle_data, created_time, updated_time) VALUES ('{}','{}','{}','{}','{}','{}','{}')
            """.format(
                    bundle_id,
                    name,
                    checksum,
                    size,
                    bundle_data,
                    created_time,
                    updated_time,
                )
            )
        )

    record = await driver.get_bundle(bundle_id)

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

    await engine.dispose()
