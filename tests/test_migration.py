import random
import uuid

from sqlalchemy import create_engine

from bin.migrate_to_single_table import IndexRecordMigrator
from indexd.index.drivers.alchemy import SQLAlchemyIndexDriver


POSTGRES_CONNECTION = "postgresql://postgres:postgres@localhost:5432/indexd_tests"  # pragma: allowlist secret


def create_record(n_records=1):
    """
    Create n_records number of records in multitable
    """

    engine = create_engine(POSTGRES_CONNECTION)
    driver = SQLAlchemyIndexDriver(POSTGRES_CONNECTION)
    did_list = []
    for _ in range(n_records):
        did = str(uuid.uuid4())
        baseid = str(uuid.uuid4())
        size = random.randint(0, 1024)
        file_name = f"file_{random.randint(0, 1024)}"
        index_metadata = {
            "metadata_key": "metadata_value",
            "some_other_key": "some_other_value",
        }
        hashes = {"md5": "some_md5", "sha1": "some_sha1"}
        urls = ["s3://bucket/data.json", "gs://bucket/data.txt"]
        urls_metadata = {
            "s3://bucket/data.json": {"metadata_key": "metadata_value"},
            "gs://bucket/data.txt": {"metadata_key": "metadata_value"},
        }
        version = str(uuid.uuid4())[:5]
        acl = random.choice(["*", "phs00001", "phs00002", "phs00003"])
        authz = random.choice(["/open", "phs00001", "phs00002"])
        rev = str(uuid.uuid4())[:8]
        uploader = "uploader"
        description = "this is a test file"

        driver.add(
            "object",
            did=did,
            size=size,
            file_name=file_name,
            metadata=index_metadata,
            urls_metadata=urls_metadata,
            version=version,
            urls=urls,
            acl=acl,
            authz=authz,
            hashes=hashes,
            baseid=baseid,
            uploader=uploader,
            description=description,
        )
        did_list.append(did)

    with engine.connect() as conn:
        result = conn.execute("SELECT COUNT(*) FROM index_record")
        count = result.scalar()
        assert count == n_records

    return did_list


def test_index_record_to_new_table():
    """
    Test index_record_to_new_table copies records from old tables to new record table.
    """
    index_record_migrator = IndexRecordMigrator(creds_file="tests/test_creds.json")
    n_records = 100
    create_record(n_records)
    index_record_migrator.index_record_to_new_table(batch_size=10)

    engine = create_engine(POSTGRES_CONNECTION)
    with engine.connect() as conn:
        result = conn.execute("SELECT COUNT(*) FROM record")
        count = result.scalar()
        assert count == n_records


def test_get_index_record_hash():
    """
    Test get_index_record_hash from IndexRecordMigrator returns the correct format
    """
    index_record_migrator = IndexRecordMigrator(creds_file="tests/test_creds.json")
    did = create_record()[0]
    result = index_record_migrator.get_index_record_hash(did)
    assert result == {"md5": "some_md5", "sha1": "some_sha1"}


def test_get_urls_record():
    """
    Test get_urls_record from IndexRecordMigrator returns the correct format
    """
    index_record_migrator = IndexRecordMigrator(creds_file="tests/test_creds.json")
    did = create_record()[0]
    result = index_record_migrator.get_urls_record(did)
    assert result == ["s3://bucket/data.json", "gs://bucket/data.txt"]


def test_get_urls_metadata():
    """
    Test get_urls_metadata from IndexRecordMigrator returns the correct format
    """
    index_record_migrator = IndexRecordMigrator(creds_file="tests/test_creds.json")
    did = create_record()[0]
    result = index_record_migrator.get_urls_metadata(did)
    assert result == {
        "s3://bucket/data.json": {"metadata_key": "metadata_value"},
        "gs://bucket/data.txt": {"metadata_key": "metadata_value"},
    }


def test_get_index_record_ace():
    """
    Test get_index_record_ace from IndexRecordMigrator returns the correct format
    """
    index_record_migrator = IndexRecordMigrator(creds_file="tests/test_creds.json")
    did = create_record()[0]
    result = index_record_migrator.get_index_record_ace(did)
    assert type(result) == list


def test_get_index_record_authz():
    """
    Test get_index_record_authz from IndexRecordMigrator returns the correct format
    """
    index_record_migrator = IndexRecordMigrator(creds_file="tests/test_creds.json")
    did = create_record()[0]
    result = index_record_migrator.get_index_record_authz(did)
    assert type(result) == list


def test_get_index_record_metadata():
    """
    Test get_index_record_metadata from IndexRecordMigrator returns the correct format
    """
    index_record_migrator = IndexRecordMigrator(creds_file="tests/test_creds.json")
    did = create_record()[0]
    result = index_record_migrator.get_index_record_metadata(did)
    assert result == {
        "metadata_key": "metadata_value",
        "some_other_key": "some_other_value",
    }
