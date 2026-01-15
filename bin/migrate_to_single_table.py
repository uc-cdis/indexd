"""
to run: python migrate_to_single_table.py --creds-path /dir/containing/db_creds --start-did <guid>
"""
import argparse
import backoff
import json
import indexd.config_helper as config_helper
from cdislogging import get_logger
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker
import re

from indexd.index.drivers.alchemy import (
    IndexRecord,
    IndexRecordAuthz,
    IndexRecordAlias,
    IndexRecordUrl,
    IndexRecordACE,
    IndexRecordMetadata,
    IndexRecordUrlMetadata,
    IndexRecordHash,
)
from indexd.index.drivers.single_table_alchemy import Record
from indexd.index.errors import MultipleRecordsFound

APP_NAME = "indexd"

logger = get_logger("migrate_single_table", log_level="debug")


def load_json(file_name):
    return config_helper.load_json(file_name, APP_NAME)


def main():
    args = parse_args()
    migrator = IndexRecordMigrator(
        creds_file=args.creds_file, batch_size=args.batch_size
    )
    migrator.index_record_to_new_table(
        offset=args.start_offset, last_seen_guid=args.start_did
    )
    return


def parse_args():
    parser = argparse.ArgumentParser(
        description="Migrate data from old indexd database to new single table database"
    )
    parser.add_argument(
        "--creds-file",
        dest="creds_file",
        help="file to the creds file for the database you're trying to copy data from multi-table to single records table. Defaults to original indexd database creds from the indexd block in the creds.json file.",
    )
    parser.add_argument(
        "--start-did",
        dest="start_did",
        help="did to start at",
        default=None,
    )
    parser.add_argument(
        "--start-offset",
        dest="start_offset",
        type=int,
        help="offset to start at",
        default=None,
    )
    parser.add_argument(
        "--batch-size",
        dest="batch_size",
        help="number of records to batch select from source table (default: 1000)",
        type=int,
        default=1000,
    )
    return parser.parse_args()


class IndexRecordMigrator:
    def __init__(self, creds_file=None, batch_size=None):
        self.logger = get_logger("migrate_single_table", log_level="debug")

        conf_data = load_json(creds_file) if creds_file else load_json("creds.json")

        usr = conf_data.get("db_username", "{{db_username}}")
        db = conf_data.get("db_database", "{{db_database}}")
        psw = conf_data.get("db_password", "{{db_password}}")
        pghost = conf_data.get("db_host", "{{db_host}}")
        pgport = 5432
        self.batch_size = batch_size

        try:
            engine = create_engine(
                f"postgresql+psycopg2://{usr}:{psw}@{pghost}:{pgport}/{db}"
            )
        except Exception as e:
            self.logger.error(f"Failed to connect to postgres: {e}")
        Base = declarative_base()
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)

        self.session = Session()

    def index_record_to_new_table(self, offset=None, last_seen_guid=None):
        """
        Collect records from index_record table, collect additional info from multiple tables and bulk insert to new record table.
        """
        try:
            self.total_records = self.session.query(IndexRecord).count()
            self.count = 0

            while True:
                if last_seen_guid is None:
                    records = (
                        self.session.query(IndexRecord)
                        .order_by(IndexRecord.did)
                        .limit(self.batch_size)
                        .all()
                    )
                elif offset is not None:
                    records = (
                        self.session.query(IndexRecord)
                        .order_by(IndexRecord.did)
                        .offset(offset - 1)
                        .limit(self.batch_size)
                        .all()
                    )
                else:
                    self.logger.info(f"Start guid set to: {last_seen_guid}")
                    records = (
                        self.session.query(IndexRecord)
                        .order_by(IndexRecord.did)
                        .filter(IndexRecord.did > last_seen_guid)
                        .limit(self.batch_size)
                        .all()
                    )

                if not records:
                    break

                try:
                    records_to_insert = self.get_info_from_mult_tables(records)
                    self.bulk_insert_records(records_to_insert)
                except Exception as e:
                    raise Exception(
                        f"Could not insert records with {e} at offset {offset} with the last seen guid {last_seen_guid}. Please re-run the job with the following --start-did {last_seen_guid}"
                    )

                last_seen_guid = records[-1].did

        except Exception as e:
            self.session.rollback()
            self.logger.error(
                f"Error in migration: {e}. Last seen guid: {last_seen_guid} at position: {self.count}."
            )
        finally:
            self.session.close()
            new_total_records = self.session.query(Record).count()
            self.logger.info(f"Number of records in old table: {self.total_records}")
            self.logger.info(f"Number of records in new table: {new_total_records}")
            if self.total_records == new_total_records:
                self.logger.info(
                    "Number of records in the new table matches the number of records in old table"
                )
            else:
                self.logger.info(
                    "Number of records in the new table DOES NOT MATCH the number of records in old table. Check logs to see if there are records that were not migrated"
                )
            self.logger.info("Finished migrating :D")

    @backoff.on_exception(
        backoff.expo, Exception, max_tries=5, max_time=60, jitter=backoff.full_jitter
    )
    def bulk_insert_records(self, records_to_insert):
        """
        bulk insert records into the new Record table
        Args:
            records_to_insert (list): List of Record objects
        """
        try:
            self.session.bulk_save_objects(records_to_insert)
            self.session.commit()
            self.count += len(records_to_insert)
            self.logger.info(
                f"Done processing {self.count}/{self.total_records} records. {(self.count * 100)/self.total_records}%"
            )
        except IntegrityError as e:
            self.session.rollback()
            self.logger.error(f"Duplicate record found for records {e}")
        except Exception as e:
            self.session.rollback()
            self.logger.error(f"Error bulk insert for records at {self.count} records")

    def get_info_from_mult_tables(self, records):
        """
        Collect records from multiple tables from old multi table infrastructure and create a list of records to insert into the new single table infrastructure

        Args:
            records (list): list of IndexRecord objects

        Returns:
            records_to_insert (list): List of Record objects
        """
        records_to_insert = []
        for record in records:
            hashes = self.get_index_record_hash(record.did)
            urls = self.get_urls_record(record.did)
            url_metadata = self.get_urls_metadata(record.did)
            acl = self.get_index_record_ace(record.did)
            authz = self.get_index_record_authz(record.did)
            alias = self.get_index_record_alias(record.did)
            metadata = self.get_index_record_metadata(record.did)
            records_to_insert.append(
                Record(
                    guid=record.did,
                    baseid=record.baseid,
                    rev=record.rev,
                    form=record.form,
                    size=record.size,
                    created_date=record.created_date,
                    updated_date=record.updated_date,
                    content_created_date=record.content_created_date,
                    content_updated_date=record.content_updated_date,
                    file_name=record.file_name,
                    version=record.version,
                    uploader=record.uploader,
                    hashes=hashes,
                    urls=urls,
                    url_metadata=url_metadata,
                    acl=acl,
                    authz=authz,
                    alias=alias,
                    record_metadata=metadata,
                )
            )
        return records_to_insert

    @backoff.on_exception(
        backoff.expo, Exception, max_tries=5, max_time=10, jitter=backoff.full_jitter
    )
    def get_index_record_hash(self, did):
        """
        Get the index record hash for the given did and return correctly formatted value
        """
        try:
            stmt = (
                self.session.query(
                    IndexRecordHash.hash_type,
                    IndexRecordHash.hash_value,
                )
                .filter(IndexRecordHash.did == did)
                .all()
            )
            res = {hash_type: hash_value for hash_type, hash_value in stmt}
            return res
        except Exception as e:
            raise Exception(f"Error with hash for {did}: {e}")

    @backoff.on_exception(
        backoff.expo, Exception, max_tries=5, max_time=10, jitter=backoff.full_jitter
    )
    def get_urls_record(self, did):
        """
        Get the urls record for the given did and return correctly formatted value
        """
        try:
            stmt = (
                self.session.query(IndexRecordUrl.url)
                .filter(IndexRecordUrl.did == did)
                .all()
            )
            res = [u.url for u in stmt]
            return res
        except Exception as e:
            raise Exception(f"Error with urls for {did}: {e}")

    @backoff.on_exception(
        backoff.expo, Exception, max_tries=5, max_time=10, jitter=backoff.full_jitter
    )
    def get_urls_metadata(self, did):
        """
        Get the urls metadata for the given did and return correctly formatted value
        """
        try:
            stmt = (
                self.session.query(
                    IndexRecordUrlMetadata.url,
                    IndexRecordUrlMetadata.key,
                    IndexRecordUrlMetadata.value,
                )
                .filter(IndexRecordUrlMetadata.did == did)
                .all()
            )
            res = {url: {key: value} for url, key, value in stmt}
            return res
        except Exception as e:
            raise Exception(f"Error with url metadata for {did}: {e}")

    @backoff.on_exception(
        backoff.expo, Exception, max_tries=5, max_time=10, jitter=backoff.full_jitter
    )
    def get_index_record_ace(self, did):
        """
        Get the index record ace for the given did and return correctly formatted value
        """
        try:
            stmt = (
                self.session.query(IndexRecordACE.ace)
                .filter(IndexRecordACE.did == did)
                .all()
            )
            res = [a.ace for a in stmt]
            return res
        except Exception as e:
            raise Exception(f"Error with ace for did {did}: {e}")

    @backoff.on_exception(
        backoff.expo, Exception, max_tries=5, max_time=10, jitter=backoff.full_jitter
    )
    def get_index_record_authz(self, did):
        """
        Get the index record authz for the given did and return the correctly formatted value
        """
        try:
            stmt = (
                self.session.query(IndexRecordAuthz.resource)
                .filter(IndexRecordAuthz.did == did)
                .all()
            )
            res = [r.resource for r in stmt]
            return res
        except Exception as e:
            raise Exception(f"Error with authz: {e}")

    @backoff.on_exception(
        backoff.expo, Exception, max_tries=5, max_time=10, jitter=backoff.full_jitter
    )
    def get_index_record_alias(self, did):
        """
        Get the index record alias for the given did and return the correctly formatted
        """
        try:
            stmt = (
                self.session.query(IndexRecordAlias.name)
                .filter(IndexRecordAlias.did == did)
                .all()
            )
            res = {}
            for did, name in stmt:
                if did not in res:
                    res[did] = []
                res[did].append(name)
            return res
        except Exception as e:
            raise Exception(f"Error with alias: {e}")

    @backoff.on_exception(
        backoff.expo, Exception, max_tries=5, max_time=10, jitter=backoff.full_jitter
    )
    def get_index_record_metadata(self, did):
        """
        Get the index record metadata for the given did and return the correctly fortmatted value
        """
        try:
            stmt = (
                self.session.query(
                    IndexRecordMetadata.key,
                    IndexRecordMetadata.value,
                )
                .filter(IndexRecordMetadata.did == did)
                .all()
            )
            res = {key: value for key, value in stmt}
            return res
        except Exception as e:
            raise Exception(f"Error with alias for did {did}: {e}")


if __name__ == "__main__":
    main()
