"""
to run: python migrate_to_single_table.py "/dir/containing/db_creds"
"""
import argparse
import json
import config_helper
from cdislogging import get_logger
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import time
import random
import re

import cProfile

from indexd.index.drivers.alchemy import (
    IndexRecord,
    IndexRecordAuthz,
    BaseVersion,
    IndexRecordAlias,
    IndexRecordUrl,
    IndexRecordACE,
    IndexRecordMetadata,
    IndexRecordUrlMetadata,
    IndexRecordHash,
)
from indexd.index.drivers.single_table_alchemy import Record

APP_NAME = "indexd"

logger = get_logger("migrate_single_table", log_level="debug")


def load_json(file_name):
    return config_helper.load_json(file_name, APP_NAME)


def main():
    args = parse_args()
    migrator = IndexRecordMigrator(conf_data=args.creds_path)
    migrator.index_record_to_new_table()
    return


def parse_args():
    parser = argparse.ArgumentParser(
        description="Migrate data from old indexd database to new single table database"
    )
    parser.add_argument(
        "--creds-path",
        help="Path to the creds file for the database you're trying to copy data from multi-table to single records table. Defaults to original indexd database creds from the indexd block in the creds.json file.",
    )
    parser.add_argument(
        "--start-did",
        help="did to start at",
        default=False,
    )
    return parser.parse_args()


class IndexRecordMigrator:
    def __init__(self, conf_data=None):
        self.logger = get_logger("migrate_single_table", log_level="debug")

        if conf_data:
            with open(conf_data, "r") as reader:
                conf_data = json.load(reader)
        else:
            conf_data = load_json("creds.json")

        usr = conf_data.get("db_username", "{{db_username}}")
        db = conf_data.get("db_database", "{{db_database}}")
        psw = conf_data.get("db_password", "{{db_password}}")
        pghost = conf_data.get("db_host", "{{db_host}}")
        pgport = 5432

        engine = create_engine(
            f"postgresql+psycopg2://{usr}:{psw}@{pghost}:{pgport}/{db}"
        )

        Base = declarative_base()
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)

        self.session = Session()

    def index_record_to_new_table(self, batch_size=1000, retry_limit=4):
        try:
            total_records = self.session.query(IndexRecord).count()
            last_seen_guid = None
            count = 0

            while True:
                if last_seen_guid is None:
                    records = (
                        self.session.query(IndexRecord)
                        .order_by(IndexRecord.did)
                        .limit(batch_size)
                        .all()
                    )
                else:
                    records = (
                        self.session.query(IndexRecord)
                        .order_by(IndexRecord.did)
                        .filter(IndexRecord.did > last_seen_guid)
                        .limit(batch_size)
                        .all()
                    )

                if not records:
                    break

                # Collect all dids in the current batch
                dids = [record.did for record in records]
                # Fetch related data for all dids in the current batch
                hashes = self.get_index_record_hash(dids)
                urls = self.get_urls_record(dids)
                url_metadata = self.get_urls_metadata(dids)
                acl = self.get_index_record_ace(dids)
                authz = self.get_index_record_authz(dids)
                alias = self.get_index_record_alias(dids)
                metadata = self.get_index_record_metadata(dids)

                records_to_insert = []

                for record in records:
                    record_hashes = hashes.get(record.did, {})
                    record_urls = urls.get(record.did, [])
                    record_url_metadata = url_metadata.get(record.did, {})
                    record_acl = acl.get(record.did, [])
                    record_authz = authz.get(record.did, [])
                    record_alias = alias.get(record.did, [])
                    record_metadata = metadata.get(record.did, {})

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

                last_seen_guid = records[-1].did

                while records_to_insert:
                    try:
                        self.session.bulk_save_objects(records_to_insert)
                        self.session.commit()
                        count += len(records_to_insert)
                        self.logger.info(
                            f"Done processing {count}/{total_records} records. {(count * 100)/total_records}%"
                        )
                        break
                    except Exception as e:
                        self.session.rollback()
                        if "duplicate key value violates unique constraint" in str(e):
                            records_to_insert = self.remove_duplicate_records(
                                records_to_insert, e
                            )
        except Exception as e:
            self.session.rollback()
            self.logger.error(f"Error in migration: {e}")

        finally:
            self.session.close()
            self.logger.info("Finished migrating :D")

    def get_index_record_hash(self, dids):
        try:
            stmt = (
                self.session.query(
                    IndexRecordHash.did,
                    IndexRecordHash.hash_type,
                    IndexRecordHash.hash_value,
                )
                .filter(IndexRecordHash.did.in_(dids))
                .all()
            )
            res = {}
            for did, hash_type, hash_value in stmt:
                if did not in res:
                    res[did] = {}
                res[did][hash_type] = hash_value
            return res
        except Exception as e:
            self.logger.error(f"Error with hashes: {e}")

    def get_urls_record(self, dids):
        try:
            stmt = (
                self.session.query(IndexRecordUrl.did, IndexRecordUrl.url)
                .filter(IndexRecordUrl.did.in_(dids))
                .all()
            )
            res = {}
            for did, url in stmt:
                if did not in res:
                    res[did] = []
                res[did].append(url)
            return res
        except Exception as e:
            self.logger.error(f"Error with urls: {e}")

    def get_urls_metadata(self, dids):
        try:
            stmt = (
                self.session.query(
                    IndexRecordUrlMetadata.did,
                    IndexRecordUrlMetadata.url,
                    IndexRecordUrlMetadata.key,
                    IndexRecordUrlMetadata.value,
                )
                .filter(IndexRecordUrlMetadata.did.in_(dids))
                .all()
            )
            res = {}
            for did, url, key, value in stmt:
                if did not in res:
                    res[did] = {}
                if url not in res[did]:
                    res[did][url] = {}
                res[did][url][key] = value
            return res
        except Exception as e:
            self.logger.error(f"Error with url metadata: {e}")

    def get_index_record_ace(self, dids):
        try:
            stmt = (
                self.session.query(IndexRecordACE.did, IndexRecordACE.ace)
                .filter(IndexRecordACE.did.in_(dids))
                .all()
            )
            res = {}
            for did, ace in stmt:
                if did not in res:
                    res[did] = []
                res[did].append(ace)
            return res
        except Exception as e:
            self.logger.error(f"Error with ace: {e}")

    def get_index_record_authz(self, dids):
        try:
            stmt = (
                self.session.query(IndexRecordAuthz.did, IndexRecordAuthz.resource)
                .filter(IndexRecordAuthz.did.in_(dids))
                .all()
            )
            res = {}
            for did, resource in stmt:
                if did not in res:
                    res[did] = []
                res[did].append(resource)
            return res
        except Exception as e:
            self.logger.error(f"Error with authz: {e}")

    def get_index_record_alias(self, dids):
        try:
            stmt = (
                self.session.query(IndexRecordAlias.did, IndexRecordAlias.name)
                .filter(IndexRecordAlias.did.in_(dids))
                .all()
            )
            res = {}
            for did, name in stmt:
                if did not in res:
                    res[did] = []
                res[did].append(name)
            return res
        except Exception as e:
            self.logger.error(f"Error with alias: {e}")

    def get_index_record_metadata(self, dids):
        try:
            stmt = (
                self.session.query(
                    IndexRecordMetadata.did,
                    IndexRecordMetadata.key,
                    IndexRecordMetadata.value,
                )
                .filter(IndexRecordMetadata.did.in_(dids))
                .all()
            )
            res = {}
            for did, key, value in stmt:
                if did not in res:
                    res[did] = {}
                res[did][key] = value
            return res
        except Exception as e:
            self.logger.error(f"Error with alias: {e}")

    def remove_duplicate_records(self, records, error):
        # Extract the key value from the error message
        key_value = re.search(r"\(guid\)=\((.*?)\)", str(error))
        key_value = key_value.group(1)
        self.logger.info(f"Removing duplicate record {key_value}")
        for record in records:
            if key_value == str(record.guid):
                records.remove(record)
                break

        return records


if __name__ == "__main__":
    main()
