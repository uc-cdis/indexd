"""

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
    migrator = IndexRecordMigrator()
    migrator.index_record_to_new_table()
    return


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

            for offset in range(0, total_records, batch_size):
                stmt = (
                    self.session.query(IndexRecord)
                    .offset(offset)
                    .limit(batch_size)
                    .yield_per(batch_size)
                )

                records_to_insert = []

                for row in stmt:
                    hashes = self.get_index_record_hash(row.did)
                    urls = self.get_urls_record(row.did)
                    url_metadata = self.get_urls_metadata(row.did)
                    acl = self.get_index_record_ace(row.did)
                    authz = self.get_index_record_authz(row.did)
                    alias = self.get_index_record_alias(row.did)
                    metadata = self.get_index_record_metadata(row.did)

                    try:
                        records_to_insert.append(
                            Record(
                                guid=row.did,
                                baseid=row.baseid,
                                rev=row.rev,
                                form=row.form,
                                size=row.size,
                                created_date=row.created_date,
                                updated_date=row.updated_date,
                                content_created_date=row.content_created_date,
                                content_updated_date=row.content_updated_date,
                                file_name=row.file_name,
                                version=row.version,
                                uploader=row.uploader,
                                hashes=hashes,
                                urls=urls,
                                url_metadata=url_metadata,
                                acl=acl,
                                authz=authz,
                                alias=alias,
                                record_metadata=metadata,
                            )
                        )
                    except Exception as e:
                        print(e)

                while len(records_to_insert) > 0:
                    try:
                        self.session.bulk_save_objects(records_to_insert)
                        self.session.commit()
                        break
                    except Exception as e:
                        self.session.rollback()
                        if "duplicate key value violates unique constraint" in str(e):
                            self.logger.error(f"Errored at {offset}: {e}")
                            records_to_insert = self.remove_duplicate_records(
                                records_to_insert, e
                            )
                        else:
                            self.logger.error(f"Ran into error at {offset}: {e}")
                            break
                self.logger.info(
                    f"Inserted {offset} records out of {total_records}. Progress: {(offset*100)/total_records}%"
                )

        except Exception as e:
            self.session.rollback()
            self.logger.error(f"Errored at {offset}: {e}")

        finally:
            self.session.close()
            self.logger.info("Finished migrating :D")

    def get_index_record_hash(self, did):
        try:
            stmt = (
                self.session.query(
                    IndexRecordHash.hash_type, IndexRecordHash.hash_value
                )
                .filter(IndexRecordHash.did == did)
                .all()
            )
            res = {hash_type: hash_value for hash_type, hash_value in stmt}
            return res

        except Exception as e:
            self.logger.error(f"Error with hash for {did}: {e}")

    def get_urls_record(self, did):
        try:
            stmt = (
                self.session.query(IndexRecordUrl.url)
                .filter(IndexRecordUrl.did == did)
                .all()
            )
            res = [u.url for u in stmt]
            return res

        except Exception as e:
            self.logger.error(f"Error with urls for {did}: {e}")

    def get_urls_metadata(self, did):
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
            self.logger.error(f"Error with url metadata for {did}: {e}")

    def get_index_record_ace(self, did):
        try:
            stmt = (
                self.session.query(IndexRecordACE.ace)
                .filter(IndexRecordACE.did == did)
                .all()
            )
            res = [a.ace for a in stmt]
            return res
        except Exception as e:
            self.logger.error(f"Error with ace for {did}: {e}")

    def get_index_record_authz(self, did):
        try:
            stmt = (
                self.session.query(IndexRecordAuthz.resource)
                .filter(IndexRecordAuthz.did == did)
                .all()
            )
            res = [r.resource for r in stmt]
            return res
        except Exception as e:
            self.logger.error(f"Error with authz for {did}: {e}")

    def get_index_record_alias(self, did):
        try:
            stmt = (
                self.session.query(IndexRecordAlias.name)
                .filter(IndexRecordAlias.did == did)
                .all()
            )
            res = [row.name for row in stmt]
            return res
        except Exception as e:
            self.logger.error(f"Error with alias for {did}: {e}")

    def get_index_record_metadata(self, did):
        try:
            stmt = (
                self.session.query(IndexRecordMetadata)
                .filter(IndexRecordMetadata.did == did)
                .all()
            )
            res = {row.key: row.value for row in stmt}
            return res
        except Exception as e:
            self.logger.error(f"Error with alias for {did}: {e}")

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
    start_time = time.time()
    parser = argparse.ArgumentParser(
        description="Migrate data from old indexd database to new single table database"
    )
    parser.add_argument(
        "creds_path",
        help="Path to the creds file for the database you're trying to copy data from multi-table to single records table. Defaults to original indexd database creds from the indexd block in the creds.json file.",
    )
    args = parser.parse_args()
    migrator = IndexRecordMigrator(conf_data=args.creds_path)
    migrator.index_record_to_new_table()
    # cProfile.run("migrator.index_record_to_new_table()", filename="profile_results.txt")
    end_time = time.time()

    print("Total Time: {}".format(end_time - start_time))
