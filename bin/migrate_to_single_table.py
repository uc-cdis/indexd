"""

"""
import json
import config_helper
from cdislogging import get_logger
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

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
    def __init__(self):
        self.logger = get_logger("migrate_single_table", log_level="debug")
        conf_data = load_json("creds.json")
        usr = conf_data.get("db_username", "{{db_username}}")
        db = conf_data.get("db_database", "{{db_database}}")
        psw = conf_data.get("db_password", "{{db_password}}")
        pghost = conf_data.get("db_host", "{{db_host}}")
        pgport = 5432
        index_config = conf_data.get("index_config")

        engine = create_engine(
            f"postgresql+psycopg2://{usr}:{psw}@{pghost}:{pgport}/{db}"
        )

        Base = declarative_base()
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)

        self.session = Session()

    def index_record_to_new_table(self, batch_size=1000):
        try:
            total_records = self.session.query(IndexRecord).count()

            for offset in range(0, total_records, batch_size):
                stmt = self.session.query(IndexRecord).offset(offset).limit(batch_size)

                records_to_insert = []

                for row in stmt:
                    hashes = self.get_index_record_hash(row.did)
                    urls = self.get_urls_record(row.did)
                    url_metadata = self.get_urls_metadata(row.did)
                    acl = self.get_index_record_ace(row.did)
                    authz = self.get_index_record_authz(row.did)
                    alias = self.get_index_record_alias(row.did)
                    metadata = self.get_index_record_metadata(row.did)

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

                self.session.bulk_save_objects(records_to_insert)

                self.session.commit()

                inserted = min(batch_size, total_records - offset)
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
            stmt = self.session.query(IndexRecordHash).filter(
                IndexRecordHash.did == did
            )
            res = {row.hash_type: row.hash_value for row in stmt}
            return res

        except Exception as e:
            self.logger.error(f"Error with hash for {did}: {e}")

    def get_urls_record(self, did):
        try:
            stmt = self.session.query(IndexRecordUrl).filter(IndexRecordUrl.did == did)
            res = [row.url for row in stmt]
            return res

        except Exception as e:
            self.logger.error(f"Error with urls for {did}: {e}")

    def get_urls_metadata(self, did):
        try:
            stmt = self.session.query(IndexRecordUrlMetadata).filter(
                IndexRecordUrlMetadata.did == did
            )
            res = {row.url: {row.key: row.value} for row in stmt}
            return res
        except Exception as e:
            self.logger.error(f"Error with url metadata for {did}: {e}")

    def get_index_record_ace(self, did):
        try:
            stmt = self.session.query(IndexRecordACE).filter(IndexRecordACE.did == did)
            res = [row.ace for row in stmt]
            return res
        except Exception as e:
            self.logger.error(f"Error with ace for {did}: {e}")

    def get_index_record_authz(self, did):
        try:
            stmt = self.session.query(IndexRecordAuthz).filter(
                IndexRecordAuthz.did == did
            )
            res = [row.resource for row in stmt]
            return res
        except Exception as e:
            self.logger.error(f"Error with authz for {did}: {e}")

    def get_index_record_alias(self, did):
        try:
            stmt = self.session.query(IndexRecordAlias).filter(
                IndexRecordAlias.did == did
            )
            res = [row.name for row in stmt]
            return res
        except Exception as e:
            self.logger.error(f"Error with alias for {did}: {e}")

    def get_index_record_metadata(self, did):
        try:
            stmt = self.session.query(IndexRecordMetadata).filter(
                IndexRecordMetadata.did == did
            )
            res = {row.key: row.value for row in stmt}
            return res
        except Exception as e:
            self.logger.error(f"Error with alias for {did}: {e}")


if __name__ == "__main__":
    main()
