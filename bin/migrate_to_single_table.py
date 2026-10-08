"""
to run: python migrate_to_single_table.py --creds-path /dir/containing/db_creds --start-did <guid>
"""

import argparse
import asyncio
import backoff
import json
import indexd.config_helper as config_helper
from cdislogging import get_logger

from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import declarative_base

from indexd.index.drivers.alchemy import (
    Base as AlchemyBase,
    IndexRecord,
    IndexRecordAuthz,
    IndexRecordAlias,
    IndexRecordUrl,
    IndexRecordACE,
    IndexRecordMetadata,
    IndexRecordUrlMetadata,
    IndexRecordHash,
)
from indexd.index.drivers.single_table_alchemy import Record, Base as SingleTableBase

APP_NAME = "indexd"

logger = get_logger("migrate_single_table", log_level="debug")


def load_json(file_name):
    return config_helper.load_json(file_name, APP_NAME)


def main():
    args = parse_args()
    migrator = IndexRecordMigrator(
        creds_file=args.creds_file, batch_size=args.batch_size
    )

    # Run the async migration using asyncio
    asyncio.run(
        migrator.index_record_to_new_table(
            offset=args.start_offset, last_seen_guid=args.start_did
        )
    )


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

        # Save the URL but DO NOT create the engine yet (__init__ is synchronous)
        self.db_url = f"postgresql+asyncpg://{usr}:{psw}@{pghost}:{pgport}/{db}"

    async def index_record_to_new_table(self, offset=None, last_seen_guid=None):
        """
        Collect records from index_record table, collect additional info from multiple tables and bulk insert to new record table.
        """
        engine = create_async_engine(self.db_url)

        # Safely create tables using the actual imported bases
        async with engine.begin() as conn:
            await conn.run_sync(AlchemyBase.metadata.create_all)
            await conn.run_sync(SingleTableBase.metadata.create_all)

        async_session_factory = async_sessionmaker(engine, expire_on_commit=False)

        async with async_session_factory() as session:
            self.session = session
            try:
                # Async count
                self.total_records = await self.session.scalar(
                    select(func.count()).select_from(IndexRecord)
                )
                self.count = 0

                while True:
                    stmt = (
                        select(IndexRecord)
                        .order_by(IndexRecord.did)
                        .limit(self.batch_size)
                    )

                    if last_seen_guid is not None:
                        self.logger.info(f"Start guid set to: {last_seen_guid}")
                        stmt = stmt.filter(IndexRecord.did > last_seen_guid)
                    elif offset is not None:
                        stmt = stmt.offset(offset - 1)

                    result = await self.session.scalars(stmt)
                    records = result.all()

                    if not records:
                        break

                    try:
                        # We must await the info gathering now
                        records_to_insert = await self.get_info_from_mult_tables(
                            records
                        )
                        await self.bulk_insert_records(records_to_insert)
                    except Exception as e:
                        raise Exception(
                            f"Could not insert records with {e} at offset {offset} with the last seen guid {last_seen_guid}. Please re-run the job with the following --start-did {last_seen_guid}"
                        )

                    last_seen_guid = records[-1].did

            except Exception as e:
                await self.session.rollback()
                self.logger.error(
                    f"Error in migration: {e}. Last seen guid: {last_seen_guid} at position: {self.count}."
                )
            finally:
                new_total_records = await self.session.scalar(
                    select(func.count()).select_from(Record)
                )
                self.logger.info(
                    f"Number of records in old table: {self.total_records}"
                )
                self.logger.info(f"Number of records in new table: {new_total_records}")
                if self.total_records == new_total_records:
                    self.logger.info(
                        "Number of records in the new table matches the number of records in old table"
                    )
                else:
                    self.logger.info(
                        "Number of records in the new table DOES NOT MATCH the number of records in old table."
                    )
                self.logger.info("Finished migrating :D")

        await engine.dispose()

    @backoff.on_exception(
        backoff.expo, Exception, max_tries=5, max_time=60, jitter=backoff.full_jitter
    )
    async def bulk_insert_records(self, records_to_insert):
        try:
            self.session.add_all(records_to_insert)
            await self.session.commit()
            self.count += len(records_to_insert)
            self.logger.info(
                f"Done processing {self.count}/{self.total_records} records. {(self.count * 100)/self.total_records}%"
            )
        except IntegrityError as e:
            await self.session.rollback()
            self.logger.error(f"Duplicate record found for records {e}")
        except Exception as e:
            await self.session.rollback()
            self.logger.error(f"Error bulk insert for records at {self.count} records")

    async def get_info_from_mult_tables(self, records):
        records_to_insert = []
        for record in records:
            hashes = await self.get_index_record_hash(record.did)
            urls = await self.get_urls_record(record.did)
            url_metadata = await self.get_urls_metadata(record.did)
            acl = await self.get_index_record_ace(record.did)
            authz = await self.get_index_record_authz(record.did)
            alias = await self.get_index_record_alias(record.did)
            metadata = await self.get_index_record_metadata(record.did)

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
    async def get_index_record_hash(self, did):
        try:
            stmt = select(IndexRecordHash.hash_type, IndexRecordHash.hash_value).filter(
                IndexRecordHash.did == did
            )
            res_proxy = await self.session.execute(stmt)
            return {row.hash_type: row.hash_value for row in res_proxy}
        except Exception as e:
            raise Exception(f"Error with hash for {did}: {e}")

    @backoff.on_exception(
        backoff.expo, Exception, max_tries=5, max_time=10, jitter=backoff.full_jitter
    )
    async def get_urls_record(self, did):
        try:
            stmt = select(IndexRecordUrl.url).filter(IndexRecordUrl.did == did)
            res_proxy = await self.session.execute(stmt)
            return [row.url for row in res_proxy]
        except Exception as e:
            raise Exception(f"Error with urls for {did}: {e}")

    @backoff.on_exception(
        backoff.expo, Exception, max_tries=5, max_time=10, jitter=backoff.full_jitter
    )
    async def get_urls_metadata(self, did):
        try:
            stmt = select(
                IndexRecordUrlMetadata.url,
                IndexRecordUrlMetadata.key,
                IndexRecordUrlMetadata.value,
            ).filter(IndexRecordUrlMetadata.did == did)
            res_proxy = await self.session.execute(stmt)
            return {row.url: {row.key: row.value} for row in res_proxy}
        except Exception as e:
            raise Exception(f"Error with url metadata for {did}: {e}")

    @backoff.on_exception(
        backoff.expo, Exception, max_tries=5, max_time=10, jitter=backoff.full_jitter
    )
    async def get_index_record_ace(self, did):
        try:
            stmt = select(IndexRecordACE.ace).filter(IndexRecordACE.did == did)
            res_proxy = await self.session.execute(stmt)
            return [row.ace for row in res_proxy]
        except Exception as e:
            raise Exception(f"Error with ace for did {did}: {e}")

    @backoff.on_exception(
        backoff.expo, Exception, max_tries=5, max_time=10, jitter=backoff.full_jitter
    )
    async def get_index_record_authz(self, did):
        try:
            stmt = select(IndexRecordAuthz.resource).filter(IndexRecordAuthz.did == did)
            res_proxy = await self.session.execute(stmt)
            return [row.resource for row in res_proxy]
        except Exception as e:
            raise Exception(f"Error with authz: {e}")

    @backoff.on_exception(
        backoff.expo, Exception, max_tries=5, max_time=10, jitter=backoff.full_jitter
    )
    async def get_index_record_alias(self, did):
        try:
            stmt = select(IndexRecordAlias.did, IndexRecordAlias.name).filter(
                IndexRecordAlias.did == did
            )
            res_proxy = await self.session.execute(stmt)
            res = {}
            for row in res_proxy:
                if row.did not in res:
                    res[row.did] = []
                res[row.did].append(row.name)
            return res
        except Exception as e:
            raise Exception(f"Error with alias: {e}")

    @backoff.on_exception(
        backoff.expo, Exception, max_tries=5, max_time=10, jitter=backoff.full_jitter
    )
    async def get_index_record_metadata(self, did):
        try:
            stmt = select(IndexRecordMetadata.key, IndexRecordMetadata.value).filter(
                IndexRecordMetadata.did == did
            )
            res_proxy = await self.session.execute(stmt)
            return {row.key: row.value for row in res_proxy}
        except Exception as e:
            raise Exception(f"Error with metadata for did {did}: {e}")


if __name__ == "__main__":
    main()
