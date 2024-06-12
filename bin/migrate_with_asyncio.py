import gc
import argparse
import json
import config_helper
from cdislogging import get_logger
from sqlalchemy import create_engine, func
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import sessionmaker
import time
import asyncio

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

APP_NAME = "indexd"

logger = get_logger("migrate_single_table", log_level="debug")


def load_json(file_name):
    return config_helper.load_json(file_name, APP_NAME)


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

        self.auto_job_config = False
        self.insertion_workers = 10
        self.batch_size = 200
        self.collection_workers = 100
        self.counter = 0
        self.psql_pool_size = self.collection_workers + self.insertion_workers
        self.max_overflow = self.insertion_workers

        self.engine = create_async_engine(
            f"postgresql+asyncpg://{usr}:{psw}@{pghost}:{pgport}/{db}",
            echo=False,
            pool_size=self.psql_pool_size,
            max_overflow=self.max_overflow,
        )
        self.async_session = sessionmaker(
            self.engine, expire_on_commit=False, class_=AsyncSession
        )

    async def init(self):
        async with self.async_session() as session:
            await session.run_sync(Record.metadata.create_all)

    async def migrate_tables(self):
        self.logger.info("Starting migration job...")
        async with self.async_session() as session:
            self.total_records = await session.scalar(
                select(func.count(IndexRecord.did))
            )
            self.logger.info(f"Total records to copy: {self.total_records}")

        if (
            self.total_records - self.batch_size * self.collection_workers
        ) < 0 or self.auto_job_config:
            self.collection_workers = int(self.total_records / self.batch_size)
            # TODO: Change this log later
            self.logger.info(
                f"Batch size and number of workers exceeds total records to be copied. Changing number of collector workers to {self.batch_size}"
            )

        if self.auto_job_config:
            self.insertion_workers = int(self.collection_workers / 2)
            self.logger.info(
                f"Setting number of insertion workers to {self.insertion_workers}."
            )

        collector_queue = asyncio.Queue(maxsize=self.collection_workers)
        loop = asyncio.get_event_loop()

        self.logger.info("Collecting Data from old IndexD Table...")
        collect_tasks = [
            loop.create_task(self.collect(collector_queue, self.batch_size, i))
            for i in range(self.collection_workers)
        ]

        self.logger.info("Inserting Data to new table")
        insert_tasks = [
            loop.create_task(self.insert_to_db(i, collector_queue))
            for i in range(self.insertion_workers)
        ]

        await asyncio.gather(*collect_tasks)

        await collector_queue.join()

        for task in insert_tasks:
            task.cancel()

        await asyncio.gather(*insert_tasks, return_exceptions=True)

        self.logger.info(
            f"Migration job completed. {self.counter} records were considered duplicates."
        )

    async def collect(self, collector_queue, batch_size, worker_id):
        offset = worker_id * batch_size
        while offset < self.total_records:
            self.logger.info(
                f"Collecting {offset} - {offset + batch_size} records with collector"
            )
            try:
                records_to_insert = await self.query_record_with_offset(
                    offset, batch_size
                )
                if not records_to_insert:
                    self.logger.info(f"No more records to collect at offset {offset}")
                    break
            except Exception as e:
                self.logger.error(
                    f"Failed to query old table for offset {offset} with {e}"
                )
                break

            self.logger.info(f"Adding records to collector queue at offset {offset}")
            await collector_queue.put(records_to_insert)

            offset += self.collection_workers * batch_size

        self.logger.info(f"Collector finished collecting records.")
        await collector_queue.put(None)

    async def insert_to_db(self, name, collector_queue):
        async with self.async_session() as session:
            while True:
                self.logger.info(f"Inserter {name} waiting for records")
                bulk_rows = await collector_queue.get()

                if bulk_rows is None:
                    self.logger.info(
                        f"Inserter {name} didn't receive any records to insert. Killing worker..."
                    )
                    break

                if not bulk_rows:
                    continue

                self.logger.info(f"Inserter {name} bulk inserting records")
                try:
                    async with session.begin():
                        for record in bulk_rows:
                            exists = await session.execute(
                                select(Record).filter_by(guid=record.guid)
                            )
                            if not exists.scalar():
                                session.add(record)
                        # session.add_all(bulk_rows)
                    await session.commit()
                except Exception as e:
                    await session.rollback()
                    if "duplicate key value violates unique constraint" in str(e):
                        self.counter += 1
                        self.logger.error(f"Duplicate key error: {e}")
                    else:
                        self.logger.error(f"Error inserting records: {e}")
                finally:
                    gc.collect()
                    collector_queue.task_done()

            self.logger.info(f"Inserter {name} finished. Killing Inserter...")

    async def query_record_with_offset(self, offset, batch_size, retry_limit=4):
        async with self.async_session() as session:
            stmt = select(IndexRecord).offset(offset).limit(batch_size)
            results = await session.execute(stmt)
            records = results.scalars().all()
            records_to_insert = []
            for row in records:
                tasks = [
                    self.get_index_record_hash(row.did),
                    self.get_urls_record(row.did),
                    self.get_urls_metadata(row.did),
                    self.get_index_record_ace(row.did),
                    self.get_index_record_authz(row.did),
                    self.get_index_record_alias(row.did),
                    self.get_index_record_metadata(row.did),
                ]
                results = await asyncio.gather(*tasks)

                (
                    hashes,
                    urls,
                    url_metadata,
                    acl,
                    authz,
                    alias,
                    metadata,
                ) = results

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
            return records_to_insert

    async def get_index_record_hash(self, did):
        async with self.async_session() as session:
            stmt = select(IndexRecordHash.hash_type, IndexRecordHash.hash_value).where(
                IndexRecordHash.did == did
            )
            results = await session.execute(stmt)
            return {hash_type: hash_value for hash_type, hash_value in results}

    async def get_urls_record(self, did):
        async with self.async_session() as session:
            stmt = select(IndexRecordUrl.url).where(IndexRecordUrl.did == did)
            results = await session.execute(stmt)
            return [url for url, in results]

    async def get_urls_metadata(self, did):
        async with self.async_session() as session:
            stmt = select(
                IndexRecordUrlMetadata.url,
                IndexRecordUrlMetadata.key,
                IndexRecordUrlMetadata.value,
            ).where(IndexRecordUrlMetadata.did == did)
            results = await session.execute(stmt)
            url_metadata = {}
            for url, key, value in results:
                if url not in url_metadata:
                    url_metadata[url] = {}
                url_metadata[url][key] = value
            return url_metadata

    async def get_index_record_ace(self, did):
        async with self.async_session() as session:
            stmt = select(IndexRecordACE.ace).where(IndexRecordACE.did == did)
            results = await session.execute(stmt)
            return [ace for ace, in results]

    async def get_index_record_authz(self, did):
        async with self.async_session() as session:
            stmt = select(IndexRecordAuthz.resource).where(IndexRecordAuthz.did == did)
            results = await session.execute(stmt)
            return [resource for resource, in results]

    async def get_index_record_alias(self, did):
        async with self.async_session() as session:
            stmt = select(IndexRecordAlias.name).where(IndexRecordAlias.did == did)
            results = await session.execute(stmt)
            return [name for name, in results]

    async def get_index_record_metadata(self, did):
        async with self.async_session() as session:
            stmt = select(IndexRecordMetadata.key, IndexRecordMetadata.value).where(
                IndexRecordMetadata.did == did
            )
            results = await session.execute(stmt)
            return {key: value for key, value in results}

    def remove_duplicate_records(self, records, error):
        key_value = re.search(r"\(guid\)=\((.*?)\)", str(error)).group(1)
        self.logger.info(f"Removing duplicate record {key_value}")
        for record in records:
            if key_value == str(record.guid):
                records.remove(record)
                break


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
    asyncio.run(migrator.migrate_tables())
    end_time = time.time()

    print("Total Time: {}".format(end_time - start_time))
