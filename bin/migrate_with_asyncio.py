import argparse
import json
import config_helper
from cdislogging import get_logger
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
import time
import random
import re
import asyncio

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


# @profile
# def main():
#     migrator = IndexRecordMigrator()
#     asyncio.run(migrator.migrate_tables())
#     return


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

        self.chunk_size = 10
        self.concurrency = 5
        self.thread_pool_size = 3
        self.buffer_size = 10
        self.batch_size = 1000
        self.n_workers = self.thread_pool_size + self.concurrency

        self.engine = create_async_engine(
            f"postgresql+asyncpg://{usr}:{psw}@{pghost}:{pgport}/{db}", echo=True
        )
        self.async_session = sessionmaker(
            self.engine, expire_on_commit=False, class_=AsyncSession
        )

        # Base = declarative_base()
        # Base.metadata.create_all(self.engine)
        # Session = sessionmaker(bind=self.engine)

        # self.session = Session()

    async def init(self):
        async with self.async_session() as session:
            await session.run_sync(Base.metadata.create_all)

    async def migrate_tables(self):
        self.logger.info("Starting migration job...")
        async with self.async_session() as session:
            self.total_records = await session.scalar(
                select(func.count(IndexRecord.did))
            )
            self.logger.info(f"Total records to copy: {self.total_records}")

        collector_queue = asyncio.Queue(maxsize=self.n_workers)
        inserter_queue = asyncio.Queue(maxsize=self.buffer_size)
        # loop = asyncio.get_event_loop()

        self.logger.info("Collecting Data from old IndexD Table...")
        offset = 0
        collecters = loop.create_task(
            self.collect(collector_queue, self.batch_size, offset)
        )
        self.logger.info("Initializing workers...")
        workers = [
            loop.create_task(self.worker(j, inserter_queue, collector_queue))
            for j in range(self.n_workers)
        ]
        self.logger.info("Inserting Data to new table")
        inserters = [
            loop.create_task(self.insert_to_db(i, inserter_queue))
            for i in range(self.concurrency)
        ]

        await asyncio.gather(collecters)
        await collector_queue.join()

        for w in workers:
            w.cancel()
        await asyncio.gather(*workers, return_exceptions=True)

        await inserter_queue.join()

        for i in inserters:
            i.cancel()
        await asyncio.gather(*inserters, return_exceptions=True)

    async def collect(self, collector_queue, batch_size, offset):
        """ """
        while True:
            self.logger.info(
                f"Collecting {offset} - {offset+batch_size} records with collector"
            )
            try:
                records_to_insert = await self.query_record_with_offset(
                    offset, batch_size
                )
            except Exception as e:
                self.logger.error(f"Failed to query old table for offset {offset}")

            if not records_to_insert:
                break

            await collector_queue.put(records_to_insert)

            if len(records_to_insert) < batch_size:
                break

            offset += batch_size

            self.logger.info(f"Added {offset} records into the collector queue")

    async def worker(self, name, collector_queue, inserter_queue):
        # Handles the semaphore
        # while not collector_queue.empty():
        #     self.logger.info(f"Worker {name} adding records to insert queue")
        #     bulk_rows = await collector_queue.get()
        #     print(bulk_rows)
        #     await inserter_queue.put(bulk_rows)
        #     collector_queue.task_done()
        while True:
            bulk_rows = await collector_queue.get()
            if bulk_rows is None:
                break
            self.logger.info(f"Worker {name} adding records to insert queue")
            await inserter_queue.put(bulk_rows)
            collector_queue.task_done()

    async def insert_to_db(self, name, inserter_queue):
        async with self.async_session() as session:
            while True:
                self.logger.info(f"Inserter {name} bulk inserting records")
                bulk_rows = await inserter_queue.get()
                try:
                    async with session.begin():
                        session.add_all(bulk_rows)
                    await session.commit()
                    # self.session.bulk_save_objects(bulk_rows)
                except Exception as e:
                    self.session.rollback()
                    if "duplicate key value violates unique constraint" in str(e):
                        self.logger.error(f"Errored at {offset}: {e}")
                    else:
                        self.logger.error(f"Ran into error at {offset}: {e}")
                        break
                finally:
                    inserter_queue.task_done()
            self.logger.info("Successfully inserted to new table!")

    async def query_record_with_offset(self, offset, batch_size, retry_limit=4):
        async with self.async_session() as session:
            stmt = (
                self.session.query(IndexRecord)
                .offset(offset)
                .limit(batch_size)
                .yield_per(batch_size)
            )
            records_to_insert = []
            for row in stmt:
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
        # Extract the key value from the error message
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
    # cProfile.run("asyncio.run(migrator.index_record_to_new_table())", filename="profile_results.txt")
    end_time = time.time()

    print("Total Time: {}".format(end_time - start_time))
