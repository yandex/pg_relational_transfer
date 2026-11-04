import asyncio
from logging import getLogger

from src.common.enums import IsolationLevel
from src.config import settings
from src.data_structures.data_node import DataBatch, DataNode
from src.database import metadata_utils
from src.database.connectors import (
    AsyncDatabaseConnector,
    SyncDatabaseConnector,
)
from src.database.foreign_data_wrapper import (
    build_tableoid_map,
    connect_to_db_as_fdw,
    drop_fdw,
)
from src.utils.asyncio_helpers import (
    background_tasks,
    run_in_background,
)
from src.utils.database_config import DatabaseConfig


logger = getLogger("DATA_WRITER_VIA_FDW")


class BaseDataWriterViaFDW:
    """Walker's base class, which connects to the foreign data wrapper and writes data through it"""

    def __init__(self, source_db_config: DatabaseConfig, target_db_config: DatabaseConfig):
        self.database_connector = AsyncDatabaseConnector(database_dsn=target_db_config.dsn)
        self.sync_database_connector = SyncDatabaseConnector(database_dsn=target_db_config.dsn)

        logger.debug("connect to source database as FDW...")
        with self.sync_database_connector as db_connector:
            connect_to_db_as_fdw(
                target_database_connector=db_connector,
                source_db_config=source_db_config,
                target_db_config=target_db_config,
            )

        source_database_connector = SyncDatabaseConnector(database_dsn=source_db_config.dsn)
        logger.debug("build tableoid_map...")
        with source_database_connector as source_connector, self.sync_database_connector as target_connector:
            self._tableoid_map = build_tableoid_map(
                source_connector=source_connector, target_connector=target_connector
            )
            self._source_metadata = metadata_utils.get_reflected_metadata(database_connector=source_connector)

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.wait()
        if exc_type is not None:
            await self.database_connector.rollback()
        else:
            await self.database_connector.commit()
        await self.disconnect()

    async def connect(self):
        await self.database_connector.begin(isolation_level=IsolationLevel.READ_COMMITTED, readonly=False)

    async def disconnect(self):
        with self.sync_database_connector as db_connector:
            drop_fdw(database_connector=db_connector)
        await self.database_connector.close()

    @staticmethod
    async def wait():
        await asyncio.gather(*background_tasks)


class SingleDataWriterViaFDW(BaseDataWriterViaFDW):
    def write_data(self, *args, **kwargs):
        run_in_background(coroutine=self._write_single_data(*args, **kwargs))

    async def _write_single_data(self, node: DataNode):
        sa_table = self._source_metadata.tables[node.table]
        remote_tableoid = self._tableoid_map[node.tableoid]

        table_columns_with_commas = ",".join(column.name for column in sa_table.columns)
        table_columns_excluded_with_commas = ",".join(f"EXCLUDED.{column.name}" for column in sa_table.columns)
        table_pk_with_commas = ",".join(column.name for column in sa_table.primary_key.columns)

        insert_query = f"""
        INSERT INTO {settings.TARGET_SCHEMA}.{sa_table}
            SELECT {table_columns_with_commas} FROM {settings.REMOTE_SCHEMA}.{sa_table}
            WHERE ctid = $1 AND tableoid = $2
            ON CONFLICT ({table_pk_with_commas})
            DO UPDATE SET ({table_columns_with_commas})=({table_columns_excluded_with_commas})"""
        query_args = [node.ctid, remote_tableoid]

        async with await self.database_connector.connect() as connection:
            await self.database_connector.execute(connection, insert_query, *query_args)


class BatchOfDataWriterViaFDW(BaseDataWriterViaFDW):
    def write_data(self, *args, **kwargs):
        run_in_background(coroutine=self._write_batch_of_data(*args, **kwargs))

    async def _write_batch_of_data(self, batch: DataBatch):
        sa_table = self._source_metadata.tables[batch.table]
        remote_keys = []
        ctid_list = []
        for key in batch.keys:
            remote_keys.append(self._tableoid_map[key[1]])
            ctid_list.append(key[0])

        table_columns_with_commas = ",".join(column.name for column in sa_table.columns)
        table_columns_excluded_with_commas = ",".join(f"EXCLUDED.{column.name}" for column in sa_table.columns)
        table_pk_with_commas = ",".join(column.name for column in sa_table.primary_key.columns)

        insert_query = f"""
        WITH pairs (ctid, tableoid) AS (SELECT * FROM unnest($1::tid[], $2::oid[]))
        INSERT INTO {settings.TARGET_SCHEMA}.{sa_table}
            SELECT {table_columns_with_commas} FROM {settings.REMOTE_SCHEMA}.{sa_table} AS source
            JOIN pairs ON source.ctid = pairs.ctid AND source.tableoid = pairs.tableoid
            ON CONFLICT ({table_pk_with_commas})
            DO UPDATE SET ({table_columns_with_commas})=({table_columns_excluded_with_commas})
        """
        query_args = [ctid_list, remote_keys]

        async with await self.database_connector.connect() as connection:
            await self.database_connector.execute(connection, insert_query, *query_args)
