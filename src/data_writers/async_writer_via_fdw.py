import asyncio
from logging import getLogger

import sqlalchemy as sa

from src.common.enums import IsolationLevel
from src.database.connectors import (
    AsyncDatabaseConnector,
    SyncDatabaseConnector,
)
from src.database.foreign_data_wrapper import (
    build_copy_query,
    build_tableoid_map,
    connect_to_db_as_fdw,
    drop_fdw,
)
from src.graphs.data_node import DataNode
from src.utils.asyncio_helpers import (
    background_tasks,
    run_in_background,
)


logger = getLogger("ASYNC_DATA_WRITER_VIA_FDW")


class AsyncDataWriterViaFDW:
    """Like SyncSingleDataWriterViaFDW but asynchronous"""

    def __init__(self, source_db_dsn: str, target_db_dsn: str):
        self.database_connector = AsyncDatabaseConnector(database_dsn=target_db_dsn)
        self.sync_database_connector = SyncDatabaseConnector(database_dsn=target_db_dsn)
        self._event_loop = None

        logger.debug("connect to source database as FDW...")
        with self.sync_database_connector as db_connector:
            connect_to_db_as_fdw(
                target_database_connector=db_connector, source_db_dsn=source_db_dsn, target_db_dsn=target_db_dsn
            )

        source_database_connector = SyncDatabaseConnector(database_dsn=source_db_dsn)
        logger.debug("build tableoid_map...")
        with source_database_connector as source_connector, self.sync_database_connector as target_connector:
            self._tableoid_map = build_tableoid_map(
                source_connector=source_connector, target_connector=target_connector
            )

    def __enter__(self):
        self._event_loop = asyncio.get_event_loop()
        self._event_loop.run_until_complete(self.connect())
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._event_loop.run_until_complete(self.wait())
        if exc_type is not None:
            self._event_loop.run_until_complete(self.database_connector.rollback())
        else:
            self._event_loop.run_until_complete(self.database_connector.commit())
        self._event_loop.run_until_complete(self.disconnect())
        self._event_loop.close()

    async def connect(self):
        await self.database_connector.begin(isolation_level=IsolationLevel.READ_COMMITTED, readonly=False)

    async def disconnect(self):
        with self.sync_database_connector as db_connector:
            drop_fdw(database_connector=db_connector)
        await self.database_connector.close()

    @staticmethod
    async def wait():
        await asyncio.gather(*background_tasks)

    def write_data(self, *args, **kwargs):
        run_in_background(coroutine=self._write_single_data(*args, **kwargs), loop=self._event_loop)

    async def copy_data(
        self,
        table: sa.Table,
        condition: str | None,
    ):
        """Copies values from the table by applying the where condition"""
        insert_query = build_copy_query(table=table, condition=condition)

        async with await self.database_connector.connect() as connection:
            await self.database_connector.execute(connection=connection, query=insert_query)

    async def _write_single_data(self, source_metadata: sa.MetaData, node: DataNode):
        sa_table = source_metadata.tables[node.table]
        remote_tableoid = self._tableoid_map[node.tableoid]
        condition = f"ctid = '{node.ctid}' AND tableoid = '{remote_tableoid}'"
        await self.copy_data(table=sa_table, condition=condition)
