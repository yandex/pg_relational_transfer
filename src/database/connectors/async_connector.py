from logging import getLogger
from typing import Any

import asyncpg

from src.common.enums import IsolationLevel
from src.config import settings
from src.database.async_connection_pool import (
    AsyncConnectionPool,
    AsyncConnectionWrapper,
    create_async_connection_pool,
)
from src.utils.retry_managers import retry_async as retry


stream_logger = getLogger("ASYNC_DATABASE_CONNECTOR")
sql_queries_logger = getLogger("sql_queries")


class AsyncDatabaseConnector:
    def __init__(self, database_dsn: str):
        self.database_dsn = database_dsn
        self.connection_pool: AsyncConnectionPool | None = None

    async def begin(self, isolation_level: IsolationLevel, readonly: bool = False):
        self.connection_pool = await create_async_connection_pool(
            pool_size=settings.CONNECTION_POOL_SIZE, db_url=self.database_dsn
        )
        await self.connection_pool.start_all(isolation_level, readonly=readonly)

    async def connect(self) -> AsyncConnectionWrapper:
        return await self.connection_pool.connect()

    async def commit(self) -> None:
        await self.connection_pool.commit_all()

    async def rollback(self) -> None:
        await self.connection_pool.rollback_all()

    async def close(self) -> None:
        await self.connection_pool.close_all()

    @staticmethod
    async def execute(connection: asyncpg.Connection, query: str) -> Any:
        query_strip = query.strip()
        stream_logger.debug(query_strip)
        sql_queries_logger.info("%s\n", query_strip)
        async with retry(exceptions=asyncpg.exceptions.PostgresConnectionError):
            return await connection.fetch(query)
