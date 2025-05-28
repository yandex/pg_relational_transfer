import asyncio
from collections.abc import Awaitable, Callable, Iterable

import asyncpg

from src.common.enums import IsolationLevel


class AsyncConnectionWrapper:
    def __init__(
        self, connection: asyncpg.Connection, release_callback: Callable[["AsyncConnectionWrapper"], Awaitable[None]]
    ):
        self.connection: asyncpg.Connection = connection
        self.transaction: asyncpg.connection.transaction.Transaction | None = None
        self._release_callback = release_callback

    def __hash__(self):
        return hash(self.connection)

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._release_callback(self)

    async def release(self):
        await self._release_callback(self)


class AsyncUniqueQueue:
    def __init__(self, values: set):
        self._queue = asyncio.Queue()
        self._set = values.copy()

        for value in self._set:
            self._queue.put_nowait(value)

    async def put(self, value):
        if value in self._set:
            raise ValueError(f"{value} is already present in the set.")
        self._set.add(value)
        await self._queue.put(value)

    async def get(self):
        value = await self._queue.get()
        self._set.remove(value)
        return value

    def __len__(self):
        return self._queue.qsize()

    def __bool__(self):
        return not self._queue.empty()

    def __iter__(self):
        yield from self._set


class AsyncConnectionPool:
    def __init__(self, connections: Iterable[asyncpg.Connection]):
        self._idle_connections: AsyncUniqueQueue = AsyncUniqueQueue(
            {AsyncConnectionWrapper(conn, self.release) for conn in connections}
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close_all()

    async def connect(self) -> AsyncConnectionWrapper:
        return await self._idle_connections.get()

    async def release(self, conn: AsyncConnectionWrapper):
        await self._idle_connections.put(conn)

    async def start_all(self, isolation_level: IsolationLevel, readonly: bool = False):
        for conn_wrapper in self._idle_connections:
            conn_wrapper.transaction = conn_wrapper.connection.transaction(
                isolation=isolation_level.value, readonly=readonly
            )
            await conn_wrapper.transaction.start()

    async def commit_all(self):
        for conn_wrapper in self._idle_connections:
            await conn_wrapper.transaction.commit()

    async def rollback_all(self):
        for conn_wrapper in self._idle_connections:
            await conn_wrapper.transaction.rollback()

    async def close_all(self):
        while self._idle_connections:
            conn_wrapper = await self._idle_connections.get()
            await conn_wrapper.connection.close()


async def create_async_connection_pool(pool_size: int, db_url: str, statement_cache_size=0) -> AsyncConnectionPool:
    connections: list[asyncpg.Connection] = []
    for _ in range(pool_size):
        conn: asyncpg.Connection = await asyncpg.connect(db_url, statement_cache_size=statement_cache_size)
        connections.append(conn)
    return AsyncConnectionPool(connections)
