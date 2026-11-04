from logging import getLogger
from typing import Any

import sqlalchemy as sa
from sqlalchemy.exc import OperationalError

from src.utils.retry_managers import retry_sync as retry


stream_logger = getLogger("SYNC_DATABASE_CONNECTOR")
sql_queries_logger = getLogger("sql_queries")


class SyncDatabaseConnector:
    def __init__(self, database_dsn: str):
        self.database_dsn = database_dsn
        self.engine = None
        self.connection = None

    def begin(self) -> None:
        self.engine = sa.create_engine(self.database_dsn, poolclass=sa.pool.NullPool)
        self.connection = self.engine.connect()
        self.connection.begin()
        stream_logger.debug("Start connection")

    def commit(self) -> None:
        self.connection.commit()

    def rollback(self) -> None:
        self.connection.rollback()

    def close(self) -> None:
        self.connection.close()
        stream_logger.debug("Close connection")

    def execute(self, query: str | sa.sql.expression.ClauseElement, parameters: dict | None = None) -> Any:
        if isinstance(query, str):
            query = sa.text(query)
        stream_logger.debug("%s", str(query).strip())
        sql_queries_logger.info("%s\n", str(query).strip())
        with retry(exceptions=OperationalError):
            return self.connection.execute(query, parameters=parameters)

    def __enter__(self):
        self.begin()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.rollback()
        else:
            self.commit()
        self.close()
