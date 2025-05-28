import abc
from logging import getLogger

import sqlalchemy as sa

from src.config import settings
from src.database.connectors import SyncDatabaseConnector
from src.database.foreign_data_wrapper import (
    build_copy_query,
    build_tableoid_map,
    connect_to_db_as_fdw,
    drop_fdw,
)
from src.graphs.data_node import DataNode
from src.graphs.table_graph import RelationEdge


logger = getLogger("SYNC_DATA_WRITER_VIA_FDW")


class SyncDataWriterViaFDW(abc.ABC):
    """Walker's abstract class, which connects to the foreign data wrapper and writes data through it"""

    def __init__(self, source_db_dsn: str, target_db_dsn: str):
        self.database_connector = SyncDatabaseConnector(database_dsn=target_db_dsn)

        logger.debug("connect to source database as FDW...")
        with self.database_connector as db_connector:
            connect_to_db_as_fdw(
                target_database_connector=db_connector, source_db_dsn=source_db_dsn, target_db_dsn=target_db_dsn
            )

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.database_connector.rollback()
        else:
            self.database_connector.commit()
        self.disconnect()

    def connect(self):
        self.database_connector.begin()

    def disconnect(self):
        with self.database_connector:
            drop_fdw(database_connector=self.database_connector)
        self.database_connector.close()

    def copy_data(
        self,
        table: sa.Table,
        condition: str | None,
    ):
        """Copies values from the table by applying the where condition"""
        insert_query = build_copy_query(table=table, condition=condition)

        self.database_connector.execute(query=insert_query)

    @abc.abstractmethod
    def write_data(self, *args, **kwargs):
        raise NotImplementedError("write_data method is not implemented")


class SyncSingleDataWriterViaFDW(SyncDataWriterViaFDW):
    """Accepts and writes one record"""

    def __init__(self, source_db_dsn: str, target_db_dsn: str):
        source_database_connector = SyncDatabaseConnector(database_dsn=source_db_dsn)
        super().__init__(source_db_dsn=source_db_dsn, target_db_dsn=target_db_dsn)

        logger.debug("build tableoid_map...")
        with source_database_connector as source_connector, self.database_connector as target_connector:
            self._tableoid_map = build_tableoid_map(
                source_connector=source_connector, target_connector=target_connector
            )
        logger.debug("tableoid_map: %s", self._tableoid_map)

    def write_data(self, *args, **kwargs):
        self._write_single_data(*args, **kwargs)

    def _write_single_data(self, source_metadata: sa.MetaData, node: DataNode):
        sa_table = source_metadata.tables[node.table]
        remote_tableoid = self._tableoid_map[node.tableoid]
        condition = f"ctid = '{node.ctid}' AND tableoid = '{remote_tableoid}'"
        self.copy_data(table=sa_table, condition=condition)


class SyncBatchOfDataWriterViaFDW(SyncDataWriterViaFDW):
    """Accepts and writes batch of records"""

    def __init__(self, source_db_dsn: str, target_db_dsn: str):
        super().__init__(source_db_dsn=source_db_dsn, target_db_dsn=target_db_dsn)

    def write_data(self, *_, **kwargs) -> int | None:
        if "node" in kwargs:
            return self.copy_related_table(node=kwargs["node"])
        elif "table" in kwargs:
            self.copy_data(table=kwargs["table"], condition=kwargs.get("condition"))
        else:
            raise ValueError("Incorrect values for handle")

    def copy_related_table(self, node: RelationEdge[sa.Table, str]) -> int:
        """
        Copies from remote.target_table to public.target_table,
        filtering by keys from public.source_table and returns the number of inserted items
        """

        source_keys_with_commas = ",".join(node.source_key)

        select_source_values_query = (
            f"SELECT {source_keys_with_commas} FROM {node.source_table} WHERE {source_keys_with_commas} IS NOT NULL"
        )
        source_value_set = set(self.database_connector.execute(query=select_source_values_query))
        if not source_value_set:
            return 0
        source_values_with_commas = map(lambda values: ",".join(f"'{value}'" for value in values), source_value_set)
        source_values_with_commas_framed_by_brackets = "(" + "),(".join(source_values_with_commas) + ")"

        target_keys_with_commas = ",".join(node.target_key)
        target_primary_keys_with_commas = ",".join(f'"{c.name}"' for c in node.target_table.primary_key.c)

        select_new_target_values_query = f"""
        SELECT {target_primary_keys_with_commas} FROM {settings.REMOTE_SCHEMA}.{node.target_table}
            WHERE ({target_keys_with_commas}) IN {source_values_with_commas_framed_by_brackets}
        """
        new_target_value_set = set(self.database_connector.execute(query=select_new_target_values_query))

        select_old_target_values_query = f"SELECT {target_primary_keys_with_commas} FROM {node.target_table}"
        old_target_value_set = set(self.database_connector.execute(query=select_old_target_values_query))

        new_target_value_set -= old_target_value_set
        if not new_target_value_set:
            return 0

        new_values_with_commas = map(lambda values: ",".join(f"'{value}'" for value in values), new_target_value_set)
        new_values_with_commas_framed_by_brackets = "(" + "),(".join(new_values_with_commas) + ")"
        condition = f"({target_primary_keys_with_commas}) IN {new_values_with_commas_framed_by_brackets}"
        self.copy_data(table=node.target_table, condition=condition)

        return len(new_target_value_set)
