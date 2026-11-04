from collections.abc import Callable, Iterator
from logging import getLogger

from src.common.enums import IsolationLevel
from src.data_structures.data_node import DataBatch
from src.data_structures.table_tree import TableTree, TableTreeEdge
from src.database.connectors.async_connector import AsyncDatabaseConnector
from src.utils.database_config import DatabaseConfig


logger = getLogger("TABLE_TREE_WALKER")

DataBatchCtidTableoid = DataBatch[tuple[str, str]]


class TableTreePreOrderWithSeparatorIterator(Iterator):
    class Separator:
        pass

    def __init__(self, table_tree_root: TableTree | None):
        self.edge_stack = []
        if table_tree_root is not None:
            for edge in reversed(table_tree_root.edges):
                self.edge_stack.append(edge)

    def __next__(self):
        if not self.edge_stack:
            raise StopIteration

        current_edge = self.edge_stack.pop()
        if self.is_separator(current_edge):
            return current_edge
        self.edge_stack.append(self.Separator())

        for edge in reversed(current_edge.target_node.edges):
            self.edge_stack.append(edge)

        return current_edge

    def is_separator(self, edge: TableTreeEdge | Separator) -> bool:
        return isinstance(edge, self.Separator)


class TableTreeWalker:
    def __init__(
        self,
        source_db_config: DatabaseConfig,
        data_sending_callback: Callable,
        table_tree: TableTree,
    ):
        self._tree_root = table_tree
        self._tree_iterator = TableTreePreOrderWithSeparatorIterator(self._tree_root)

        self.database_connector = AsyncDatabaseConnector(database_dsn=source_db_config.dsn)
        self._data_sending_callback = data_sending_callback

    async def start_walk(self) -> None:
        await self._traverse_for_table_tree()

    async def _find_target_batch(
        self, tree_edge: TableTreeEdge, source_batch: DataBatchCtidTableoid | None = None
    ) -> DataBatchCtidTableoid:
        async with await self.database_connector.connect() as conn:
            query_args = []
            if not source_batch:
                select_next_batch_query = f"SELECT ctid, tableoid FROM {tree_edge.target_table}"
                if tree_edge.where_clause:
                    select_next_batch_query += f" WHERE ({tree_edge.where_clause})"
            else:
                select_next_batch_query = f"""
                WITH pairs (ctid, tableoid) AS (
                    SELECT * FROM unnest($1::tid[], $2::oid[])
                )
                SELECT ctid, tableoid FROM {tree_edge.target_table}
                WHERE {tree_edge.target_key} IN (
                    SELECT {tree_edge.source_key} FROM {tree_edge.source_table} AS source
                    JOIN pairs ON source.ctid = pairs.ctid AND source.tableoid = pairs.tableoid
                )
                """
                ctid_list = [key[0] for key in source_batch.keys]
                tableoid_list = [key[1] for key in source_batch.keys]
                query_args = [ctid_list, tableoid_list]
                if tree_edge.where_clause:
                    select_next_batch_query += f" AND ({tree_edge.where_clause})"
            next_ids = await self.database_connector.execute(conn, select_next_batch_query, *query_args)

        return DataBatchCtidTableoid(table=tree_edge.target_table, keys=set(tuple(r.values()) for r in next_ids))

    async def _traverse_for_table_tree(self) -> None:
        await self.database_connector.begin(IsolationLevel.REPEATABLE_READ, readonly=True)
        data_batch_stack: list[DataBatchCtidTableoid] = []

        for tree_edge in self._tree_iterator:
            if self._tree_iterator.is_separator(tree_edge):
                data_batch_stack.pop()
                continue
            source_batch: DataBatchCtidTableoid | None = data_batch_stack[-1] if tree_edge.source_table else None
            target_batch = await self._find_target_batch(tree_edge=tree_edge, source_batch=source_batch)
            self._data_sending_callback(batch=target_batch)
            logger.debug("Target batch: %s", target_batch)
            data_batch_stack.append(target_batch)

        await self.database_connector.close()
