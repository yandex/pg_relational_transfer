import asyncio
from collections.abc import Callable
from logging import getLogger

import sqlalchemy as sa

from src.common.enums import IsolationLevel
from src.database.connectors import AsyncDatabaseConnector
from src.database.connectors.sync_connector import SyncDatabaseConnector
from src.database.metadata_utils import get_reflected_metadata
from src.graph_rules import GraphRuleManager
from src.graphs.data_node import DataNode
from src.graphs.table_graph import (
    RelationEdge,
    TableGraph,
    build_table_graph_from_tables,
)
from src.node_keepers.node_keeper import NodeIdKeeper
from src.node_keepers.node_queue import NodeQueue
from src.utils.timer import timer


logger = getLogger("ASYNC_DATA_GRAPH_WALKER")


class AsyncDataGraphWalker:
    """Like SyncDataWalker but asynchronous"""

    def __init__(
        self,
        source_db_dsn: str,
        graph_rule_manager: GraphRuleManager,
        data_sending_callback: Callable,
        database_tables: dict[str, sa.Table],
    ):
        self.database_connector = AsyncDatabaseConnector(database_dsn=source_db_dsn)

        self._graph_rule_manager = graph_rule_manager
        self._data_sending_callback = data_sending_callback
        self._database_tables = database_tables
        self._event_loop = None

        with SyncDatabaseConnector(database_dsn=source_db_dsn) as sync_database_connector:
            self._metadata = get_reflected_metadata(database_connector=sync_database_connector)

    def start_walk(self):
        self._event_loop = asyncio.new_event_loop()
        self._event_loop.run_until_complete(self._run_bfs_for_data_graph())
        self._event_loop.close()

    async def _find_start_nodes(self) -> list[DataNode]:
        start_nodes: list[DataNode] = []

        async def initial_select(_table: str):
            async with await self.database_connector.connect() as conn:
                condition = self._graph_rule_manager.source_rules.get_where_condition(_table)
                query = f"SELECT ctid, tableoid FROM {_table} WHERE {condition}"
                ids = await self.database_connector.execute(connection=conn, query=query)
                for ctid, tableoid in ids:
                    start_nodes.append(DataNode(_table, ctid, tableoid))

        async with asyncio.TaskGroup() as tg:
            for table in self._graph_rule_manager.source_rules.tables:
                tg.create_task(initial_select(table))

        return start_nodes

    async def _find_next_nodes(self, cur_node: DataNode, graph_of_tables: TableGraph) -> list[DataNode]:
        next_nodes: list[DataNode] = []

        async def select_next_nodes(_ref_node: RelationEdge):
            async with await self.database_connector.connect() as conn:
                select_next_ctid_query = f"""
                SELECT ctid, tableoid FROM {_ref_node.target_table}
                    WHERE ({", ".join(_ref_node.target_key)}) = (
                        SELECT ({", ".join(_ref_node.source_key)}) FROM {_ref_node.source_table}
                            WHERE ctid = '{cur_node.ctid}' AND tableoid = '{cur_node.tableoid}'
                    )
                """

                select_next_ctid_query = self._graph_rule_manager.data_graph_rules.enrich_query(
                    query=select_next_ctid_query,
                    node=cur_node,
                    edge=ref_node,
                )

                next_ids = await self.database_connector.execute(connection=conn, query=select_next_ctid_query)
                for next_ctid, next_tableoid in next_ids:
                    next_nodes.append(DataNode(_ref_node.target_table, next_ctid, next_tableoid))

        async with asyncio.TaskGroup() as tg:
            for ref_node in graph_of_tables[cur_node.table]:
                tg.create_task(select_next_nodes(ref_node))

        return next_nodes

    @timer
    async def _run_bfs_for_data_graph(self) -> None:
        graph_of_tables = build_table_graph_from_tables(
            database_tables=self._database_tables, extract_table_function=lambda table: table.name
        )

        logger.debug("start session...")
        await self.database_connector.begin(IsolationLevel.REPEATABLE_READ, readonly=True)

        graph_of_tables = graph_of_tables + graph_of_tables.get_inverse()  # делаем двунаправленный граф
        graph_of_tables = self._graph_rule_manager.table_graph_rules.update_graph(graph_of_tables)
        logger.debug("graph_of_tables: %s", graph_of_tables)

        logger.debug("find start nodes...")
        start_nodes: list[DataNode] = await self._find_start_nodes()
        logger.debug("start nodes: %s", ", ".join(map(str, start_nodes)))
        nodes_visited: NodeIdKeeper = NodeIdKeeper(start_nodes)
        node_queue: NodeQueue = NodeQueue(start_nodes)
        logger.debug("start of the main loop...")
        while node_queue:
            logger.debug("start of the iteration...")
            cur_node: DataNode = node_queue.popleft()
            logger.debug("current node: %s", cur_node)
            logger.debug("push for copy...")
            self._data_sending_callback(node=cur_node, source_metadata=self._metadata)
            logger.debug("find next nodes...")
            for next_node in await self._find_next_nodes(cur_node=cur_node, graph_of_tables=graph_of_tables):
                logger.debug("next node: %s", next_node)
                if next_node not in nodes_visited:
                    logger.debug("new node!")
                    nodes_visited.add(next_node)
                    node_queue.append(next_node)
            logger.debug("end of the iteration\nnodes_visited: %s\nnode_queue: %s", nodes_visited, node_queue)
        logger.debug("end of the main loop")
