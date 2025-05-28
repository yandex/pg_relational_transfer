from collections.abc import Callable
from logging import getLogger

import sqlalchemy as sa

from src.database.connectors import SyncDatabaseConnector
from src.database.metadata_utils import get_reflected_metadata
from src.graph_rules import GraphRuleManager
from src.graphs.data_node import DataNode
from src.graphs.table_graph import (
    TableGraph,
    build_table_graph_from_tables,
)
from src.node_keepers.node_keeper import NodeIdKeeper
from src.node_keepers.node_queue import NodeQueue
from src.utils.timer import timer


logger = getLogger("SYNC_DATA_GRAPH_WALKER")


class SyncDataGraphWalker:
    """The algorithm runs the BFS through the data graph, starting from the initial data"""

    def __init__(
        self,
        source_db_dsn: str,
        graph_rule_manager: GraphRuleManager,
        data_sending_callback: Callable,
        database_tables: dict[str, sa.Table],
    ):
        self.database_connector = SyncDatabaseConnector(database_dsn=source_db_dsn)

        self._graph_rule_manager = graph_rule_manager
        self._data_sending_callback = data_sending_callback
        self._database_tables = database_tables

        with self.database_connector:
            self._metadata = get_reflected_metadata(database_connector=self.database_connector)

    def start_walk(self):
        self._run_bfs_for_data_graph()

    def _find_start_nodes(self) -> list[DataNode]:
        result = []
        for table in self._graph_rule_manager.source_rules.tables:
            condition = self._graph_rule_manager.source_rules.get_where_condition(table)
            query = f"SELECT ctid, tableoid FROM {table} WHERE {condition}"
            ids = self.database_connector.execute(query=query).fetchall()
            for ctid, tableoid in ids:
                result.append(DataNode(table, ctid, tableoid))
        return result

    def _find_next_nodes(self, cur_node: DataNode, graph_of_tables: TableGraph) -> list[DataNode]:
        for ref_node in graph_of_tables[cur_node.table]:
            select_next_ctid_query = f"""
            SELECT ctid, tableoid FROM {ref_node.target_table}
                WHERE ({", ".join(ref_node.target_key)}) = (
                    SELECT ({", ".join(ref_node.source_key)}) FROM {ref_node.source_table}
                        WHERE ctid = '{cur_node.ctid}' AND tableoid = '{cur_node.tableoid}'
                )
            """

            select_next_ctid_query = self._graph_rule_manager.data_graph_rules.enrich_query(
                query=select_next_ctid_query,
                node=cur_node,
                edge=ref_node,
            )

            next_ids = self.database_connector.execute(query=select_next_ctid_query).fetchall()
            for next_ctid, next_tableoid in next_ids:
                yield DataNode(ref_node.target_table, next_ctid, next_tableoid)

    @timer
    def _run_bfs_for_data_graph(self) -> None:
        graph_of_tables = build_table_graph_from_tables(
            database_tables=self._database_tables, extract_table_function=lambda sa_table: sa_table.name
        )

        logger.debug("start session...")
        self.database_connector.begin()

        graph_of_tables = graph_of_tables + graph_of_tables.get_inverse()  # делаем двунаправленный граф
        graph_of_tables = self._graph_rule_manager.table_graph_rules.update_graph(graph_of_tables)
        logger.debug("graph_of_tables: %s", graph_of_tables)

        logger.debug("find start nodes...")
        start_nodes: list[DataNode] = self._find_start_nodes()
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
            for next_node in self._find_next_nodes(cur_node=cur_node, graph_of_tables=graph_of_tables):
                logger.debug("next node: %s", next_node)
                if next_node not in nodes_visited:
                    logger.debug("new node!")
                    nodes_visited.add(next_node)
                    node_queue.append(next_node)
            logger.debug("end of the iteration\nnodes_visited: %s\nnode_queue: %s", nodes_visited, node_queue)
        logger.debug("end of the main loop")
