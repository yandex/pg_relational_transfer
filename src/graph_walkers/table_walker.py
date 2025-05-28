from collections import deque
from collections.abc import Callable, Iterable
from logging import getLogger

import sqlalchemy as sa

from src.common.errors import TableNotFoundError
from src.graph_rules import GraphRuleManager
from src.graphs.table_graph import (
    RelationEdge,
    TableGraph,
    build_table_graph_from_tables,
)
from src.utils.timer import timer


logger = getLogger("TABLE_GRAPH_WALKER")


class TableGraphWalker:
    """
    The algorithm walks through the tables, starting with the initial data, first "up" through the links, then "down"
    """

    def __init__(
        self,
        source_db_dsn: str,
        graph_rule_manager: GraphRuleManager,
        data_sending_callback: Callable,
        database_tables: dict[str, sa.Table],
    ):
        self._graph_rule_manager = graph_rule_manager
        self._data_sending_callback = data_sending_callback
        self._database_tables = database_tables

    def start_walk(self):
        self._run_deep_search_for_table_graph()

    def _get_metadata_tables(self) -> list[sa.Table]:
        result_tables: list[sa.Table] = []
        for table_name in self._graph_rule_manager.source_rules.tables:
            table = self._database_tables.get(table_name)
            if table is None:
                raise TableNotFoundError(table_name)
            result_tables.append(table)
        return result_tables

    @staticmethod
    def build_subgraph_using_dfs(
        graph: TableGraph[sa.Table], source: Iterable[sa.Table]
    ) -> tuple[TableGraph[sa.Table], list[sa.Table]]:
        tables_to_visit: list[sa.Table] = list(source)
        visited_tables: set[sa.Table] = set()
        subgraph = TableGraph[sa.Table]()

        while tables_to_visit:
            source = tables_to_visit.pop()
            if source not in visited_tables:
                targets = graph[source]
                for node in targets:
                    subgraph.add_edge(RelationEdge(source, node.target_table, node.source_key, node.target_key))
                    tables_to_visit.append(node.target_table)
                visited_tables.add(source)

        return subgraph, list(visited_tables)

    def deep_copy(
        self,
        graph: TableGraph,
        initial_tables: list[sa.Table],
        from_existing: bool = False,
    ) -> None:
        roots: list[sa.Table] = list()

        nodes: deque[RelationEdge] = deque()
        while initial_tables:
            table = initial_tables.pop()
            if not from_existing:
                filter_ = self._graph_rule_manager.source_rules.get_where_condition(table.name)
                self._data_sending_callback(table=table, condition=filter_)
            if graph[table]:
                roots.append(table)
                for node in graph[table]:
                    nodes.append(node)

        while nodes:
            node: RelationEdge = nodes.pop()
            new_rows_num = self._data_sending_callback(node=node)
            if new_rows_num > 0:
                for node_ in graph[node.target_table]:
                    nodes.append(node_)

    @timer
    def _run_deep_search_for_table_graph(self) -> None:
        graph = build_table_graph_from_tables(
            database_tables=self._database_tables, extract_table_function=lambda table: table
        )
        graph = self._graph_rule_manager.table_graph_rules.update_graph(graph)

        initial_tables = self._get_metadata_tables()

        inverse_graph = graph.get_inverse()
        inv_subgraph, visited_tables = self.build_subgraph_using_dfs(inverse_graph, initial_tables)
        self.deep_copy(
            graph=inv_subgraph,
            initial_tables=initial_tables,
        )

        subgraph, _ = self.build_subgraph_using_dfs(graph, visited_tables)
        self.deep_copy(
            graph=subgraph,
            initial_tables=visited_tables,
            from_existing=True,
        )
