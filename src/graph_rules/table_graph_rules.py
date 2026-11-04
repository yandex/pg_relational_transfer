from copy import deepcopy
from logging import getLogger
from typing import Annotated

from pydantic import BaseModel, BeforeValidator

from src.data_structures.table_graph import RelationEdge, TableGraph
from src.utils.rule_utils import convert_list_to_set, parse_key


table_graph_rules_logger = getLogger("TABLE_GRAPH_RULES")


class TableGraphRule(BaseModel):
    def update_graph(self, graph: TableGraph, initial_graph: TableGraph) -> TableGraph:
        raise NotImplementedError


class StandardTableGraphRule(TableGraphRule):
    table: str


class NoEnterTableGraphRule(StandardTableGraphRule):
    def update_graph(self, graph: TableGraph, _: TableGraph) -> TableGraph:
        inv_graph = graph.get_inverse()
        if self.table in inv_graph:
            del inv_graph[self.table]
        return inv_graph.get_inverse()


class NoExitTableGraphRule(StandardTableGraphRule):
    def update_graph(self, graph: TableGraph, _: TableGraph) -> TableGraph:
        result_graph = deepcopy(graph)
        if self.table in graph:
            del result_graph[self.table]
        return result_graph


class LimitDistanceTableGraphRule(StandardTableGraphRule):
    max_distance: int = 3

    def update_graph(self, graph: TableGraph, initial_graph: TableGraph) -> TableGraph:
        limited_graph = TableGraph()

        edges = []
        visited_tables = {self.table}
        for edge in graph[self.table]:
            edges.append((edge, 1))

        while edges:
            current_edge, distance = edges.pop()
            if distance > self.max_distance:
                continue
            limited_graph.add_edge(current_edge)
            visited_tables.add(current_edge.target_table)
            for next_edge in graph[current_edge.target_table]:
                if next_edge.target_table not in visited_tables:
                    edges.append((next_edge, distance + 1))

        return limited_graph


class AddEdgesTableGraphRule(TableGraphRule):
    from_table: str
    to_tables: Annotated[set[str], BeforeValidator(convert_list_to_set)]

    def update_graph(self, graph: TableGraph, initial_graph: TableGraph) -> TableGraph:
        result_graph = deepcopy(graph)
        for rel_edge in initial_graph[self.from_table]:
            if rel_edge.target_table in self.to_tables:
                result_graph.add_edge(rel_edge)

        return result_graph


class AddEdgeTableGraphRule(TableGraphRule):
    from_table: str
    to_table: str
    from_key: Annotated[tuple[str, ...], BeforeValidator(parse_key)]
    to_key: Annotated[tuple[str, ...], BeforeValidator(parse_key)]

    def update_graph(self, graph: TableGraph, initial_graph: TableGraph) -> TableGraph:
        result_graph = deepcopy(graph)
        new_edge = RelationEdge(
            source_table=self.from_table, target_table=self.to_table, source_key=self.from_key, target_key=self.to_key
        )
        if any(map(lambda x: x == new_edge, initial_graph[self.from_table])):
            result_graph.add_edge(new_edge)
        else:
            table_graph_rules_logger.warning("Edge %s does not exist!", new_edge)

        return result_graph


class AddNodeTableGraphRule(StandardTableGraphRule):
    def update_graph(self, graph: TableGraph, initial_graph: TableGraph) -> TableGraph:
        result_graph = deepcopy(graph)
        for edge in initial_graph[self.table]:
            result_graph.add_edge(edge)

        return result_graph
