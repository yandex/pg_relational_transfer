from collections import defaultdict
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Generic, TypeVar

import sqlalchemy as sa

from src.utils.safe_merge import safe_merge


Ttable = TypeVar("Ttable")
Tfield = TypeVar("Tfield")


@dataclass(frozen=True)
class RelationEdge(Generic[Ttable, Tfield]):
    source_table: Ttable
    target_table: Ttable
    source_key: tuple[Tfield, ...]
    target_key: tuple[Tfield, ...]

    def __str__(self):
        return f"{self.source_table}.{self.source_key} -> {self.target_table}.{self.target_key}"


class TableGraph(Generic[Ttable]):
    """
    Graph of tables (data links)
    'graph[table] = {edge1, edge2, ...}' means that the table (node) has incident relations (edges) edge1, edge2, ...
    """

    def __init__(self, graph: dict[Ttable, set[RelationEdge]] | None = None):
        self._graph: dict[Ttable, set[RelationEdge]] = defaultdict(set)
        if graph is not None:
            self._graph = graph

    def add_edge(self, edge: RelationEdge) -> None:
        targets = self._graph[edge.source_table]
        targets.add(edge)

    def nodes(self) -> Iterable[Ttable]:
        return self._graph.keys()

    def __contains__(self, key: Ttable):
        return key in self._graph

    def __add__(self, other):
        return TableGraph(safe_merge(self._graph, other._graph))

    def __getitem__(self, key: Ttable):
        return self._graph[key]

    def __delitem__(self, key: Ttable):
        del self._graph[key]

    def edges(self) -> Iterable[RelationEdge]:
        for targets in self._graph.values():
            yield from targets

    def get_inverse(self) -> "TableGraph":
        inverse_graph = TableGraph()
        for edge in self.edges():
            inverse_graph.add_edge(
                RelationEdge(
                    source_table=edge.target_table,
                    target_table=edge.source_table,
                    source_key=edge.target_key,
                    target_key=edge.source_key,
                )
            )
        return inverse_graph

    def __str__(self):
        """
        table1:
            table1.field1 -> table2.field2
            table1.field3 -> table3.field4
        table2:
            table2.field5 -> table1.field1
        ...
        :return: string representation of the graph
        """
        return "\n".join(
            f"{table}:\n\t{'\n\t'.join(str(rel) for rel in edges)}" for table, edges in self._graph.items()
        )


def build_table_graph_from_tables(
    *, database_tables: dict[str, sa.Table], extract_table_function: Callable[[sa.Table], Ttable]
) -> TableGraph:
    graph = TableGraph()

    for table in database_tables.values():
        for constraint in table.foreign_key_constraints:
            fkeys = tuple(element.column.name for element in constraint.elements)
            is_one_to_one = tuple(element.name for element in table.primary_key.columns) == tuple(
                constraint.column_keys
            )
            if is_one_to_one:
                graph.add_edge(
                    RelationEdge(
                        source_table=extract_table_function(constraint.referred_table),
                        target_table=extract_table_function(table),
                        source_key=fkeys,
                        target_key=tuple(constraint.column_keys),
                    )
                )
            graph.add_edge(
                RelationEdge(
                    source_table=extract_table_function(table),
                    target_table=extract_table_function(constraint.referred_table),
                    source_key=tuple(constraint.column_keys),
                    target_key=fkeys,
                )
            )

    return graph
