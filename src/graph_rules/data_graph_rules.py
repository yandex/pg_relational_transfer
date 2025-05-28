from src.graphs.data_node import DataNode
from src.graphs.table_graph import RelationEdge


class DataGraphRule:
    def __init__(self, table: str, where: str, **_):
        self._table = table
        self._where = where

    def enrich_query(self, query: str, **_) -> str:
        raise NotImplementedError


class NoEnterDataGraphRule(DataGraphRule):
    def enrich_query(self, query: str, **_) -> str:
        return query + f" AND NOT {self._where}"


class NoExitDataGraphRule(DataGraphRule):
    def enrich_query(self, query: str, node: DataNode, edge: RelationEdge, **_) -> str:
        return query + f" AND NOT EXISTS(SELECT 1 FROM {edge.source_table} WHERE ctid='{node.ctid}' AND {self._where})"
