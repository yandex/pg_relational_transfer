from dataclasses import dataclass

from src.common.enums import TraversalRuleTypes
from src.graph_rules.data_graph_rules import DataGraphRule
from src.graph_rules.table_graph_rules import TableGraphRule
from src.graphs.data_node import DataNode
from src.graphs.table_graph import RelationEdge, TableGraph


@dataclass
class GraphRuleManager:
    source_rules: "SourceGraphRules"
    table_graph_rules: "TableGraphRules"
    data_graph_rules: "DataGraphRules"


class SourceGraphRules:
    """
    Static data of the SOURCE RULES rules.
    They mean the data (tables) from which the walkers algorithms need to be run.
    """

    def __init__(self, rules: list[dict]):
        self._table_to_condition = dict()
        for rule in rules:
            table = rule["table"]
            condition = rule["where"]
            self._table_to_condition[table] = condition

    def get_where_condition(self, table: str) -> str:
        assert table in self._table_to_condition
        return self._table_to_condition[table]

    @property
    def tables(self) -> list[str]:
        return list(self._table_to_condition.keys())

    def __str__(self):
        """
        table1: condition1, ...
        table2: condition2, ...
        ...
        :return: string representation of the source rules
        """
        return "\n".join(f"{table}: {condition}" for table, condition in self._table_to_condition.items())


class TableGraphRules:
    """
    Static data of the TABLE GRAPH RULES.
    They change the structure of the graph that walker traverses.
    """

    def __init__(self, rules: list[TableGraphRule]):
        self._rules = rules

    def update_graph(self, graph: TableGraph) -> TableGraph:
        for rule in self._rules:
            graph = rule.update_graph(graph)
        return graph


@dataclass
class DataGraphRules:
    """
    Static data of the DATA GRAPH RULES.
    They filter the nodes that walker traverses.
    """

    def __init__(self, rules: dict[str, dict[TraversalRuleTypes, list[DataGraphRule]]]):
        self._rules = rules

    def enrich_query(self, query: str, node: DataNode, edge: RelationEdge) -> str:
        if edge.target_table in self._rules and TraversalRuleTypes.NO_ENTER in self._rules[edge.target_table]:
            for rule in self._rules[edge.target_table][TraversalRuleTypes.NO_ENTER]:
                query = rule.enrich_query(query=query)

        if edge.source_table in self._rules and TraversalRuleTypes.NO_EXIT in self._rules[edge.source_table]:
            for rule in self._rules[edge.source_table][TraversalRuleTypes.NO_EXIT]:
                query = rule.enrich_query(query=query, node=node, edge=edge)

        return query
