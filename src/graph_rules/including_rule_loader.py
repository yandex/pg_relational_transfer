from src.common.enums import TraversalRuleTypes

from .data_graph_rules import DataGraphRule
from .rule_loader import RuleLoader
from .rule_managers import DataGraphRules, IncludingTableGraphRules
from .table_graph_rules import AddEdgesTableGraphRule, AddEdgeTableGraphRule, AddNodeTableGraphRule, TableGraphRule


class IncludingRuleLoader(RuleLoader):
    _TABLE_GRAPH_RULE_TO_RULE_CLS_MAP: dict[TraversalRuleTypes, type[TableGraphRule]] = {
        TraversalRuleTypes.ADD_EDGE: AddEdgeTableGraphRule,
        TraversalRuleTypes.ADD_EDGES: AddEdgesTableGraphRule,
        TraversalRuleTypes.ADD_NODE: AddNodeTableGraphRule,
    }

    # TODO
    _DATA_GRAPH_RULE_TO_RULE_CLS_MAP: dict[TraversalRuleTypes, type[DataGraphRule]] = {}

    @classmethod
    def _load_traversal_rules(cls, traversal_rules: list) -> tuple[IncludingTableGraphRules, DataGraphRules]:
        table_graph_rules: list[TableGraphRule] = [
            cls._TABLE_GRAPH_RULE_TO_RULE_CLS_MAP[rule["type"]](**value)
            for rule in traversal_rules
            for value in rule["values"]
        ]
        return IncludingTableGraphRules(rules=table_graph_rules), DataGraphRules(rules={})
