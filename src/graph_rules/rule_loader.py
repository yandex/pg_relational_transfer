from collections import defaultdict
import json
from logging import getLogger

from src.common.enums import TraversalRuleTypes
from src.graph_rules.data_graph_rules import (
    DataGraphRule,
    NoEnterDataGraphRule,
    NoExitDataGraphRule,
)
from src.graph_rules.rule_managers import (
    DataGraphRules,
    GraphRuleManager,
    SourceGraphRules,
    TableGraphRules,
)
from src.graph_rules.table_graph_rules import (
    LimitDistanceTableGraphRule,
    NoEnterTableGraphRule,
    NoExitTableGraphRule,
    TableGraphRule,
)


logger = getLogger("RULE_LOADER")


class RuleLoader:
    _TABLE_GRAPH_RULE_TO_RULE_CLS_MAP: dict[TraversalRuleTypes, type[TableGraphRule]] = {
        TraversalRuleTypes.NO_ENTER: NoEnterTableGraphRule,
        TraversalRuleTypes.NO_EXIT: NoExitTableGraphRule,
        TraversalRuleTypes.LIMIT_DISTANCE: LimitDistanceTableGraphRule,
    }

    _DATA_GRAPH_RULE_TO_RULE_CLS_MAP: dict[TraversalRuleTypes, type[DataGraphRule]] = {
        TraversalRuleTypes.NO_ENTER: NoEnterDataGraphRule,
        TraversalRuleTypes.NO_EXIT: NoExitDataGraphRule,
    }

    @classmethod
    def load_rules(cls, rules_path: str) -> GraphRuleManager:
        logger.debug("rules path: %s", rules_path)
        with open(rules_path) as f:
            rules = json.load(f)
        source_rules = rules.get("source_rules")
        if not isinstance(source_rules, list):
            raise ValueError(
                "Invalid source rules: %s. Format of source rules must be: '[rule1, rule2, ...]'", source_rules
            )

        source_rule_tables = set()
        for rule in source_rules:
            if not isinstance(rule, dict) or {"table", "where"} != set(rule):
                raise ValueError(
                    f"Invalid rule: {rule}. "
                    f"Format of source rule must be: '{{'table': 'table name', 'where': 'condition'}}'"
                )
            table_name = rule["table"]
            if table_name in source_rule_tables:
                raise ValueError(
                    f"Tables in source rules should be unique. Found duplicate: {table_name}"
                )
            source_rule_tables.add(table_name)
        logger.debug("result source_rules: %s", source_rules)

        traversal_rules = rules.get("traversal_rules")
        if not isinstance(traversal_rules, list):
            raise ValueError(
                "Invalid traversal rules: %s. Format of source rules must be: '[rule1, rule2, ...]'", traversal_rules
            )

        for rule in traversal_rules:
            if not isinstance(rule, dict) or {"type", "values"} != set(rule) or not isinstance(rule["values"], list):
                raise ValueError(
                    f"Invalid rule: {rule}. "
                    f"Format of source rule must be: "
                    f"'{{'type': 'rule type', 'values': [{{'table': 'table name'(, 'where': 'condition')}}]}}'"
                )

        table_graph_rules = []
        data_graph_rules = defaultdict(dict)

        for rule in traversal_rules:
            raw_rule_type = rule["type"]
            if raw_rule_type not in TraversalRuleTypes:
                raise NotImplementedError(f"Unknown rule type ({raw_rule_type}).")
            rule_type = TraversalRuleTypes(raw_rule_type)
            for value in rule["values"]:
                if "where" not in value:
                    table_graph_rules.append(RuleLoader._TABLE_GRAPH_RULE_TO_RULE_CLS_MAP[rule_type](**value))
                else:
                    if rule_type == TraversalRuleTypes.LIMIT_DISTANCE:
                        raise NotImplementedError(
                            f"Rule {raw_rule_type} is not implemented as DataGraphRule. Try using it without 'where'."
                        )
                    if "table" not in value:
                        raise ValueError(f"Invalid rule value: {value}. Value should contain 'table'")

                    table = value["table"]
                    if rule_type not in data_graph_rules[table]:
                        data_graph_rules[table][rule_type] = []
                    data_graph_rules[table][rule_type].append(
                        RuleLoader._DATA_GRAPH_RULE_TO_RULE_CLS_MAP[rule_type](**value)
                    )

        logger.debug("result table_graph_rules: %s", table_graph_rules)
        logger.debug("result data_graph_rules: %s", data_graph_rules)

        source_rules = SourceGraphRules(rules=source_rules)
        table_graph_rules = TableGraphRules(rules=table_graph_rules)
        data_graph_rules = DataGraphRules(rules=data_graph_rules)

        return GraphRuleManager(
            source_rules=source_rules, table_graph_rules=table_graph_rules, data_graph_rules=data_graph_rules
        )
