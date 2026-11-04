import sqlalchemy as sa

from src.common.enums import RuleLoaderTypes
from src.common.errors import TableNotFoundError
from src.data_structures.table_graph import RelationEdge, TableGraph, build_table_graph_from_tables
from src.database import metadata_utils
from src.database.connectors.sync_connector import SyncDatabaseConnector
from src.graph_rules import GraphRuleManager, IncludingRuleLoader, RuleLoader, SourceGraphRules


class PathBuilder:
    LOADER_TYPE_MAP = {RuleLoaderTypes.EXCLUDING: RuleLoader, RuleLoaderTypes.INCLUDING: IncludingRuleLoader}

    @staticmethod
    def _validate_source_rules(*, source_rules: SourceGraphRules, database_tables: dict[str, sa.Table]) -> None:
        for table_name in source_rules.tables:
            if table_name not in database_tables:
                raise TableNotFoundError(table_name)

    @staticmethod
    def _do_build_path(graph_of_tables: TableGraph, source_rules: SourceGraphRules, output_path: str, max_depth: int):
        stack = [(RelationEdge(None, table, (), ()), 0) for table in source_rules.tables]

        with open(output_path, "w") as f:
            while stack:
                edge, depth = stack.pop()
                if depth == 0:
                    f.write(edge.target_table + "\n")
                else:
                    f.write("  " * depth + f"{edge.source_key} -> {edge.target_table}.{edge.target_key}\n")
                print("  " * depth + edge.target_table)
                if depth < max_depth:
                    for relation_edge in graph_of_tables[edge.target_table]:
                        stack.append((relation_edge, depth + 1))

    @staticmethod
    def build_path(
        db_connection_string: str, rule_path: str, output_path: str, max_depth: int, rule_loader_type: RuleLoaderTypes
    ):
        with SyncDatabaseConnector(database_dsn=db_connection_string) as source_database_connector:
            metadata = metadata_utils.get_reflected_metadata(database_connector=source_database_connector)
            database_tables = metadata_utils.get_tables_from_metadata(metadata=metadata)

        graph_rule_manager: GraphRuleManager = PathBuilder.LOADER_TYPE_MAP[rule_loader_type].load_rules(
            rules_path=rule_path
        )
        PathBuilder._validate_source_rules(
            source_rules=graph_rule_manager.source_rules, database_tables=database_tables
        )

        graph_of_tables = build_table_graph_from_tables(
            database_tables=database_tables, extract_table_function=lambda table: table.name
        )

        graph_of_tables = graph_of_tables + graph_of_tables.get_inverse()  # делаем двунаправленный граф
        graph_of_tables = graph_rule_manager.table_graph_rules.update_graph(graph_of_tables)

        PathBuilder._do_build_path(graph_of_tables, graph_rule_manager.source_rules, output_path, max_depth)
