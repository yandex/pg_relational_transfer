from logging import getLogger
import sys

import click

from src.common.enums import WalkerVersion, WriterVersion
from src.graph_rules import GraphRuleManager, RuleLoader
from src.task_managers import DataManager, SchemaManager


logger = getLogger("CLI")


@click.command("print-schema")
@click.option("--db", required=True, help="Database connection url")
@click.option(
    "--table",
    required=False,
    default=[],
    help="Origin table name(s). If empty, outputs the entire schema.",
    multiple=True,
)
@click.option("--output", required=False, help="Name of the output file. By default the output is in stdout")
def print_schema(db: str, table: list[str], output: str | None = None) -> None:
    """Output a schema of dependent tables in the PlantUML language to standard output."""
    # TODO: добавить возможность прокинуть схему (по умолчанию default)
    file = open(output, "w") if output else sys.stdout
    try:
        SchemaManager.print_schema(db=db, source_tables=table, output=file)
    except Exception:
        logger.exception("An exception occurred during operation")
    finally:
        if output:
            file.close()


@click.command("clone-schema")
@click.option("--source-db", required=True, help="Source database connection url")
@click.option("--target-db", required=True, help="Target database connection url")
def clone_schema(source_db: str, target_db: str) -> None:
    """Clone a schema from one database to another."""
    # TODO: добавить возможность прокинуть параметр --schema + через ENV
    try:
        SchemaManager.clone_schema(source_db=source_db, target_db=target_db)
    except Exception:
        logger.exception("An exception occurred during operation")


@click.command("clear-schema")
@click.option("--db", required=True, help="Database connection url")
@click.option("--schema", required=False, help="Schema for clearing (default=public)", default="public")
def clear_schema(db: str, schema: str) -> None:
    """Clear database schema."""
    try:
        SchemaManager.recreate_schema(db=db, schema=schema)
    except Exception:
        logger.exception("An exception occurred during operation")


@click.command("clone-data")
@click.option("--source-db", required=True, help="Source database connection url")
@click.option("--target-db", required=True, help="Target database connection url")
@click.option(
    "--rule-path",
    required=True,
    help="Path to the file with the graph rules. ",
)
@click.option("--walker", required=False, help="Walker version", default=WalkerVersion.DATA_WALKER_SYNC)
@click.option("--writer", required=False, help="Writer version", default=WriterVersion.SINGLE_DATA_VIA_FDW_SYNC)
def clone_data(source_db: str, target_db: str, rule_path: str, walker: WalkerVersion, writer: WriterVersion) -> None:
    """Transfer related data from one database to another."""
    try:
        graph_rule_manager: GraphRuleManager = RuleLoader.load_rules(rules_path=rule_path)
        DataManager.start_cloning_data(
            source_db_url=source_db,
            target_db_url=target_db,
            graph_rule_manager=graph_rule_manager,
            walker_version=walker,
            writer_version=writer,
        )
    except Exception:
        logger.exception("An exception occurred during operation")


@click.command("clear-data")
@click.option("--db", required=True, help="Database connection string")
def clear_data(db: str) -> None:
    """Clear database data"""
    # TODO: добавить возможность прокинуть параметр --schema + через ENV
    try:
        DataManager.delete_data(db_dsn=db)
    except Exception:
        logger.exception("An exception occurred during operation")
