import asyncio
from logging import getLogger
import sys

import click

from src.common.enums import RuleLoaderTypes, WalkerVersion, WriterVersion
from src.config import settings
from src.task_managers import DataManager, SchemaManager
from src.utils.database_config import DatabaseConfig, create_database_config_from_dsn
from src.utils.path_builder import PathBuilder


logger = getLogger("CLI")


def read_database_config(dsn: str, password_from_env_vars: str | None = None) -> DatabaseConfig:
    if not dsn:
        raise ValueError("Database connection url is required")
    db_config = create_database_config_from_dsn(dsn)

    if db_config.password is None:
        db_config.password = (
            password_from_env_vars
            if password_from_env_vars is not None
            else click.prompt(f"password for {db_config.dsn}", hide_input=True)
        )
    return db_config


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
    # TODO: добавить возможность прокинуть схему (по умолчанию public)
    file = open(output, "w") if output else sys.stdout  # TODO: move it to class
    try:
        db_config = read_database_config(db, password_from_env_vars=settings.DB_PASSWORD)
        SchemaManager.print_schema(db_config=db_config, source_tables=table, output=file)
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
        source_db_config = read_database_config(source_db, password_from_env_vars=settings.SOURCE_DB_PASSWORD)
        target_db_config = read_database_config(target_db, password_from_env_vars=settings.TARGET_DB_PASSWORD)
        SchemaManager.clone_schema(
            source_db_config=source_db_config, target_db_config=target_db_config, schema_name="public"
        )
    except Exception:
        logger.exception("An exception occurred during operation")


@click.command("clear-schema")
@click.option("--db", required=True, help="Database connection url")
@click.option("--schema", required=False, help="Schema for clearing (default=public)", default="public")
def clear_schema(db: str, schema: str) -> None:
    """Clear database schema."""
    try:
        db_config = read_database_config(db, password_from_env_vars=settings.DB_PASSWORD)
        SchemaManager.recreate_schema(db_config=db_config, schema=schema)
    except Exception:
        logger.exception("An exception occurred during operation")


@click.command("clone-data")
@click.option("--source-db", required=True, help="Source database connection url")
@click.option("--target-db", required=True, help="Target database connection url")
@click.option(
    "--rule-path",
    required=True,
    help="Path to the file with the walker rules",
)
@click.option("--walker", required=False, help="Walker version", default=WalkerVersion.DATA_GRAPH_WALKER)
@click.option("--writer", required=False, help="Writer version", default=WriterVersion.VIA_FDW_SINGLE)
def clone_data(source_db: str, target_db: str, rule_path: str, walker: WalkerVersion, writer: WriterVersion) -> None:
    """Transfer related data from one database to another."""
    try:
        source_db_config = read_database_config(source_db, password_from_env_vars=settings.SOURCE_DB_PASSWORD)
        target_db_config = read_database_config(target_db, password_from_env_vars=settings.TARGET_DB_PASSWORD)
        data_manager = DataManager(
            source_db_config=source_db_config,
            target_db_config=target_db_config,
            walker_version=walker,
            writer_version=writer,
            rule_path=rule_path,
        )
        asyncio.run(data_manager.clone_data())
    except Exception:
        logger.exception("An exception occurred during operation")


@click.command("clear-data")
@click.option("--db", required=True, help="Database connection string")
def clear_data(db: str) -> None:
    """Clear database data"""
    # TODO: добавить возможность прокинуть параметр --schema + через ENV
    try:
        db_config = read_database_config(db, password_from_env_vars=settings.DB_PASSWORD)
        DataManager.delete_data(db_config=db_config)
    except Exception:
        logger.exception("An exception occurred during operation")


@click.command("build-path")
@click.option("--db", required=True, help="Database connection string")
@click.option(
    "--rule-path",
    required=True,
    help="Path to the file with the graph rules. ",
)
@click.option("--max-depth", required=False, help="Max depth of the path", default=10)
@click.option("--output-path", required=False, help="Path to the output file", default="path.yaml")
@click.option("--rule-loader-type", required=False, help="Type of rule loader", default=RuleLoaderTypes.INCLUDING)
def build_path(db: str, rule_path: str, max_depth: int, output_path: str, rule_loader_type: RuleLoaderTypes) -> None:
    """Write path through the table graph for given db with given rules."""
    PathBuilder.build_path(
        db_connection_string=db,
        rule_path=rule_path,
        max_depth=max_depth,
        output_path=output_path,
        rule_loader_type=rule_loader_type,
    )
