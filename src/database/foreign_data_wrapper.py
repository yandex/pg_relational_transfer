import sqlalchemy as sa

from src.config import settings
from src.database.connectors import SyncDatabaseConnector
from src.utils.parse_dsn import parse_dsn


def connect_to_db_as_fdw(target_database_connector: SyncDatabaseConnector, source_db_dsn: str, target_db_dsn: str):
    local_user, *_ = parse_dsn(target_db_dsn)
    remote_user, remote_pwd, remote_host, remote_port, remote_db_name = parse_dsn(source_db_dsn)
    if settings.OVERRIDE_REMOTE_HOST:
        remote_host = settings.OVERRIDE_REMOTE_HOST
    if settings.OVERRIDE_REMOTE_PORT:
        remote_port = settings.OVERRIDE_REMOTE_PORT
    target_database_connector.execute(
        f"""
    CREATE EXTENSION IF NOT EXISTS postgres_fdw;
    CREATE SERVER IF NOT EXISTS remote_fdw FOREIGN DATA WRAPPER postgres_fdw
        OPTIONS (dbname '{remote_db_name}', host '{remote_host}', port '{remote_port}');

    CREATE USER MAPPING IF NOT EXISTS
        FOR "{local_user}" SERVER remote_fdw OPTIONS (user '{remote_user}', password '{remote_pwd}');
    GRANT USAGE ON FOREIGN SERVER remote_fdw TO "{local_user}";

    DROP SCHEMA IF EXISTS "{settings.REMOTE_SCHEMA}" CASCADE;
    CREATE SCHEMA IF NOT EXISTS "{settings.REMOTE_SCHEMA}";
    IMPORT FOREIGN SCHEMA "{settings.SOURCE_SCHEMA}" FROM SERVER remote_fdw INTO "{settings.REMOTE_SCHEMA}";
    """
    )


def drop_fdw(database_connector: SyncDatabaseConnector):
    database_connector.execute(f'DROP SCHEMA IF EXISTS "{settings.REMOTE_SCHEMA}" CASCADE')
    database_connector.execute("DROP SERVER IF EXISTS remote_fdw CASCADE")


def build_tableoid_map(
    source_connector: SyncDatabaseConnector,
    target_connector: SyncDatabaseConnector,
) -> dict[str, str]:
    """
    Returns a dictionary, where the key is the tableoid from the source database,
    and the value is the tableoid from the target database of the remote schema
    """
    query_for_source = f"""
        SELECT c.relname, c.oid
        FROM pg_class c
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE c.relkind = 'r'
            AND n.nspname = '{settings.SOURCE_SCHEMA}';
    """
    query_for_target = "SELECT ftoptions, ftrelid FROM pg_foreign_table"  # ftoptions: [schema_name=...,table_name=...]
    source_name_oid_map = dict(source_connector.execute(query=query_for_source).fetchall())
    target_name_oid_map = {
        key[1].split("=")[1]: value for key, value in target_connector.execute(query=query_for_target).fetchall()
    }
    return {source_name_oid_map[name]: target_name_oid_map[name] for name in source_name_oid_map}


def build_copy_query(table: sa.Table, condition: str | None) -> str:
    table_columns_with_commas = ",".join(f'"{column.name}"' for column in table.columns)
    table_columns_excluded_with_commas = ",".join(f"EXCLUDED.{column.name}" for column in table.columns)
    table_pk_with_commas = ",".join(f'"{column.name}"' for column in table.primary_key.columns)

    return f"""
    INSERT INTO {settings.TARGET_SCHEMA}.{table}
        SELECT * FROM {settings.REMOTE_SCHEMA}.{table} {"WHERE " + condition if condition else ""}
        ON CONFLICT ({table_pk_with_commas})
        DO UPDATE SET ({table_columns_with_commas})=({table_columns_excluded_with_commas})"""
