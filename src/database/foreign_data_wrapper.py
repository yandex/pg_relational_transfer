from src.config import settings
from src.database.connectors import SyncDatabaseConnector
from src.utils.database_config import DatabaseConfig


def connect_to_db_as_fdw(
    target_database_connector: SyncDatabaseConnector, source_db_config: DatabaseConfig, target_db_config: DatabaseConfig
):
    remote_host = settings.OVERRIDE_REMOTE_HOST if settings.OVERRIDE_REMOTE_HOST else source_db_config.host
    remote_port = settings.OVERRIDE_REMOTE_PORT if settings.OVERRIDE_REMOTE_PORT else source_db_config.port
    target_database_connector.execute(
        f"""
    CREATE EXTENSION IF NOT EXISTS postgres_fdw;
    CREATE SERVER IF NOT EXISTS remote_fdw FOREIGN DATA WRAPPER postgres_fdw
        OPTIONS (
            dbname '{source_db_config.database}',
            host '{remote_host}',
            port '{remote_port}'
        );

    CREATE USER MAPPING IF NOT EXISTS
        FOR "{target_db_config.username}" SERVER remote_fdw OPTIONS (
            user '{source_db_config.username}', password '{source_db_config.password}'
        );
    GRANT USAGE ON FOREIGN SERVER remote_fdw TO "{target_db_config.username}";

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
