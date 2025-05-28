import sqlalchemy as sa

from src.database.connectors.sync_connector import SyncDatabaseConnector


def get_tables_from_metadata(metadata: sa.MetaData) -> dict[str, sa.Table]:
    return metadata.tables


def get_reflected_metadata(database_connector: SyncDatabaseConnector) -> sa.MetaData:
    metadata = sa.MetaData()
    metadata.reflect(database_connector.engine)
    return metadata
