from logging import getLogger

import sqlalchemy as sa

from src.common.enums import WalkerVersion, WriterVersion
from src.common.errors import TableNotFoundError
from src.data_writers import (
    AsyncDataWriterViaFDW,
    DataWriterProtocol,
    DataWriterToFile,
    SyncBatchOfDataWriterViaFDW,
    SyncSingleDataWriterViaFDW,
)
from src.database import metadata_utils
from src.database.connectors.sync_connector import SyncDatabaseConnector
from src.database.metadata_utils import get_reflected_metadata
from src.graph_rules import GraphRuleManager, SourceGraphRules
from src.graph_walkers import (
    AsyncDataGraphWalker,
    GraphWalkerProtocol,
    SyncDataGraphWalker,
    TableGraphWalker,
)
from src.utils.timer import timer


logger = getLogger("DATA_MANAGER")


class DataManager:
    """Manages the data, including running the transfer algorithm"""

    _VERSION_TO_WALKER_MAP: dict[WalkerVersion, type[GraphWalkerProtocol]] = {
        WalkerVersion.TABLE_WALKER: TableGraphWalker,
        WalkerVersion.DATA_WALKER_SYNC: SyncDataGraphWalker,
        WalkerVersion.DATA_WALKER_ASYNC: AsyncDataGraphWalker,
    }

    _VERSION_TO_WRITER_MAP: dict[WriterVersion, type[DataWriterProtocol]] = {
        WriterVersion.TO_FILE: DataWriterToFile,
        WriterVersion.SINGLE_DATA_VIA_FDW_SYNC: SyncSingleDataWriterViaFDW,
        WriterVersion.BATCH_OF_DATA_VIA_FDW_SYNC: SyncBatchOfDataWriterViaFDW,
        WriterVersion.VIA_FDW_ASYNC: AsyncDataWriterViaFDW,
    }

    _INCOMPATIBLE_WALKER_TO_WRITERS_MAP: dict[WalkerVersion, set[WriterVersion]] = {
        WalkerVersion.TABLE_WALKER: {
            WriterVersion.TO_FILE,
            WriterVersion.SINGLE_DATA_VIA_FDW_SYNC,
            WriterVersion.VIA_FDW_ASYNC,
        },
        WalkerVersion.DATA_WALKER_SYNC: {WriterVersion.BATCH_OF_DATA_VIA_FDW_SYNC},
        WalkerVersion.DATA_WALKER_ASYNC: {WriterVersion.BATCH_OF_DATA_VIA_FDW_SYNC},
    }

    @classmethod
    @timer
    def start_cloning_data(
        cls,
        *,
        source_db_url: str,
        target_db_url: str,
        graph_rule_manager: GraphRuleManager,
        walker_version: WalkerVersion,
        writer_version: WriterVersion,
    ):
        with SyncDatabaseConnector(database_dsn=source_db_url) as source_database_connector:
            metadata = metadata_utils.get_reflected_metadata(database_connector=source_database_connector)
            database_tables = metadata_utils.get_tables_from_metadata(metadata=metadata)

        cls._validate_source_rules(source_rules=graph_rule_manager.source_rules, database_tables=database_tables)
        cls._validate_compatibility_of_walker_and_writer(walker_version=walker_version, writer_version=writer_version)

        walker_class = cls._VERSION_TO_WALKER_MAP[walker_version]
        writer_class = cls._VERSION_TO_WRITER_MAP[writer_version]

        with writer_class(
            source_db_dsn=source_db_url,
            target_db_dsn=target_db_url,
        ) as writer:
            walker = walker_class(
                source_db_dsn=source_db_url,
                graph_rule_manager=graph_rule_manager,
                data_sending_callback=writer.write_data,
                database_tables=database_tables,
            )
            walker.start_walk()

    @classmethod
    def _validate_source_rules(cls, *, source_rules: SourceGraphRules, database_tables: dict[str, sa.Table]) -> None:
        for table_name in source_rules.tables:
            if table_name not in database_tables:
                raise TableNotFoundError(table_name)

    @classmethod
    def _validate_compatibility_of_walker_and_writer(
        cls, *, walker_version: WalkerVersion, writer_version: WriterVersion
    ) -> None:
        if writer_version in cls._INCOMPATIBLE_WALKER_TO_WRITERS_MAP[walker_version]:
            raise ValueError(f"Incompatible walker version {walker_version} and writer version {writer_version}")

    @classmethod
    def delete_data(cls, *, db_dsn: str) -> None:
        database_connector = SyncDatabaseConnector(database_dsn=db_dsn)

        with database_connector:
            db_metadata = get_reflected_metadata(database_connector=database_connector)
            for table in reversed(db_metadata.sorted_tables):
                database_connector.execute(table.delete())
