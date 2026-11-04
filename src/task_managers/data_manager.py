from logging import getLogger

from src.common.enums import WalkerVersion, WriterVersion
from src.common.errors import TableNotFoundError
from src.database import metadata_utils
from src.database.connectors.sync_connector import SyncDatabaseConnector
from src.database_walkers import DataGraphWalker, GraphWalkerProtocol, TableTreeWalker, TreeWalkerProtocol
from src.database_writers import (
    BatchOfDataWriterViaFDW,
    DataWriterProtocol,
    DataWriterToFile,
    SingleDataWriterViaFDW,
)
from src.graph_rules import GraphRuleManager, RuleLoader, SourceGraphRules, TableTreeLoader
from src.utils.database_config import DatabaseConfig
from src.utils.timer import timer


logger = getLogger("DATA_MANAGER")


class DataManager:
    """Manages the data, including running the transfer algorithm"""

    _VERSION_TO_WALKER_MAP: dict[WalkerVersion, type[GraphWalkerProtocol | TreeWalkerProtocol]] = {
        WalkerVersion.DATA_GRAPH_WALKER: DataGraphWalker,
        WalkerVersion.TABLE_TREE_WALKER: TableTreeWalker,
    }

    _VERSION_TO_WRITER_MAP: dict[WriterVersion, type[DataWriterProtocol]] = {
        WriterVersion.TO_FILE: DataWriterToFile,
        WriterVersion.VIA_FDW_SINGLE: SingleDataWriterViaFDW,
        WriterVersion.VIA_FDW_BATCH: BatchOfDataWriterViaFDW,
    }

    _INCOMPATIBLE_WALKER_TO_WRITERS_MAP: dict[WalkerVersion, set[WriterVersion]] = {
        WalkerVersion.DATA_GRAPH_WALKER: {WriterVersion.VIA_FDW_BATCH},
        WalkerVersion.TABLE_TREE_WALKER: {WriterVersion.VIA_FDW_SINGLE},
    }

    def __init__(
        self,
        source_db_config: DatabaseConfig,
        target_db_config: DatabaseConfig,
        walker_version: WalkerVersion,
        writer_version: WriterVersion,
        rule_path: str | None = None,
    ):
        self.source_db_config = source_db_config
        self.target_db_config = target_db_config

        self._validate_compatibility_of_walker_and_writer(walker_version=walker_version, writer_version=writer_version)

        self.additional_walker_kwargs = {}
        if walker_version == WalkerVersion.DATA_GRAPH_WALKER:
            graph_rule_manager: GraphRuleManager = RuleLoader.load_rules(rules_path=rule_path)
            self._validate_source_rules(source_rules=graph_rule_manager.source_rules)
            self.additional_walker_kwargs["graph_rule_manager"] = graph_rule_manager
        elif walker_version == WalkerVersion.TABLE_TREE_WALKER:
            table_tree = TableTreeLoader.load_table_tree(filename=rule_path)
            self.additional_walker_kwargs["table_tree"] = table_tree

        self.walker_class = self._VERSION_TO_WALKER_MAP[walker_version]
        self.writer_class = self._VERSION_TO_WRITER_MAP[writer_version]

    @timer
    async def clone_data(self):
        async with self.writer_class(
            source_db_config=self.source_db_config,
            target_db_config=self.target_db_config,
        ) as writer:
            walker = self.walker_class(
                source_db_config=self.source_db_config,
                data_sending_callback=writer.write_data,
                **self.additional_walker_kwargs,
            )
            await walker.start_walk()

    def _validate_source_rules(self, *, source_rules: SourceGraphRules) -> None:
        with SyncDatabaseConnector(database_dsn=self.source_db_config.dsn) as source_database_connector:
            metadata = metadata_utils.get_reflected_metadata(database_connector=source_database_connector)
            database_tables = metadata_utils.get_tables_from_metadata(metadata=metadata)
        for table_name in source_rules.tables:
            if table_name not in database_tables:
                raise TableNotFoundError(table_name)

    def _validate_compatibility_of_walker_and_writer(
        self, *, walker_version: WalkerVersion, writer_version: WriterVersion
    ) -> None:
        if writer_version in self._INCOMPATIBLE_WALKER_TO_WRITERS_MAP.get(walker_version, {}):
            raise ValueError(f"Incompatible walker version {walker_version} and writer version {writer_version}")

    @classmethod
    def delete_data(cls, *, db_config: DatabaseConfig) -> None:
        database_connector = SyncDatabaseConnector(database_dsn=db_config.dsn)

        with database_connector:
            db_metadata = metadata_utils.get_reflected_metadata(database_connector=database_connector)
            for table in reversed(db_metadata.sorted_tables):
                database_connector.execute(table.delete())
