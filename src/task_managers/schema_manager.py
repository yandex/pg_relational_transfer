from io import TextIOWrapper

import sqlalchemy as sa

from src.data_structures.table_graph import (
    TableGraph,
    build_table_graph_from_tables,
)
from src.database.connectors import SyncDatabaseConnector
from src.database.metadata_utils import (
    get_reflected_metadata,
    get_tables_from_metadata,
)
from src.database_walkers import TableGraphWalker
from src.utils.database_config import DatabaseConfig


class SchemaManager:
    """Manages database schema"""

    @classmethod
    def print_schema(cls, *, db_config: DatabaseConfig, source_tables: list[str], output: TextIOWrapper) -> None:
        database_connector = SyncDatabaseConnector(database_dsn=db_config.dsn)
        with database_connector:
            metadata = get_reflected_metadata(database_connector=database_connector)
        tables: dict[str, sa.Table] = get_tables_from_metadata(metadata=metadata)

        graph: TableGraph[sa.Table] = build_table_graph_from_tables(
            database_tables=tables, extract_table_function=lambda x: x
        )

        if len(source_tables) > 0:
            source_tables_set = set(source_tables)
            source_tables_sa = filter(lambda x: x.name in source_tables_set, tables.values())
            graph, _ = TableGraphWalker.build_subgraph_using_dfs(graph=graph, source=source_tables_sa)

        output.write("@startuml\n")
        for source_table in graph.nodes():
            output.write(f"class {source_table.name}\n")
        for edge in graph.edges():
            output.write(
                f'{edge.source_table.name} "{edge.source_key}" --> "{edge.target_key}" {edge.target_table.name}\n'
            )
        output.write("@enduml\n")

    @classmethod
    def clone_schema(
        cls,
        *,
        source_db_config: DatabaseConfig,
        target_db_config: DatabaseConfig,
        schema_name: str,
        include_extensions=True,
        exclude_constraints=True,
        exclude_indexes=True,
        sync_sequences=True,
    ):
        """
        Clone a database schema from source to target database.

        This class method creates a copy of a specified schema from the source database to the target database,
        with options to control various aspects of the cloning process.

        Args:
            source_db_config (DatabaseConfig): DSN of the source database.
            target_db_config (DatabaseConfig): DSN of the target database.
            schema_name (str): Name of the schema to clone.
            include_extensions (bool, optional): Whether to include extensions in the clone. Defaults to True.
            exclude_constraints (bool, optional): Whether to exclude constraints from the clone. Defaults to True.
            exclude_indexes (bool, optional): Whether to exclude indexes from the clone. Defaults to True.
            sync_sequences (bool, optional): Whether to synchronize sequences between databases. Defaults to True.
        """
        source_connector = SyncDatabaseConnector(database_dsn=source_db_config.dsn)
        target_connector = SyncDatabaseConnector(database_dsn=target_db_config.dsn)

        with source_connector, target_connector:
            if include_extensions:
                cls._clone_extensions(source_connector=source_connector, target_connector=target_connector)
            metadata = get_reflected_metadata(database_connector=source_connector)
            if exclude_constraints:
                cls._exclude_constraints(metadata=metadata)
            if exclude_indexes:
                cls._exclude_indexes(metadata=metadata)
            if sync_sequences:
                cls._sync_sequences(metadata=metadata, source_connector=source_connector, schema_name=schema_name)
            metadata.create_all(bind=target_connector.connection)

    @classmethod
    def recreate_schema(cls, db_config: DatabaseConfig, schema: str) -> None:
        database_connector = SyncDatabaseConnector(database_dsn=db_config.dsn)
        with database_connector as connector:
            connector.execute(f"DROP SCHEMA {schema} CASCADE")
            connector.execute(f"CREATE SCHEMA {schema}")

    @classmethod
    def _sync_sequences(
        cls, *, metadata: sa.MetaData, source_connector: SyncDatabaseConnector, schema_name: str
    ) -> None:
        """
        This class method retrieves sequence information from the source database and updates the corresponding
        sequences in the database's metadata. It handles both standalone sequences and sequences associated
        with identity columns.

        Args:
            metadata: SQLAlchemy metadata object representing the database schema.
            source_connector: Database connector for the source database.
            schema_name: Name of the schema containing the sequences to synchronize.
        """
        postgres_type_map = {
            "int8": sa.BIGINT,
            "bigint": sa.BIGINT,
            "int4": sa.INTEGER,
            "integer": sa.INTEGER,
            "int2": sa.SMALLINT,
            "smallint": sa.SMALLINT,
        }
        select_sequences_query = f"""
            SELECT
                sequencename, last_value, increment_by, start_value, min_value, max_value, cycle, data_type
            FROM pg_sequences
            WHERE schemaname = '{schema_name}'
        """
        db_sequences_map = {
            row.sequencename: row for row in source_connector.execute(select_sequences_query).mappings()
        }
        select_sequence_name_query = "SELECT pg_get_serial_sequence(:table, :column)"

        identity_names_set = set()
        for table in metadata.tables.values():
            for column in table.c:
                if column.identity:
                    identity_name = (
                        source_connector.execute(
                            select_sequence_name_query, {"table": table.name, "column": column.name}
                        )
                        .scalar_one()
                        .split(".")[-1]
                    )
                    sequence = db_sequences_map[identity_name]
                    column.identity.start = (
                        sequence.last_value + sequence.increment_by
                        if sequence.last_value is not None
                        else sequence.start_value
                    )
                    identity_names_set.add(identity_name)
                elif (
                    column.server_default is not None
                    and hasattr(column.server_default, "arg")
                    and "nextval" in str(column.server_default.arg)
                ):
                    # this may not work well if the table contains two or more auto-increment columns
                    # without identification, because sqlalchemy accepts a single auto-increment field.
                    column.autoincrement = False

        for sequence_name, sequence_details in db_sequences_map.items():
            if sequence_name not in identity_names_set:
                sa.Sequence(
                    name=sequence_details.sequencename,
                    start=sequence_details.last_value + sequence_details.increment_by
                    if sequence_details.last_value is not None
                    else sequence_details.start_value,
                    increment=sequence_details.increment_by,
                    minvalue=sequence_details.min_value,
                    maxvalue=sequence_details.max_value,
                    cycle=sequence_details.cycle,
                    data_type=postgres_type_map[sequence_details.data_type],
                    metadata=metadata,
                )

    @classmethod
    def _exclude_constraints(
        cls,
        *,
        metadata: sa.MetaData,
        exclude_primary_key_constraint=False,
        exclude_foreign_key_constraint=True,
        exclude_unique_constraint=True,
        exclude_check_constraint=True,
    ):
        """
        Class method to remove specific constraints from all tables in the provided SQLAlchemy metadata.

        Iterates through each table in the metadata and removes constraints based on the provided flags.
        Supported constraint types include primary key, foreign key, unique, and check constraints.

        Args:
            metadata (sa.MetaData): The SQLAlchemy metadata object containing the database schema.
            exclude_primary_key_constraint (bool, optional): If True, primary key constraints are removed.
                Defaults to False.
            exclude_foreign_key_constraint (bool, optional): If True, foreign key constraints are removed.
                Defaults to True.
            exclude_unique_constraint (bool, optional): If True, unique constraints are removed. Defaults to True.
            exclude_check_constraint (bool, optional): If True, check constraints are removed. Defaults to True.
        """
        for table in metadata.tables.values():
            for constraint in table.constraints.copy():
                if (
                    (isinstance(constraint, sa.PrimaryKeyConstraint) and exclude_primary_key_constraint)
                    or (isinstance(constraint, sa.ForeignKeyConstraint) and exclude_foreign_key_constraint)
                    or (isinstance(constraint, sa.UniqueConstraint) and exclude_unique_constraint)
                    or (isinstance(constraint, sa.CheckConstraint) and exclude_check_constraint)
                ):
                    table.constraints.remove(constraint)

    @classmethod
    def _exclude_indexes(cls, *, metadata: sa.MetaData):
        for table in metadata.tables.values():
            table.indexes.clear()

    @classmethod
    def _clone_extensions(
        cls, *, source_connector: SyncDatabaseConnector, target_connector: SyncDatabaseConnector
    ) -> None:
        """Clone all extensions"""
        select_extensions_query = "SELECT extname FROM pg_extension"
        extensions = source_connector.execute(query=select_extensions_query).scalars().all()
        for extension in extensions:
            create_extension_query = f'CREATE EXTENSION IF NOT EXISTS "{extension}"'
            target_connector.execute(query=create_extension_query)

    @classmethod
    def _clone_constraints(
        cls, *, source_connector: SyncDatabaseConnector, target_connector: SyncDatabaseConnector
    ) -> None:
        """Clone FK, Unique, Check constraints from source db"""
        metadata = get_reflected_metadata(database_connector=source_connector)

        # get constraints information
        unique_constraints: list[sa.UniqueConstraint] = []  # unique constraints should be added before fk
        other_constraints: list[sa.CheckConstraint | sa.ForeignKeyConstraint] = []
        for table in metadata.tables.values():
            for constraint in table.constraints:
                if isinstance(constraint, sa.UniqueConstraint):
                    unique_constraints.append(constraint)
                elif isinstance(constraint, sa.CheckConstraint | sa.ForeignKeyConstraint):
                    other_constraints.append(constraint)

        # paste constraints to new db
        for constraint in unique_constraints + other_constraints:
            target_connector.execute(
                query=f'ALTER TABLE "{constraint.table.name}" DROP CONSTRAINT IF EXISTS "{constraint.name}"',
            )

            match type(constraint):
                case sa.UniqueConstraint:
                    columns_with_commas = ",".join(f'"{c.name}"' for c in constraint.columns)
                    target_connector.execute(
                        query=f"""
                        ALTER TABLE "{constraint.table.name}" ADD CONSTRAINT "{constraint.name}"
                            UNIQUE ({columns_with_commas})
                        """,
                    )

                case sa.ForeignKeyConstraint:
                    keys_with_commas = ",".join(f'"{key}"' for key in constraint.column_keys)
                    columns_with_commas = ",".join(f'"{el.column.name}"' for el in constraint.elements)
                    target_connector.execute(
                        query=f"""
                        ALTER TABLE "{constraint.table.name}" ADD CONSTRAINT "{constraint.name}"
                            FOREIGN KEY ({keys_with_commas})
                            REFERENCES "{constraint.referred_table.name}" ({columns_with_commas})
                            {f"DEFERRABLE INITIALLY {constraint.initially}" if constraint.deferrable else ""}
                        """,
                    )
                case sa.CheckConstraint:
                    target_connector.execute(
                        query=f"""
                        ALTER TABLE "{constraint.table.name}" ADD CONSTRAINT "{constraint.name}"
                            CHECK ({constraint.sqltext})
                        """,
                    )
                case _:
                    raise NotImplementedError("Unknown constraint type")
