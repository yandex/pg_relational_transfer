from dataclasses import dataclass
from io import TextIOWrapper

import sqlalchemy as sa

from src.config import settings
from src.database.connectors import SyncDatabaseConnector
from src.database.metadata_utils import (
    get_reflected_metadata,
    get_tables_from_metadata,
)
from src.graph_walkers import TableGraphWalker
from src.graphs.table_graph import (
    TableGraph,
    build_table_graph_from_tables,
)


@dataclass
class Sequence:
    name: str
    start_val: int
    min_val: int
    max_val: int
    increment_by: int
    last_val: int | None
    table_name: str | None = None
    column_name: str | None = None


class SchemaManager:
    """Manages database schema"""

    @classmethod
    def print_schema(cls, *, db: str, source_tables: list[str], output: TextIOWrapper) -> None:
        database_connector = SyncDatabaseConnector(database_dsn=db)
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
    def clone_schema(cls, *, source_db: str, target_db: str):
        source_connector = SyncDatabaseConnector(database_dsn=source_db)
        target_connector = SyncDatabaseConnector(database_dsn=target_db)

        with source_connector, target_connector:
            cls._clone_user_types(source_connector=source_connector, target_connector=target_connector)
            cls._clone_extensions(source_connector=source_connector, target_connector=target_connector)
            cls._clone_sequences(source_connector=source_connector, target_connector=target_connector)
            cls._clone_schema_of_tables(source_connector=source_connector, target_connector=target_connector)

    @classmethod
    def recreate_schema(cls, db: str, schema: str) -> None:
        database_connector = SyncDatabaseConnector(database_dsn=db)
        with database_connector as connector:
            connector.execute(f"DROP SCHEMA {schema} CASCADE")
            connector.execute(f"CREATE SCHEMA {schema}")

    @classmethod
    def _clone_user_types(
        cls, *, source_connector: SyncDatabaseConnector, target_connector: SyncDatabaseConnector
    ) -> None:
        """
        Clone data types (from pg_type) with following values (all conditions must be met):
         - type (typtype column) are equal to one of: domain (d), enum type (e);
         - namespace is not equal to: pg_catalog, information_schema.
        (see https://www.postgresql.org/docs/current/catalog-pg-type.html)
        """
        select_query = f"""
        SELECT n.nspname AS schema_name,
               t.typname AS type_name,
               t.typtype,
               CASE WHEN t.typtype = 'd' THEN
                   pg_catalog.format_type(t.typbasetype, NULL)
               ELSE NULL END AS data_type,
               CASE WHEN t.typtype = 'e' THEN
                   string_agg('''' || e.enumlabel || '''', ', ')
               ELSE NULL END AS enum_labels,
               CASE WHEN t.typtype = 'd' THEN
                   pg_get_constraintdef(c.oid)
               ELSE NULL END AS constraint_defs
        FROM pg_type t
        LEFT JOIN pg_namespace n ON n.oid = t.typnamespace
        LEFT JOIN pg_enum e ON t.oid = e.enumtypid
        LEFT JOIN pg_constraint c ON t.oid = c.contypid
        WHERE n.nspname NOT IN ({settings.EXCLUDED_SCHEMAS})
        AND t.typtype IN ('d', 'e')
        GROUP BY n.nspname, t.typname, t.typtype, t.typbasetype, c.oid
        """
        user_types = source_connector.execute(query=select_query).fetchall()

        for schema_name, type_name, type_kind, data_type, enum_labels, constraint_defs in user_types:
            if type_kind == "d":  # domains
                create_domain_query = f"CREATE DOMAIN {schema_name}.{type_name} AS {data_type}"
                if constraint_defs:
                    create_domain_query += f" {constraint_defs}"
                target_connector.execute(query=create_domain_query)
            elif type_kind == "e":  # enumerations
                if enum_labels:
                    create_enum_query = f"CREATE TYPE {schema_name}.{type_name} AS ENUM ({enum_labels})"
                    target_connector.execute(query=create_enum_query)
            else:
                raise ValueError(f"Unknown type kind: {type_kind}")

    @classmethod
    def _clone_extensions(
        cls, *, source_connector: SyncDatabaseConnector, target_connector: SyncDatabaseConnector
    ) -> None:
        """Clone all extensions"""
        select_extensions_query = "SELECT extname FROM pg_extension"
        extensions = source_connector.execute(query=select_extensions_query).fetchall()
        for extension in extensions:
            create_extension_query = f"CREATE EXTENSION IF NOT EXISTS {extension[0]}"
            target_connector.execute(query=create_extension_query)

    @classmethod
    def _clone_schema_of_tables(
        cls, *, source_connector: SyncDatabaseConnector, target_connector: SyncDatabaseConnector
    ) -> None:
        """Clone schema of tables without non-identity sequences and non-pk constraints"""
        metadata = get_reflected_metadata(database_connector=source_connector)

        for table in metadata.tables.values():
            # delete non-pk constraints from metadata:
            table.constraints.clear()
            table.indexes.clear()
            table.foreign_keys.clear()
            table.foreign_key_constraints.clear()
            # delete non-identity sequences from metadata
            if table.autoincrement_column is not None and not table.autoincrement_column.server_default.is_identity:
                table.primary_key._autoincrement_column = None
                for column in table.columns:
                    column.autoincrement = False
                    column.server_default = None

        metadata.create_all(bind=target_connector.connection)

    @classmethod
    def _clone_sequences(
        cls, *, source_connector: SyncDatabaseConnector, target_connector: SyncDatabaseConnector
    ) -> None:
        """Clone all sequences"""

        # get sequences information
        select_sequences_query = (
            "SELECT sequencename, start_value, min_value, max_value, increment_by, last_value FROM pg_sequences"
        )
        sequences_data: sa.Sequence[sa.Row] = source_connector.execute(query=select_sequences_query).fetchall()
        sequences: dict[str, Sequence] = {seq_data[0]: Sequence(*seq_data) for seq_data in sequences_data}
        select_seq_table_col_query = """
        SELECT s.relname, t.relname, a.attname
        FROM pg_class s
            JOIN pg_depend d ON d.objid=s.oid AND d.classid='pg_class'::regclass AND d.refclassid='pg_class'::regclass
            JOIN pg_class t ON t.oid=d.refobjid
            JOIN pg_namespace n ON n.oid=t.relnamespace
            JOIN pg_attribute a ON a.attrelid=t.oid AND a.attnum=d.refobjsubid
            WHERE s.relkind='S' and d.deptype='a'
        """
        seq_table_col_data: sa.Sequence[sa.Row] | None = source_connector.execute(
            query=select_seq_table_col_query
        ).fetchall()

        for seq_name, table_name, column_name in seq_table_col_data:
            sequences[seq_name].table_name = table_name
            sequences[seq_name].column_name = column_name

        # paste sequences to new db
        for sequence in sequences.values():
            create_sequence_query = f"""
                CREATE SEQUENCE IF NOT EXISTS {sequence.name}
                INCREMENT BY {sequence.increment_by}
                MINVALUE {sequence.min_val} MAXVALUE {sequence.max_val}
                START WITH {sequence.start_val}
                OWNED BY {sequence.table_name + "." + sequence.column_name if sequence.table_name else "NONE"};
            """
            target_connector.execute(query=create_sequence_query)
            if sequence.last_val:
                setval_query = f"SELECT setval('{sequence.name}', {sequence.last_val})"
                target_connector.execute(query=setval_query)

    CONSTRAINT_TYPES = {sa.PrimaryKeyConstraint: "PRIMARY KEY", sa.UniqueConstraint: ""}

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
