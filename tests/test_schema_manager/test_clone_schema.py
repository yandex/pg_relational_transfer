from asyncpg import Connection

from src.common.enums import IsolationLevel
from src.database.connectors.async_connector import AsyncDatabaseConnector
from src.task_managers.schema_manager import SchemaManager


class TestCloneSchema:
    async def get_extension_names(self, connection: Connection):
        query_result = await AsyncDatabaseConnector.execute(
            connection=connection, query="SELECT extname FROM pg_extension"
        )
        return [row["extname"] for row in query_result]

    async def get_user_table_names(self, connection: Connection):
        query_result = await AsyncDatabaseConnector.execute(
            connection=connection, query="SELECT tablename FROM pg_tables WHERE schemaname = 'public'"
        )
        return [row["tablename"] for row in query_result]

    async def get_user_type_names(self, connection: Connection):
        query = """
        SELECT t.typname AS typname
        FROM pg_type t
        JOIN pg_namespace n ON t.typnamespace = n.oid
        WHERE n.nspname = 'public'
        AND NOT EXISTS (
            SELECT 1
            FROM pg_class c
            WHERE c.relnamespace = t.typnamespace AND c.reltype = t.oid
        )
        AND NOT EXISTS (
            SELECT 1
            FROM pg_depend d
            WHERE d.objid = t.oid AND d.deptype = 'e'
        )
        AND t.typelem = 0
        """
        query_result = await AsyncDatabaseConnector.execute(
            connection=connection,
            query=query,
        )
        return [row["typname"] for row in query_result]

    async def get_constraint_names_without_not_null(self, connection: Connection):
        query = """
        SELECT con.conname AS constraint_name
        FROM pg_constraint AS con
        JOIN pg_class AS t ON con.conrelid = t.oid
        JOIN pg_namespace AS ns ON t.relnamespace = ns.oid
        WHERE ns.nspname = 'public'
        """
        query_result = await AsyncDatabaseConnector.execute(connection=connection, query=query)
        return [row["constraint_name"] for row in query_result]

    async def get_sequence_names(self, connection: Connection):
        query_result = await AsyncDatabaseConnector.execute(
            connection=connection, query="SELECT sequencename FROM pg_sequences"
        )
        return [row["sequencename"] for row in query_result]

    async def get_index_names(self, connection: Connection):
        query = """
        SELECT
            i.relname AS index_name
        FROM
            pg_class AS i
        JOIN
            pg_index AS ix ON i.oid = ix.indexrelid
        JOIN
            pg_class AS t ON t.oid = ix.indrelid
        JOIN
            pg_namespace AS ns ON t.relnamespace = ns.oid
        WHERE
            ix.indisprimary = FALSE
            AND t.relkind IN ('r', 'm')
            AND ns.nspname = 'public'
        """
        query_result = await AsyncDatabaseConnector.execute(connection=connection, query=query)
        return [row["indexname"] for row in query_result]

    async def test_base_scenario(self, source_database, target_database):
        SchemaManager.clone_schema(
            source_db_config=source_database, target_db_config=target_database, schema_name="public"
        )

        target_connector = AsyncDatabaseConnector(database_dsn=target_database.dsn)

        await target_connector.begin(isolation_level=IsolationLevel.READ_COMMITTED)
        async with await target_connector.connect() as target_conn:
            actual_extension_names = await self.get_extension_names(target_conn)
            actual_table_names = await self.get_user_table_names(target_conn)
            actual_type_names = await self.get_user_type_names(target_conn)
            actual_constraint_names = await self.get_constraint_names_without_not_null(target_conn)
            actual_sequence_names = await self.get_sequence_names(target_conn)
            actual_index_names = await self.get_index_names(target_conn)

            assert set(actual_extension_names) == {"plpgsql", "btree_gist"}, actual_extension_names
            assert set(actual_table_names) == {
                "authors",
                "book_series",
                "books",
                "readers",
                "rentals",
                "author_details",
            }, actual_table_names
            assert set(actual_type_names) == {"rental_status"}, actual_type_names
            assert set(actual_constraint_names) == {
                "authors_pkey",
                "book_series_pkey",
                "books_pkey",
                "readers_pkey",
                "rentals_pkey",
                "author_details_pkey",
            }, actual_constraint_names
            assert set(actual_sequence_names) == {
                "authors_seq",
                "book_series_series_id_seq",
                "books_book_id_seq",
                "unused_seq",
            }, actual_sequence_names
            assert set(actual_index_names) == set(), actual_index_names
        await target_connector.close()
