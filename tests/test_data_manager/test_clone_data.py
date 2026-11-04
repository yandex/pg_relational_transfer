from pathlib import Path

import pytest

from src.common.enums import IsolationLevel, WalkerVersion, WriterVersion
from src.database.connectors.async_connector import AsyncDatabaseConnector
from src.task_managers.data_manager import DataManager
from src.task_managers.schema_manager import SchemaManager
from tests.utils.get_data import (
    get_author_details_ids,
    get_authors_ids,
    get_book_series_ids,
    get_books_ids,
    get_readers_ids,
    get_rentals_ids,
)


PATH_TO_RULES = Path(__file__).resolve().parents[1] / "rules"


class TestCloneData:
    @pytest.mark.parametrize(
        "walker_version,"
        "writer_version,"
        "rule_filename,"
        "expected_authors_ids,"
        "expected_author_details_ids,"
        "expected_books_ids,"
        "expected_book_series_ids,"
        "expected_readers_ids,"
        "expected_rentals_ids",
        [
            (
                WalkerVersion.DATA_GRAPH_WALKER,
                WriterVersion.VIA_FDW_SINGLE,
                "source_authors_1.json",
                [1],
                [1],
                [1, 2],
                [1],
                [1],
                [(1, 1), (2, 1)],
            ),
            (
                WalkerVersion.DATA_GRAPH_WALKER,
                WriterVersion.VIA_FDW_SINGLE,
                "no_exit_table_books.json",
                [1],
                [1],
                [1, 2],
                [],
                [],
                [],
            ),
            (
                WalkerVersion.DATA_GRAPH_WALKER,
                WriterVersion.VIA_FDW_SINGLE,
                "no_enter_table_books.json",
                [1],
                [1],
                [],
                [],
                [],
                [],
            ),
            (
                WalkerVersion.DATA_GRAPH_WALKER,
                WriterVersion.VIA_FDW_SINGLE,
                "limit_distance_authors_2.json",
                [1],
                [1],
                [1, 2],
                [1],
                [],
                [(1, 1), (2, 1)],
            ),
            (
                WalkerVersion.DATA_GRAPH_WALKER,
                WriterVersion.VIA_FDW_SINGLE,
                "no_exit_data_books_1_readers_1.json",
                [1],
                [1],
                [1, 2],
                [1],
                [1],
                [(2, 1)],
            ),
            (
                WalkerVersion.DATA_GRAPH_WALKER,
                WriterVersion.VIA_FDW_SINGLE,
                "no_enter_data_books_1_readers_1.json",
                [1],
                [1],
                [2],
                [1],
                [],
                [(2, 1)],
            ),
            (
                WalkerVersion.TABLE_TREE_WALKER,
                WriterVersion.VIA_FDW_BATCH,
                "tree_authors_book_series.tree",
                [1, 2],
                [],
                [1, 2, 3, 4],
                [1, 2],
                [1, 2],
                [(1, 1), (2, 1), (3, 2), (4, 2)],
            ),
            (
                WalkerVersion.TABLE_TREE_WALKER,
                WriterVersion.VIA_FDW_BATCH,
                "tree_authors_with_where.tree",
                [1],
                [],
                [1],
                [],
                [1],
                [(1, 1)],
            ),
        ],
    )
    async def test_clone_with_rules(
        self,
        source_database,
        target_database,
        walker_version,
        writer_version,
        rule_filename,
        expected_authors_ids,
        expected_author_details_ids,
        expected_books_ids,
        expected_book_series_ids,
        expected_readers_ids,
        expected_rentals_ids,
    ):
        SchemaManager.clone_schema(
            source_db_config=source_database, target_db_config=target_database, schema_name="public"
        )

        data_manager = DataManager(
            source_db_config=source_database,
            target_db_config=target_database,
            walker_version=walker_version,
            writer_version=writer_version,
            rule_path=f"{PATH_TO_RULES}/{rule_filename}",
        )
        await data_manager.clone_data()

        target_connector = AsyncDatabaseConnector(database_dsn=target_database.dsn)
        await target_connector.begin(isolation_level=IsolationLevel.READ_COMMITTED)
        async with await target_connector.connect() as target_conn:
            actual_authors_ids = await get_authors_ids(target_conn)
            actual_book_series_ids = await get_book_series_ids(target_conn)
            actual_books_ids = await get_books_ids(target_conn)
            actual_readers_ids = await get_readers_ids(target_conn)
            actual_rentals_ids = await get_rentals_ids(target_conn)
            actual_author_details_ids = await get_author_details_ids(target_conn)

            assert sorted(actual_authors_ids) == expected_authors_ids
            assert sorted(actual_author_details_ids) == expected_author_details_ids
            assert sorted(actual_books_ids) == expected_books_ids
            assert sorted(actual_book_series_ids) == expected_book_series_ids
            assert sorted(actual_readers_ids) == expected_readers_ids
            assert sorted(actual_rentals_ids) == expected_rentals_ids
        await target_connector.close()
