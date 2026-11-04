from asyncpg import Connection

from src.database.connectors.async_connector import AsyncDatabaseConnector


async def get_authors_ids(connection: Connection) -> list[int]:
    query = "SELECT author_id FROM authors"
    query_result = await AsyncDatabaseConnector.execute(connection=connection, query=query)
    return [row["author_id"] for row in query_result]


async def get_book_series_ids(connection: Connection) -> list[int]:
    query = "SELECT series_id FROM book_series"
    query_result = await AsyncDatabaseConnector.execute(connection=connection, query=query)
    return [row["series_id"] for row in query_result]


async def get_books_ids(connection: Connection) -> list[int]:
    query = "SELECT book_id FROM books"
    query_result = await AsyncDatabaseConnector.execute(connection=connection, query=query)
    return [row["book_id"] for row in query_result]


async def get_readers_ids(connection: Connection) -> list[int]:
    query = "SELECT reader_id FROM readers"
    query_result = await AsyncDatabaseConnector.execute(connection=connection, query=query)
    return [row["reader_id"] for row in query_result]


async def get_rentals_ids(connection: Connection) -> list[tuple[int, int]]:
    query = "SELECT book_id, reader_id FROM rentals"
    query_result = await AsyncDatabaseConnector.execute(connection=connection, query=query)
    return [(row["book_id"], row["reader_id"]) for row in query_result]


async def get_author_details_ids(connection: Connection) -> list[int]:
    query = "SELECT author_id FROM author_details"
    query_result = await AsyncDatabaseConnector.execute(connection=connection, query=query)
    return [row["author_id"] for row in query_result]
