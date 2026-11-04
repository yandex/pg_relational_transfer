# pylint: disable=redefined-outer-name
# pylint: disable=unused-argument
from collections.abc import Generator

import pytest
import sqlalchemy as sa
from sqlalchemy_utils import create_database, database_exists, drop_database

from src.config.test import Settings
from src.utils.database_config import DatabaseConfig, create_database_config_from_dsn
from tests.utils.execute_sql_script import (
    execute_create_schema_script,
    execute_insert_data_script,
)


settings = Settings()


@pytest.fixture(scope="session")
def source_database() -> Generator[DatabaseConfig]:
    source_database_dsn = settings.source_database_dsn
    if database_exists(source_database_dsn):
        drop_database(source_database_dsn)
    create_database(source_database_dsn)
    engine = sa.create_engine(source_database_dsn, poolclass=sa.pool.NullPool)
    execute_create_schema_script(engine)
    execute_insert_data_script(engine)
    try:
        yield create_database_config_from_dsn(source_database_dsn)
    finally:
        drop_database(source_database_dsn)


@pytest.fixture
def target_database() -> Generator[DatabaseConfig]:
    target_database_dsn = settings.target_database_dsn
    if database_exists(target_database_dsn):
        drop_database(target_database_dsn)
    create_database(target_database_dsn)
    try:
        yield create_database_config_from_dsn(target_database_dsn)
    finally:
        drop_database(target_database_dsn)


@pytest.fixture(autouse=True)
async def clear_target_database(target_database):
    engine = sa.create_engine(target_database.dsn, poolclass=sa.pool.NullPool)
    with engine.connect() as connection:
        connection.execute(sa.text("DROP SCHEMA public CASCADE"))
        connection.execute(sa.text("CREATE SCHEMA public"))
        connection.commit()
