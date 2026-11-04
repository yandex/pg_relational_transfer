from os import environ

from src.config.default import Settings as DefaultSettings
from src.utils.database_config import build_dsn


class Settings(DefaultSettings):
    SOURCE_DATABASE_NAME = "pg_relational_transfer_test_source"
    SOURCE_DATABASE_HOST = environ.get("TEST_DB_HOST", "127.0.0.1")
    SOURCE_DATABASE_PORT = environ.get("TEST_DB_PORT", "55432")
    SOURCE_DATABASE_USER = environ.get("TEST_DB_USER", "postgres")
    SOURCE_DATABASE_PASSWORD = environ.get("TEST_DB_PASSWORD", "password")

    TARGET_DATABASE_NAME = "pg_relational_transfer_test_target"
    TARGET_DATABASE_HOST = SOURCE_DATABASE_HOST
    TARGET_DATABASE_PORT = SOURCE_DATABASE_PORT
    TARGET_DATABASE_USER = SOURCE_DATABASE_USER
    TARGET_DATABASE_PASSWORD = SOURCE_DATABASE_PASSWORD

    CONNECTION_POOL_SIZE = int(environ.get("CONNECTION_POOL_SIZE", "1"))

    @property
    def source_database_dsn(self):
        return build_dsn(
            dialect="postgresql",
            driver=None,
            username=self.SOURCE_DATABASE_USER,
            password=self.SOURCE_DATABASE_PASSWORD,
            host=self.SOURCE_DATABASE_HOST,
            port=self.SOURCE_DATABASE_PORT,
            database=self.SOURCE_DATABASE_NAME,
        )

    @property
    def target_database_dsn(self):
        return build_dsn(
            dialect="postgresql",
            driver=None,
            username=self.TARGET_DATABASE_USER,
            password=self.TARGET_DATABASE_PASSWORD,
            host=self.TARGET_DATABASE_HOST,
            port=self.TARGET_DATABASE_PORT,
            database=self.TARGET_DATABASE_NAME,
        )
