from dataclasses import dataclass
from os import environ


@dataclass
class Settings:
    ENV = environ.get("ENV", "local")

    SOURCE_DATABASE_NAME = environ.get("SOURCE_DATABASE_NAME", "source")
    SOURCE_DATABASE_HOST = environ.get("SOURCE_DATABASE_HOST", "localhost")
    SOURCE_DATABASE_PORT = environ.get("SOURCE_DATABASE_PORT", "5432")
    SOURCE_DATABASE_USER = environ.get("SOURCE_DATABASE_USER", "postgres")
    SOURCE_DATABASE_PASSWORD = environ.get("SOURCE_DATABASE_PASSWORD", "password")

    TARGET_DATABASE_NAME = environ.get("TARGET_DATABASE_NAME", "source")
    TARGET_DATABASE_HOST = environ.get("TARGET_DATABASE_HOST", "localhost")
    TARGET_DATABASE_PORT = environ.get("TARGET_DATABASE_PORT", "5432")
    TARGET_DATABASE_USER = environ.get("TARGET_DATABASE_USER", "postgres")
    TARGET_DATABASE_PASSWORD = environ.get("TARGET_DATABASE_PASSWORD", "password")

    SOURCE_SCHEMA = environ.get("SOURCE_SCHEMA", "public")
    TARGET_SCHEMA = environ.get("TARGET_SCHEMA", "public")
    REMOTE_SCHEMA = environ.get("REMOTE_SCHEMA", "remote")

    EXCLUDED_SCHEMAS = environ.get("EXCLUDED_SCHEMAS", "'pg_catalog', 'information_schema'")
    CONNECTION_POOL_SIZE = int(environ.get("CONNECTION_POOL_SIZE", "5"))

    STREAM_LOG_LEVEL = environ.get("STREAM_LOG_LEVEL", "INFO")
    QUERIES_LOG_FILENAME = environ.get("QUERIES_LOG_FILENAME", "queries_log.txt")
    WRITER_TO_FILE_LOG_FILENAME = environ.get("WRITER_TO_FILE_LOG_FILENAME", "writer_to_file_log.txt")

    OVERRIDE_REMOTE_HOST = environ.get("OVERRIDE_REMOTE_HOST")
    OVERRIDE_REMOTE_PORT = environ.get("OVERRIDE_REMOTE_PORT")

    @property
    def source_database_dsn(self):
        return (
            f"postgresql://"
            f"{self.SOURCE_DATABASE_USER}:"
            f"{self.SOURCE_DATABASE_PASSWORD}@"
            f"{self.SOURCE_DATABASE_HOST}:"
            f"{self.SOURCE_DATABASE_PORT}/"
            f"{self.SOURCE_DATABASE_NAME}"
        )

    @property
    def target_database_dsn(self):
        return (
            f"postgresql://"
            f"{self.TARGET_DATABASE_USER}:"
            f"{self.TARGET_DATABASE_PASSWORD}@"
            f"{self.TARGET_DATABASE_HOST}:"
            f"{self.TARGET_DATABASE_PORT}/"
            f"{self.TARGET_DATABASE_NAME}"
        )
