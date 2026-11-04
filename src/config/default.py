from dataclasses import dataclass
from os import environ


@dataclass
class Settings:
    ENV = environ.get("ENV", "local")

    SOURCE_SCHEMA = environ.get("SOURCE_SCHEMA", "public")
    TARGET_SCHEMA = environ.get("TARGET_SCHEMA", "public")
    REMOTE_SCHEMA = environ.get("REMOTE_SCHEMA", "remote")

    CONNECTION_POOL_SIZE = int(environ.get("CONNECTION_POOL_SIZE", "5"))

    STREAM_LOG_LEVEL = environ.get("STREAM_LOG_LEVEL", "INFO")
    QUERIES_LOG_FILENAME = environ.get("QUERIES_LOG_FILENAME", "queries_log.txt")
    WRITER_TO_FILE_LOG_FILENAME = environ.get("WRITER_TO_FILE_LOG_FILENAME", "writer_to_file_log.txt")

    OVERRIDE_REMOTE_HOST = environ.get("OVERRIDE_REMOTE_HOST")
    OVERRIDE_REMOTE_PORT = environ.get("OVERRIDE_REMOTE_PORT")

    SOURCE_DB_PASSWORD = environ.get("SOURCE_DB_PASSWORD")
    TARGET_DB_PASSWORD = environ.get("TARGET_DB_PASSWORD")
    DB_PASSWORD = environ.get("DB_PASSWORD")
