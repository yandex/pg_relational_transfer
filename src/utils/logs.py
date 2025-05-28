from logging.config import dictConfig
import sys

from src.config import settings


LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "standard": {"format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"},
        "only_message": {"format": "%(message)s"},
    },
    "handlers": {
        "stream": {
            "class": "logging.StreamHandler",
            "level": settings.STREAM_LOG_LEVEL,
            "formatter": "standard",
            "stream": sys.stdout,
        },
        "sql_queries": {
            "class": "logging.FileHandler",
            "level": "INFO",
            "formatter": "standard",
            "filename": settings.QUERIES_LOG_FILENAME,
            "mode": "w",
            "encoding": "utf-8",
        },
        "writer_to_file": {
            "class": "logging.FileHandler",
            "level": "INFO",
            "formatter": "only_message",
            "filename": settings.WRITER_TO_FILE_LOG_FILENAME,
            "mode": "w",
            "encoding": "utf-8",
        },
    },
    "loggers": {
        "": {"handlers": ["stream"], "level": settings.STREAM_LOG_LEVEL, "propagate": False},
        "sql_queries": {"handlers": ["sql_queries"], "level": "INFO", "propagate": False},
        "writer_to_file": {"handlers": ["writer_to_file"], "level": "INFO", "propagate": False},
    },
}


def setup_logging():
    dictConfig(LOGGING_CONFIG)
