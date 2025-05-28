from os import environ

from src.config.default import Settings as DefaultSettings


class Settings(DefaultSettings):
    SOURCE_DATABASE_NAME = environ.get("POSTGRES_RECIPE_DBNAME")
    SOURCE_DATABASE_HOST = environ.get("POSTGRES_RECIPE_HOST")
    SOURCE_DATABASE_PORT = environ.get("POSTGRES_RECIPE_PORT")
    SOURCE_DATABASE_USER = environ.get("POSTGRES_RECIPE_USER")
    SOURCE_DATABASE_PASSWORD = environ.get("POSTGRES_RECIPE_PASSWORD")

    TARGET_DATABASE_NAME = "target"
    TARGET_DATABASE_HOST = environ.get("POSTGRES_RECIPE_HOST")
    TARGET_DATABASE_PORT = environ.get("POSTGRES_RECIPE_PORT")
    TARGET_DATABASE_USER = environ.get("POSTGRES_RECIPE_USER")
    TARGET_DATABASE_PASSWORD = environ.get("POSTGRES_RECIPE_PASSWORD")
