from dataclasses import dataclass
from urllib.parse import unquote, urlparse


@dataclass
class DatabaseConfig:
    username: str
    password: str | None
    host: str | None
    port: int | str
    database: str

    dialect: str = "postgresql"
    driver: str | None = None

    @property
    def dsn(self) -> str:
        return build_dsn(
            dialect=self.dialect,
            driver=self.driver,
            username=self.username,
            password=self.password,
            host=self.host,
            port=self.port,
            database=self.database,
        )


def create_database_config_from_dsn(dsn: str) -> DatabaseConfig:
    """
    Parses the PostgreSQL connection string (DSN/URI) into components.

    Args:
        dsn: The connection string, for example:
             "postgresql://user:password@host:port/database"

    Returns:
        DatabaseConfig:
        The values can be None if the component is missing from the row.
        The special characters in the username and password will be decoded.
    """
    if not dsn:
        return {}

    result = urlparse(dsn)

    dialect = None
    driver = None
    if result.scheme:
        parts = result.scheme.split("+", 1)
        dialect = parts[0]
        if len(parts) > 1:
            driver = parts[1]

    username = unquote(result.username) if result.username else None
    password = unquote(result.password) if result.password else None
    host = result.hostname
    port = result.port

    database = result.path.lstrip("/") if result.path else None

    return DatabaseConfig(
        dialect=dialect,
        driver=driver,
        username=username,
        password=password,
        host=host,
        port=port,
        database=database,
    )


def build_dsn(
    dialect: str,
    driver: str | None,
    username: str,
    password: str | None,
    host: str | None,
    port: int | str,
    database: str,
) -> str:
    """
    Builds the PostgreSQL connection string (DSN/URI) from components.

    Args:
        dialect: The dialect of the database.
        driver: The driver of the database.
        username: The username of the database.
        password: The password of the database.
        host: The host of the database.
        port: The port of the database.
        database: The database name.

    Returns:
        str: The connection string.
    """
    dialect_plus_driver = f"{dialect}+{driver}" if driver else dialect
    return (
        f"{dialect_plus_driver}://"
        f"{username}"
        f"{':' + password if password else ''}"
        f"{'@' + host if host else ''}"
        f"{':' + str(port) if port else ''}"
        f"/{database}"
    )
