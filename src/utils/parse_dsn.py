def parse_dsn(dsn: str) -> tuple[str, str, str, str, str]:
    _, rest = dsn.split("://", 1)
    username, rest = rest.split(":", 1)
    password, rest = rest.split("@", 1)
    host, rest = rest.split(":", 1)
    port, db_name = rest.split("/", 1)
    return username, password, host, port, db_name
