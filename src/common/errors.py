class TableNotFoundError(Exception):
    def __init__(self, table_name: str):
        super().__init__(f"Failed to find table '{table_name}' in the database")
