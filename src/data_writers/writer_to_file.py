class DataWriterToFile:
    """
    THIS IS A FUTURE CLASS
    A writer that writes data to a file
    It can have complex logic: for example, it can extract and write only Primary Key data,
    and the recording format can be arbitrarily complex.
    At the same time, it can have memory: for example, record the amount of data for each table.
    """

    def __init__(self, source_db_dsn: str, target_db_dsn: str): ...

    def write_data(self, *args, **kwargs): ...

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
