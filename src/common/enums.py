import enum


class IsolationLevel(enum.StrEnum):
    SERIALIZABLE = "serializable"
    REPEATABLE_READ = "repeatable_read"
    READ_COMMITTED = "read_committed"


class WalkerVersion(enum.StrEnum):
    TABLE_WALKER = "table_walker"
    DATA_WALKER_SYNC = "data_walker_sync"
    DATA_WALKER_ASYNC = "data_walker_async"


class WriterVersion(enum.StrEnum):
    TO_FILE = "to_file"
    SINGLE_DATA_VIA_FDW_SYNC = "single_data_via_FDW_sync"
    BATCH_OF_DATA_VIA_FDW_SYNC = "batch_of_data_via_FDW_sync"
    VIA_FDW_ASYNC = "via_FDW_async"


class TraversalRuleTypes(enum.StrEnum):
    NO_ENTER = "no_enter"
    NO_EXIT = "no_exit"
    LIMIT_DISTANCE = "limit_distance"
