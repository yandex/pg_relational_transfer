import enum


class IsolationLevel(enum.StrEnum):
    SERIALIZABLE = "serializable"
    REPEATABLE_READ = "repeatable_read"
    READ_COMMITTED = "read_committed"


class WalkerVersion(enum.StrEnum):
    DATA_GRAPH_WALKER = "data_graph_walker"
    TABLE_TREE_WALKER = "table_tree_walker"


class WriterVersion(enum.StrEnum):
    TO_FILE = "to_file"
    VIA_FDW_SINGLE = "via_fdw_single"
    VIA_FDW_BATCH = "via_fdw_batch"


class TraversalRuleTypes(enum.StrEnum):
    NO_ENTER = "no_enter"
    NO_EXIT = "no_exit"
    LIMIT_DISTANCE = "limit_distance"
    ADD_EDGE = "add_edge"
    ADD_EDGES = "add_edges"
    ADD_NODE = "add_node"


class RuleLoaderTypes(enum.StrEnum):
    INCLUDING = "including"
    EXCLUDING = "excluding"
