from collections.abc import Callable
from typing import Protocol

from src.data_structures.table_tree import TableTree
from src.graph_rules import GraphRuleManager
from src.utils.database_config import DatabaseConfig


class BaseWalkerProtocol(Protocol):
    async def start_walk(self) -> None: ...


class GraphWalkerProtocol(BaseWalkerProtocol):
    def __init__(
        self,
        source_db_config: DatabaseConfig,
        data_sending_callback: Callable,
        graph_rule_manager: GraphRuleManager,
    ): ...

    async def start_walk(self) -> None: ...


class TreeWalkerProtocol(BaseWalkerProtocol):
    def __init__(
        self,
        source_db_config: DatabaseConfig,
        data_sending_callback: Callable,
        table_tree: TableTree,
    ): ...

    async def start_walk(self) -> None: ...
