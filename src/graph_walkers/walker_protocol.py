from collections.abc import Callable
from typing import Protocol

import sqlalchemy as sa

from src.graph_rules import GraphRuleManager


class GraphWalkerProtocol(Protocol):
    def __init__(
        self,
        source_db_dsn: str,
        graph_rule_manager: GraphRuleManager,
        data_sending_callback: Callable,
        database_tables: dict[str, sa.Table],
    ): ...

    def start_walk(self) -> None: ...
