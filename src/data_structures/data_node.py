from dataclasses import dataclass
from typing import TypeVar


Tkey = TypeVar("Tkey")


@dataclass(frozen=True)
class DataNode:
    """
    Node of the data graph.
    It contains:
        - the name of the table to which the node (data) belongs;
        - the tableoid of the table to which the node (data) belongs;
        - the ctid of the data.
    """

    table: str
    ctid: str
    tableoid: str

    def __str__(self):
        return f"({self.table}, {self.ctid}, {self.tableoid})"


@dataclass(frozen=True)
class DataBatch[Tkey]:
    """
    Batch of data nodes.
    It contains:
        - the name of the table to which the batch of nodes (data) belongs;
        - the set of unique keys of the data.
    """

    table: str
    keys: set[Tkey]

    def __str__(self):
        return f"({self.table}, {self.keys})"
