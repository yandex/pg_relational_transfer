from dataclasses import dataclass


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
