from typing import TypeVar


Ttable = TypeVar("Ttable")
Tfield = TypeVar("Tfield")


class TableTree[Ttable, Tfield]:
    def __init__(self, table: Ttable | None, edges: list["TableTreeEdge[Ttable, Tfield]"] | None = None):
        self.table = table
        self.edges = edges if edges is not None else []


class TableTreeEdge[Ttable, Tfield]:
    def __init__(
        self,
        source_node: TableTree[Ttable, Tfield],
        target_node: TableTree[Ttable, Tfield],
        source_key: tuple[Tfield, ...],
        target_key: tuple[Tfield, ...],
        where_clause: str | None = None,
    ):
        self.source_node = source_node
        self.target_node = target_node
        self.source_key = self._key_tuple_to_str(source_key)
        self.target_key = self._key_tuple_to_str(target_key)
        self.where_clause = where_clause

    @staticmethod
    def _key_tuple_to_str(tuple_key) -> str:
        return "(" + ", ".join(tuple_key) + ")"

    @property
    def source_table(self) -> str | None:
        return self.source_node.table

    @property
    def target_table(self) -> str | None:
        return self.target_node.table
