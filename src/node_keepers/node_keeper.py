from collections import defaultdict
from collections.abc import Iterable

from src.graphs.data_node import DataNode


class NodeIdKeeper:
    def __init__(self, nodes: Iterable[DataNode]):
        self._keeper: dict[str, set[str]] = defaultdict(set)
        for node in nodes:
            self.add(node)

    def __contains__(self, node: DataNode):
        return node.ctid in self._keeper[node.tableoid]

    def add(self, node: DataNode):
        self._keeper[node.tableoid].add(node.ctid)
