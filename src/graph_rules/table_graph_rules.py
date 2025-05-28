from src.graphs.table_graph import TableGraph


class TableGraphRule:
    def __init__(self, table: str, **_):
        self._table = table

    def update_graph(self, graph: TableGraph) -> TableGraph:
        raise NotImplementedError


class NoEnterTableGraphRule(TableGraphRule):
    def update_graph(self, graph: TableGraph) -> TableGraph:
        inv_graph = graph.get_inverse()
        if self._table in inv_graph:
            del inv_graph[self._table]
        return inv_graph.get_inverse()


class NoExitTableGraphRule(TableGraphRule):
    def update_graph(self, graph: TableGraph) -> TableGraph:
        if self._table in graph:
            del graph[self._table]
        return graph


class LimitDistanceTableGraphRule(TableGraphRule):
    def __init__(self, table: str, max_distance: int = 1, **_):
        self.max_distance = max_distance
        super().__init__(table, **_)

    def update_graph(self, graph: TableGraph) -> TableGraph:
        limited_graph = TableGraph()

        edges = []
        for edge in graph[self._table]:
            edges.append((edge, 1))

        while edges:
            edge, distance = edges.pop()
            if distance <= self.max_distance:
                limited_graph.add_edge(edge)

        return limited_graph
