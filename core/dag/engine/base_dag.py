from abc import ABC, abstractmethod


class BaseDag(ABC):

    @abstractmethod
    def list_nodes(self) -> set:
        """Return all nodes"""

    @abstractmethod
    def depend_nodes(self, node) -> set:
        """Gets the upper set of nodes on which the specified node depends"""


class Dag(BaseDag):

    def __init__(self, graph, **kwargs):
        self.graph = graph
        self.kwargs = kwargs

    def list_nodes(self):
        return set(self.graph)

    def depend_nodes(self, node):
        return {k for k, v in self.graph.items() if node in v}

    def depend_nodes_with_all(self, node, all_depend_nodes):
        for k, v in self.graph.items():
            if node in v:
                all_depend_nodes.add(k)
                self.depend_nodes_with_all(k, all_depend_nodes)
        return all_depend_nodes

    def nearest_scheduler_nodes(self, scheduled_nodes):
        nearest_enable_scheduler_nodes = set()
        list_nodes = self.list_nodes()
        wait_scheduler_nodes = list_nodes - scheduled_nodes
        for ws in wait_scheduler_nodes:
            depend_nodes_with_all = self.depend_nodes_with_all(ws, set())
            if not depend_nodes_with_all - scheduled_nodes:
                nearest_enable_scheduler_nodes.add(ws)
        return nearest_enable_scheduler_nodes
