import abc
import typing

from prairiedog.edge import Edge


class Graph(metaclass=abc.ABCMeta):
    """We expect this to be a directed graph.
    """

    @abc.abstractmethod
    def upsert_node(self, node: int, labels: dict = None):
        """
        Upsert nodes with labels.
        :param node:
        :param labels:
        :return:
        """
        pass

    @abc.abstractmethod
    def add_edge(self, edge: Edge):
        pass

    @abc.abstractmethod
    def clear(self):
        pass

    @property
    @abc.abstractmethod
    def nodes(self) -> set:
        pass

    @property
    @abc.abstractmethod
    def edges(self) -> set:
        pass

    @abc.abstractmethod
    def get_labels(self, node: str) -> dict:
        pass

    @abc.abstractmethod
    def save(self, f: str):
        pass

    @property
    @abc.abstractmethod
    def edgelist(self) -> typing.Generator:
        pass

    @abc.abstractmethod
    def set_graph_labels(self, labels: dict):
        pass

    @abc.abstractmethod
    def filter(self):
        pass

    @abc.abstractmethod
    def __len__(self):
        pass

    #########
    # Queries
    #########

    @abc.abstractmethod
    def connected(self, node_a: str, node_b: str) -> typing.Tuple[
                    bool, typing.Tuple]:
        pass

    @abc.abstractmethod
    def path(self, node_a: str, node_b: str) -> tuple:
        pass

    @staticmethod
    def _edge_map(edges: typing.Tuple[Edge]) -> dict:
        d = {}
        for edge in edges:
            # We only keep the edges with the largest ids
            if edge.origin in d:
                if d[edge.origin] < edge.incr:
                    d[edge.origin] = edge.incr
            else:
                d[edge.origin] = edge.incr
        return d

    @staticmethod
    def matching_edges(src_edges: typing.Tuple[Edge],
                       tgt_edges: typing.Tuple[Edge]) -> typing.Tuple[
                        bool, typing.Tuple[Edge]]:
        src_map = Graph._edge_map(src_edges)
        tgt_map = Graph._edge_map(tgt_edges)

        list_edges = []
        for k, v in src_map:
            # Check if there's a target edge with the same origin
            if k in tgt_map and tgt_map[k] > v:
                for edge in src_edges:
                    if edge.origin == k and edge.incr == v:
                        list_edges.append(edge)

        return True if len(list_edges) > 0 else False, tuple(list_edges)
