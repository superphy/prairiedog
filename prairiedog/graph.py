import abc
import typing
import logging

from prairiedog.edge import Edge
from prairiedog.node import Node

log = logging.getLogger("prairiedog")


class Graph(metaclass=abc.ABCMeta):
    """We expect this to be a directed graph.
    """

    @abc.abstractmethod
    def upsert_node(self, node: Node, echo: bool = True) -> typing.Optional[
            Node]:
        """
        Upsert nodes with labels.
        :param echo:
        :param node:
        :return:
        """
        pass

    @abc.abstractmethod
    def add_edge(self, edge: Edge, echo: bool = True) -> typing.Optional[Edge]:
        pass

    @abc.abstractmethod
    def clear(self):
        pass

    @property
    @abc.abstractmethod
    def nodes(self) -> typing.Set[Node]:
        pass

    @property
    @abc.abstractmethod
    def edges(self) -> typing.Set[Edge]:
        pass

    @abc.abstractmethod
    def get_labels(self, node: str) -> dict:
        pass

    @abc.abstractmethod
    def save(self, f: str = None):
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
                if d[edge.origin] < edge.edge_value:
                    d[edge.origin] = edge.edge_value
            else:
                d[edge.origin] = edge.edge_value
        return d

    @staticmethod
    def matching_edges(src_edges: typing.Tuple[Edge],
                       tgt_edges: typing.Tuple[Edge]) -> typing.Tuple[
            bool, typing.Tuple[Edge]]:
        src_map = Graph._edge_map(src_edges)
        tgt_map = Graph._edge_map(tgt_edges)

        list_src_edges = []
        for k, v in src_map.items():
            # Check if there's a target edge with the same origin.
            # We accept the case where the value is the same in case its a
            # direct edge to the target node.
            if k in tgt_map and tgt_map[k] >= v:
                # Select the Edge object from src_edges
                for edge in src_edges:
                    # If this edge is the one we're looking for, add it to ret.
                    if edge.origin == k and edge.edge_value == v:
                        list_src_edges.append(edge)

        connected = True if len(list_src_edges) > 0 else False

        if not connected:
            log.warning("No connected edges found for src_map {} and tgt_map"
                        "{}".format(src_map, tgt_map))
        else:
            log.debug("Found connecting edge(s) {} ".format(list_src_edges))
        return connected, tuple(list_src_edges)
