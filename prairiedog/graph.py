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
    def path(self, node_a: str, node_b: str) -> typing.Tuple[tuple, tuple]:
        pass

    @staticmethod
    def matching_edges(src_edges: typing.Tuple[Edge],
                       tgt_edges: typing.Tuple[Edge]) -> typing.Tuple[
            bool, typing.Tuple[Edge]]:

        # Set of src edges from which you can find a target edge
        st = set()

        for src_edge in src_edges:
            for tgt_edge in tgt_edges:
                # Check if these are on the same contig
                if src_edge.edge_type == tgt_edge.edge_type:
                    # Check if the tgt_edge is upstream (or the same) as the
                    # src edge
                    if src_edge.edge_value <= tgt_edge.edge_value:
                        st.add(src_edge)

        connected = True if len(st) > 0 else False

        if not connected:
            log.warning("No connected edges found")
        else:
            log.debug("Found connecting edge(s):")
            for src_edge in st:
                log.debug(src_edge)
        return connected, tuple(st)
