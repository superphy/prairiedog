import abc
import typing


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
    def add_edge(self, node_a: int, node_b: int, labels: dict = None,
                 edge_type: str = 'e', edge_value: str = 'und'):
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
