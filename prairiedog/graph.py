import abc
import logging

log = logging.getLogger(__name__)


class Graph(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def upsert_node(self, node):
        pass

    @abc.abstractmethod
    def add_edge(self, node_a, node_b):
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
