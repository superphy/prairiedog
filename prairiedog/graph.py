import abc


class Graph(metaclass=abc.ABCMeta):
    """We expect this to be a directed graph.
    """
    @abc.abstractmethod
    def upsert_node(self, node: str, labels: dict = None):
        """
        Upsert nodes with labels.
        :param node:
        :param labels:
        :return:
        """
        pass

    @abc.abstractmethod
    def add_edge(self, node_a: str, node_b: str):
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
