import logging
import typing

import pydgraph

from prairiedog.edge import Edge
from prairiedog.graph import Graph
from prairiedog.node import Node

log = logging.getLogger("prairiedog")

DGRAPH_URL = 'localhost:9080'
SCHEME = ''


class Dgraph(Graph):

    def __init__(self):
        self.client_stub = pydgraph.DgraphClientStub(DGRAPH_URL)
        self.client = pydgraph.DgraphClient(self.client_stub)
        self._txn = None

    @property
    def txn(self):
        if self._txn is None:
            self._txn = self.client.txn()
            return self._txn
        else:
            return self._txn

    def upsert_node(self, node: Node, echo: bool = True) -> typing.Optional[
            Node]:
        pass

    def add_edge(self, edge: Edge, echo: bool = True) -> typing.Optional[Edge]:
        nquads = """
        _:{src} <km> "{src}" .
        _:{tgt} <km> "{tgt}" .
        _:{src} <e> _:{tgt} (type={type}, value={value}) .
        """.format(src=edge.src, tgt=edge.tgt, type=edge.edge_type,
                   value=edge.edge_value)
        self.txn.mutate(set_nquads=nquads)

    def clear(self):
        op = pydgraph.Operation(drop_all=True)
        self.client.alter(op)

    @property
    def nodes(self) -> typing.Set[Node]:
        pass

    @property
    def edges(self) -> typing.Set[Edge]:
        pass

    def get_labels(self, node: str) -> dict:
        pass

    def save(self, f: str = None):
        self.txn.commit()
        self._txn = None

    @property
    def edgelist(self) -> typing.Generator:
        pass

    def set_graph_labels(self, labels: dict):
        pass

    def filter(self):
        pass

    def __len__(self):
        pass

    def connected(self, node_a: str, node_b: str) -> typing.Tuple[
        bool, typing.Tuple]:
        pass

    def path(self, node_a: str, node_b: str) -> tuple:
        pass
