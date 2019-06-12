import logging
import typing
import shutil

import LemonGraph

import prairiedog.graph

log = logging.getLogger("prairiedog")

DB_PATH = 'pangenome.lemongraph'

class LGGraph(prairiedog.graph.Graph):
    def __init__(self):
        self.g = LemonGraph.Graph(DB_PATH)
        self._ctx = None
        self._txn = None
        
    @property
    def ctx(self):
        if self._ctx is None:
            self._ctx = self.g.transaction(write=True)
        return self._ctx

    @property
    def txn(self):
        if self._txn is None:
            self._txn = self.ctx.__enter__()
        return self._txn

    def upsert_node(self, node: str, labels: dict = None):
        pass

    def add_edge(self, node_a: str, node_b: str, labels: dict = None):
        na = self.txn.node(type='km', value=node_a)
        nb = self.txn.node(type='km', value=node_b)

        if labels is not None:
            for k, v in labels.items():
                edge = self.txn.edge(src=na, tgt=nb, type=k, value=v)
        else:
            edge = self.txn.edge(src=na, tgt=nb, type='s', value='v')

    def clear(self):
        self.g.delete()

    @property
    def nodes(self) -> set:
        return set(self.txn.nodes())

    @property
    def edges(self) -> set:
        return set(self.txn.edges())

    def get_labels(self, node: str) -> dict:
        return dict(self.txn.nodes()[node])

    def save(self, f):
        self.ctx.__exit__(None, None, None)
        # shutil.copy2(DB_PATH, f)
        self._ctx = None
        self._txn = None

    @property
    def edgelist(self) -> typing.Generator:
        raise NotImplemented

    def set_graph_labels(self, labels: dict):
        for k, v in labels.items():
            self.txn[k] = v

    def filter(self):
        pass

    def __len__(self):
        return -1
