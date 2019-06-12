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
        self.ctx = self.g.transaction(write=True)
        self.txn = self.ctx.__enter__()

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
