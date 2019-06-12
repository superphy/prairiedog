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

    def upsert_node(self, node: str, labels: dict = None):
        pass

    def add_edge(self, node_a: str, node_b: str, labels: dict = None):
        with self.g.transaction(write=True) as txn:
            na = txn.node(type='km', value=node_a)
            nb = txn.node(type='km', value=node_b)
            log.debug("Created nodes {} {}".format(na, nb))

            edge = txn.edge(src=na, tgt=nb, type='km', value='s')
            log.debug("Created edge {}".format(edge))
            if labels is not None:
                log.debug("Trying to add edge labels {} ...".format(labels))
                for k, v in labels.items():
                    edge[str(k)] = v

        log.debug("Done adding edge")

    def clear(self):
        pass

    @property
    def nodes(self) -> set:
        return set(self.txn.nodes())

    @property
    def edges(self) -> set:
        return set(self.txn.edges())

    def get_labels(self, node: str) -> dict:
        return dict(self.txn.nodes()[node])

    def save(self, f):
        shutil.copy2(DB_PATH, f)

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
