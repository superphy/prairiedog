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
            # Check if node exists
            tup = txn.query('n(value="{}")'.format(node_a))
            if len(tup) != 0:
                # Or something went wrong and we have duplicates
                assert len(tup) == 1
                # Note that the returned node is only valid within a txn
                na = tup[0]
                log.debug("Found existing node {}".format(na))
            else:
                na = txn.node(type='km', value=node_a)
                log.debug("Created node {}".format(na))

            tup = txn.query('n(value="{}")'.format(node_a))
            if len(tup) != 0:
                # Or something went wrong and we have duplicates
                assert len(tup) == 1
                # Note that the returned node is only valid within a txn
                nb = tup[0]
                log.debug("Found existing node {}".format(nb))
            else:
                nb = txn.node(type='km', value=node_a)
                log.debug("Created node {}".format(nb))

            nb = txn.node(type='km', value=node_b)
            log.debug("Created nodes {} {}".format(na, nb))

            if labels is not None:
                log.debug("Trying to add edge labels {} ...".format(labels))
                for k, v in labels.items():
                    tup = txn.query(
                        'n(value="{}")-e(value="{}")-n(value="{}")'.format(
                            node_a, v, node_b
                        ))
                    if len(tup) != 0:
                        log.debug(
                            "Edge {} already exists, skipping...".format(
                                tup
                            ))
                    else:
                        edge = txn.edge(src=na, tgt=nb, type=k, value=v)
                        log.debug("Created edge {}".format(edge))
            else:
                # TODO: add query here
                edge = txn.edge(src=na, tgt=nb, type='s', value='v')
            log.debug("Created edge {}".format(edge))

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
