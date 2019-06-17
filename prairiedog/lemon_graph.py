import os
import logging
import typing

import LemonGraph

import prairiedog.graph
import prairiedog.config
from prairiedog.edge import Edge
from prairiedog.node import Node

log = logging.getLogger("prairiedog")

DB_PATH = '{}/pangenome.lemongraph'.format(prairiedog.config.OUTPUT_DIRECTORY)


class LGGraph(prairiedog.graph.Graph):
    """
    LemonGraph defines directed edges.
    """

    def __init__(self, db_path: str = None, delete_on_exit=False):
        log.debug("Creating LemonGraph with backing file {}".format(
            db_path if db_path is not None else DB_PATH
        ))
        if db_path is not None:
            self.g = LemonGraph.Graph(db_path)
        else:
            os.makedirs(prairiedog.config.OUTPUT_DIRECTORY, exist_ok=True)
            self.g = LemonGraph.Graph(DB_PATH)
        self._ctx = None
        self._txn = None
        self.delete_on_exit = delete_on_exit

    def __del__(self):
        """
        Deletes the database file on garbage collection
        :return:
        """
        if self.delete_on_exit:
            log.debug("Wiping LemonGraph")
            self.clear()

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

    def upsert_node(self, node: Node):
        n = self.txn.node(type=node.node_type, value=node.value)

        if node.labels is not None:
            for k, v in node.labels.items():
                n[k] = v
        pass

    def add_edge(self, edge: Edge):
        na = self.txn.node(type='n', value=edge.src)
        nb = self.txn.node(type='n', value=edge.tgt)

        # Add the edge
        e = self.txn.edge(src=na, tgt=nb, type=edge.edge_type,
                          value=edge.edge_value)

        if edge.incr is not None:
            e['incr'] = edge.incr

        if edge.labels is not None:
            for k, v in edge.labels.items():
                e[k] = v

    def clear(self):
        self.g.delete()

    @property
    def nodes(self) -> typing.Set[Node]:
        nodes = set(self.txn.nodes())
        r_nodes = []
        for node in nodes:
            # Create a dictionary to store other labels
            labels = {}
            for k, v in node.items():
                if k not in ('value', 'type', 'ID'):
                    labels[k] = v
            labels = None if not labels else labels
            r_nodes.append(
                Node(value=node['value'], node_type=node['type'],
                     db_id=node['ID'],labels=labels)
            )
        return set(r_nodes)

    @property
    def edges(self) -> typing.Set[Edge]:
        return set(self.txn.edges())

    def get_labels(self, node: str) -> dict:
        return dict(self.txn.nodes()[node])

    def save(self, f):
        self.ctx.__exit__(None, None, None)
        self._ctx = None
        self._txn = None

    @property
    def edgelist(self) -> typing.Generator:
        raise NotImplementedError

    def set_graph_labels(self, labels: dict):
        for k, v in labels.items():
            self.txn[k] = v

    def filter(self):
        pass

    def __len__(self):
        return -1

    def connected(self, node_a: str, node_b: str) -> typing.Tuple[
                    bool, typing.Tuple]:
        # Gather edges from these nodes and only return the edges
        edges_a = tuple(self.txn.query('@n(value="{}")->e()'.format(
            node_a)))
        if len(edges_a) == 0:
            return False, ()
        edges_b = tuple(self.txn.query('@n(value="{}")->e()'.format(
            node_b)))
        if len(edges_b) == 0:
            return False, ()

        # Unravel theses edge tuples; this should return a tuple of
        # dictionaries
        edges_a = (e[0] for e in edges_a)
        edges_b = (e[0] for e in edges_b)

        # Convert these to Edge objects
        edges_a = (
            Edge(
                src=e['srcID'],
                tgt=e['tgtID'],
                edge_type=e['type'],
                edge_value=e['value'],
                incr=e['incr']
            )
            for e in edges_a
        )
        edges_b = (
            Edge(
                src=e['srcID'],
                tgt=e['tgtID'],
                edge_type=e['type'],
                edge_value=e['value'],
                incr=e['incr']
            )
            for e in edges_b
        )

        # If matched, the src edges are where we should start from to find a
        # path
        matched, src_edges = LGGraph.matching_edges(tuple(edges_a),
                                                    tuple(edges_b))
        if matched is not True:
            return False, ()
        else:
            return True, src_edges

    def path(self, node_a: str, node_b: str) -> tuple:
        pass
