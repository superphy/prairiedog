import os
import logging
import typing

import LemonGraph

import prairiedog.graph
import prairiedog.config
from prairiedog.edge import Edge
from prairiedog.node import Node
from prairiedog.errors import GraphException


log = logging.getLogger("prairiedog")

DB_PATH = '{}/pangenome.lemongraph'.format(prairiedog.config.OUTPUT_DIRECTORY)


class LGGraph(prairiedog.graph.Graph):
    """
    LemonGraph defines directed edges.
    """

    def __init__(self, db_path: str = None, delete_on_exit=False):
        if db_path is not None:
            self.db_path = db_path
        else:
            os.makedirs(prairiedog.config.OUTPUT_DIRECTORY, exist_ok=True)
            self.db_path = DB_PATH
        log.debug("Creating LemonGraph with backing file {}".format(
            self.db_path))
        self.g = LemonGraph.Graph(self.db_path)
        self._ctx = None
        self._txn = None
        self.delete_on_exit = delete_on_exit

    def __del__(self):
        """
        Deletes the database file on garbage collection
        :return:
        """
        if self.delete_on_exit:
            log.debug("Wiping LemonGraph with backing file {}".format(
                self.db_path))
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

    @staticmethod
    def _parse_node(node: dict) -> Node:
        """
        Parses the node returned by LemonGraph into our Node class
        :param node:
        :return:
        """
        # Create a dictionary to store other labels
        labels = {}
        for k, v in node.items():
            if k not in ('value', 'type', 'ID'):
                labels[k] = v
        labels = None if not labels else labels
        return Node(value=node['value'], node_type=node['type'],
                    db_id=node['ID'], labels=labels)

    @staticmethod
    def _parse_edge(edge: dict) -> Edge:
        """
        Parses the edge returned by LemonGraph into our Edge class
        :param edge:
        :return:
        """
        # Create a dictionary to store other labels
        labels = {}
        for k, v in edge.items():
            if k not in ('value', 'type', 'ID', 'srcID', 'tgtID'):
                labels[k] = v
        labels = None if not labels else labels
        return Edge(src=edge['srcID'], tgt=edge['tgtID'],
                    edge_type=edge['type'], edge_value=edge['value'],
                    labels=labels, db_id=edge['ID'])

    def upsert_node(self, node: Node) -> Node:
        n = self.txn.node(type=node.node_type, value=node.value)

        if node.labels is not None:
            for k, v in node.labels.items():
                n[k] = v
        return LGGraph._parse_node(n)

    def add_edge(self, edge: Edge) -> Edge:
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

        return LGGraph._parse_edge(e)

    def clear(self):
        self.g.delete()

    @property
    def nodes(self) -> typing.Set[Node]:
        nodes = set(self.txn.nodes())
        r_nodes = []
        for node in nodes:
            r_nodes.append(LGGraph._parse_node(node))
        return set(r_nodes)

    @property
    def edges(self) -> typing.Set[Edge]:
        edges = set(self.txn.edges())
        r_edges = []
        for edge in edges:
            r_edges.append(LGGraph._parse_edge(edge))
        return set(r_edges)

    def get_labels(self, node: str) -> dict:
        return dict(self.txn.nodes()[node])

    def save(self, f=None):
        self.ctx.__exit__(None, None, None)
        self._ctx = None
        self._txn = None

    def new_txn(self, write=True):
        self.ctx.__exit__(None, None, None)
        self._ctx = self.g.transaction(write=write)
        self._txn = self.ctx.__enter__()

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
        with self.g.transaction(write=False) as txn:
            # Gather edges from these nodes and only return the edges
            query_a = '@n(value="{}")->e()'.format(node_a)
            edges_a = tuple(txn.query(query_a))
            if len(edges_a) == 0:
                log.warning("No edges found for query {}".format(query_a))
                return False, ()
            query_b = 'e()->@n(value="{}")'.format(node_b)
            edges_b = tuple(txn.query(query_b))
            if len(edges_b) == 0:
                log.warning("No edges found for query {}".format(query_b))
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

            # If matched, the src edges are where we should start from to find
            # a path
            matched, src_edges = LGGraph.matching_edges(tuple(edges_a),
                                                        tuple(edges_b))
            if matched is not True:
                return False, ()
            else:
                return True, src_edges

    def _find_path(self, edge_a: Edge, edge_b: Edge) -> typing.Tuple[Node]:
        query = 'n()'
        i = edge_a.incr
        # This will only add 1 edge if edge_a.incr == edge_b.incr
        while i <= edge_b.incr:
            query += '->@e(type="{}",value="{}",incr="{}")->n()'.format(
                edge_a.edge_type, edge_a.edge_value, i
            )
            i += 1
        log.debug("Using query {}".format(query))

        with self.g.transaction(write=False) as txn:
            chains = tuple(txn.query(query))
            raise GraphException(g=self)

    def path(self, node_a: str, node_b: str) -> tuple:
        connected, src_edges = self.connected(node_a, node_b)
        if not connected:
            return ()
        # Iterate through the possible paths; can be 1 or more.
        for src_edge in src_edges:
            log.info("Finding path between {} and {} with source edge {}"
                     "".format(node_a, node_b, src_edge))
            with self.g.transaction(write=False) as txn:
                # Find the last edge we're looking for.
                query = 'e(type="{}",value="{}")->n(value="{}")'.format(
                    src_edge.edge_type, src_edge.edge_value, node_b)
                tgt_edges = tuple(txn.query(query))

                # There should be at least one connection, but can be more
                # if there are repeats.
                if len(tgt_edges) == 0:
                    raise GraphException(g=self)

                log.info("Checking for {} target edges".format(len(tgt_edges)))
                for tgt_edge in tgt_edges:
                    log.info("Checking for target edge {}".format(tgt_edge))
                    path = self._find_path(src_edge, tgt_edge)
