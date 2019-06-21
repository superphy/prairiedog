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

DB_PATH = os.path.join(
    prairiedog.config.OUTPUT_DIRECTORY,
    'pangenome.lemongraph')


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
        ret = LemonGraph.lib.graph_set_mapsize(self.g._graph, (4 << 30) * 10)
        assert (0 == ret)
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
                    edge_type=edge['type'], edge_value=int(edge['value']),
                    labels=labels, db_id=edge['ID'])

    def upsert_node(self, node: Node, echo: bool = True) -> typing.Optional[
                                                            Node]:
        # TODO: the db will error if you call upsert_node on the same node,
        #  then set the labels differently within the same transaction. Either
        #  we have to use a new txn (which is expensive for txn log) or figure
        #  another work around. Currently, we only add nodes via add_edge()
        #  which works fine for our use case, and upsert_node isn't called.
        n = self.txn.node(type=node.node_type, value=node.value)

        if node.labels is not None:
            for k, v in node.labels.items():
                n[k] = v

        if echo:
            return LGGraph._parse_node(n)

    def add_edge(self, edge: Edge, echo: bool = True) -> typing.Optional[Edge]:
        na = self.txn.node(type='n', value=edge.src)
        nb = self.txn.node(type='n', value=edge.tgt)

        # Add the edge
        e = self.txn.edge(src=na, tgt=nb, type=edge.edge_type,
                          value=str(edge.edge_value))

        if edge.labels is not None:
            for k, v in edge.labels.items():
                e[k] = v

        if echo:
            return LGGraph._parse_edge(e)

    def clear(self):
        self.g.delete()

    @property
    def nodes(self) -> typing.Set[Node]:
        nodes = set(self.txn.nodes())
        r_nodes = []
        for node in nodes:
            log.debug("raw node: {}".format(node))
            r_nodes.append(LGGraph._parse_node(node))
        return set(r_nodes)

    @property
    def edges(self) -> typing.Set[Edge]:
        edges = set(self.txn.edges())
        r_edges = []
        for edge in edges:
            log.debug("raw edge: {}".format(edge))
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
                LGGraph._parse_edge(e)
                for e in edges_a
            )
            edges_b = (
                LGGraph._parse_edge(e)
                for e in edges_b
            )

            # If matched, the src edges are where we should start from to find
            # a path
            matched, src_edges = LGGraph.matching_edges(tuple(edges_a),
                                                        tuple(edges_b))
            if matched is not True:
                return False, ()
            else:
                log.info("Found {} connections between {} and {}".format(
                    len(src_edges), node_a, node_b
                ))
                log.debug("src_edges are {}".format(src_edges))
                return True, src_edges

    def _find_path(self, edge_a: Edge, edge_b: Edge, txn) -> typing.Tuple[
            Node]:
        query = 'n()'
        i = edge_a.edge_value
        log.info("Edge along {} has len {}".format(
            edge_a.edge_type, edge_b.edge_value - edge_a.edge_value))
        # This will only add 1 edge if edge_a.incr == edge_b.incr
        while i <= edge_b.edge_value:
            query += '->@e(type="{}",value="{}")->n()'.format(
                edge_a.edge_type, i
            )
            i += 1
        log.debug("Using query {}".format(query))

        chains = tuple(txn.query(query))
        log.debug("Got chains {}".format(chains))
        # There should only be one chain per path
        assert len(chains) == 1
        # In the nested chain (a tuple), there should be at least 2 nodes
        chain = chains[0]
        assert len(chain) >= 2

        # Convert the chain into a tuple of Nodes and return
        nodes = tuple(self._parse_node(n) for n in chain)
        return nodes

    def path(self, node_a: str, node_b: str) -> typing.Tuple[
            typing.Tuple[typing.Tuple[Node], ...],
            typing.Tuple[typing.Dict[str, typing.Any], ...]]:
        connected, src_edges = self.connected(node_a, node_b)
        if not connected:
            return tuple(), tuple()
        # Iterate through the possible paths; can be 1 or more.
        paths = []
        paths_meta = []
        for src_edge in src_edges:
            log.debug("Finding path between {} and {} with source edge {}"
                      "".format(node_a, node_b, src_edge))
            with self.g.transaction(write=False) as txn:
                # Find the last edge we're looking for.
                query = 'e(type="{}")->@n(value="{}")'.format(
                    src_edge.edge_type, node_b)
                log.debug("Using query {}".format(query))
                tgt_edges = tuple(txn.query(query))
                log.debug("Got tgt_edges {}".format(tgt_edges))
                # Unravel
                tgt_edges = tuple(LGGraph._parse_edge(e[0]) for e in tgt_edges)
                log.debug("tgt_edges after unraveling {}".format(tgt_edges))

                # There should be at least one connection, but can be more
                # if there are repeats.
                if len(tgt_edges) == 0:
                    raise GraphException(g=self)

                log.debug("Checking for {} target edges".format(
                    len(tgt_edges)))
                for tgt_edge in tgt_edges:
                    log.debug("Checking for target edge {}".format(tgt_edge))
                    path_nodes = self._find_path(src_edge, tgt_edge, txn)
                    if len(path_nodes) < 2:
                        raise GraphException(g=self)
                    log.debug("Found path of length {}".format(
                        len(path_nodes)))
                    log.debug("Got path {}".format(path_nodes))

                    # Append
                    paths.append(path_nodes)
                    paths_meta.append(
                        {
                            'edge_type': src_edge.edge_type,
                            **src_edge.labels
                        })
        return tuple(paths), tuple(paths_meta)
