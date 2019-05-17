import logging
import typing

import networkx as nx

import prairiedog.graph

log = logging.getLogger("prairiedog")


class NetworkXGraph(prairiedog.graph.Graph):
    def __init__(self):
        self.g = nx.DiGraph()

    def upsert_node(self, node: str, labels: dict = None):
        if labels and isinstance(labels, dict):
            self.g.add_node(node_for_adding=node, **labels)
        else:
            self.g.add_node(node)

    def add_edge(self, node_a: str, node_b: str):
        self.g.add_edge(node_a, node_b)

    def clear(self):
        self.g.clear()

    @property
    def nodes(self) -> set:
        return set(self.g.nodes)

    @property
    def edges(self) -> set:
        return set(self.g.edges)

    def get_labels(self, node: str) -> dict:
        return dict(self.g.nodes[node])

    def save(self, f):
        log.info("Writing graphs out with prefix {}".format(f))
        nx.write_multiline_adjlist(self.g, f + '_multiline_adjlist.txt')
        nx.write_edgelist(self.g, f + '_edgelist.txt')

    @property
    def edgelist(self) -> typing.Generator:
        return nx.generate_edgelist(self.g, data=False)
