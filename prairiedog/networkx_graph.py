import networkx as nx

import prairiedog.graph


class NetworkXGraph(prairiedog.graph.Graph):
    def __init__(self):
        self.g = nx.Graph()

    def upsert_node(self, node):
        self.g.add_node(node)

    def link(self, node_a, node_b):
        self.g.add_edge(node_a, node_b)

    def clear(self):
        self.g.clear()

    @property
    def nodes(self) -> list:
        return list(self.g.nodes)
