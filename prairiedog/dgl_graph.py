import logging
import typing
import pickle

import dgl

import prairiedog.graph

log = logging.getLogger("prairiedog")

class DGLGraph(prairiedog.graph.Graph):
    def __init__(self):
        self.g = dgl.DGLGraph()

    def upsert_node(self, node: int, labels: dict = None):
        pass

    def add_edge(self, node_a: int, node_b: int):
        pass

    def clear(self):
        self.g.clear()

    @property
    def nodes(self) -> set:
        return set(self.g.nodes)

    @property
    def edges(self) -> set:
        return set(self.g.edges)

    def get_labels(self, node: str) -> dict:
        return self.g.nodes[node].data

    def save(self, f: str):
        pickle.dump(
            self.g,
            open('f', 'wb')
        )

    @property
    def edgelist(self) -> typing.Generator:
        return self.g.edges

    def set_graph_labels(self, labels: dict):
        for k, v in labels:
            self.g.ndata[k] = v

    def __len__(self):
        return len(self.g)

    def filter(self):
        pass

    def update_edge_label(self, src: int, dst: int, key: str, value: str):
        self.g.edges[src, dst] = value
