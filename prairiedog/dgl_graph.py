import logging
import typing
import pickle

import dgl
import torch as th

import prairiedog.graph

log = logging.getLogger("prairiedog")


class DGLGraph(prairiedog.graph.Graph):
    def __init__(self, n_labels: int):
        self.g = dgl.DGLGraph(multigraph=True)
        self.g.set_n_initializer(dgl.init.zero_initializer)
        self.labels = th.nn.functional.one_hot(
            th.arange(0, n_labels)
        )

    def upsert_node(self, node: int, labels: dict = None):
        if labels:
            raise NotImplemented()
        else:
            if self.g.has_node(node):
                pass
            else:
                # Nodes are not added by ID
                self.g.add_nodes(1)

    def add_edge(self, node_a: int, node_b: int, labels: dict = None):
        if labels is not None:
            encoded_labels = {}
            for k, v in labels.items():
                encoded_labels[k] = self.labels[v]
            self.g.add_edge(node_a, node_b, encoded_labels)
        else:
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
