import logging
import typing

import dill
import torch as th
import numpy as np
from torch_geometric.data import Data

import prairiedog.graph

log = logging.getLogger("prairiedog")


class TGGraph(prairiedog.graph.Graph):
    def __init__(self):
        self.y = {}
        self.edge_list_a = []
        self.edge_list_b = []

    def upsert_node(self, node: str, labels: dict = None,
                    label: th.tensor = None):
        if label is not None:
            self.y[node] = label
        else:
            raise NotImplemented

    def add_edge(self, node_a: str, node_b: str):
        self.edge_list_a.append(node_a)
        self.edge_list_b.append(node_b)

    def clear(self):
        pass

    @property
    def nodes(self) -> set:
        return set([i for i, _ in enumerate(self.y)])

    @property
    def edges(self) -> set:
        return set(
            (self.edge_list_a[i], self.edge_list_b[i])
            for i in range(0, len(self.edge_list_a))
        )

    def get_labels(self, node: str) -> dict:
        return {'y': self.y[int(node)]}

    @staticmethod
    def _sorted_values(d: dict) -> list:
        result = [d[key] for key in sorted(d.keys(), reverse=False)]
        return result

    def save(self, f):
        log.info("Converting to a torch_geometric.data")
        data = Data(
            edge_index=th.tensor(
                [self.edge_list_a, self.edge_list_b], dtype=th.long),
            y=th.from_numpy(
                np.array(TGGraph._sorted_values(self.y))
            ).to(th.long)
        )
        log.info("Writing graph out with name {}".format(f))
        dill.dump(data, open(f, 'wb'), protocol=4)

    @property
    def edgelist(self) -> typing.Generator:
        def gen():
            for edge in self.edges:
                yield edge
        return gen()

    def set_graph_labels(self, labels: dict):
        pass

    def filter(self):
        pass

    def __len__(self):
        return len(self.edge_list_a)
