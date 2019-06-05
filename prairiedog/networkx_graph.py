import logging
import typing

import networkx as nx

import prairiedog.graph

log = logging.getLogger("prairiedog")


class NetworkXGraph(prairiedog.graph.Graph):
    def __init__(self):
        self.g = nx.Graph()

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
        log.info("Writing graph out with name {}".format(f))
        nx.write_gpickle(self.g, f)

    @property
    def edgelist(self) -> typing.Generator:
        return nx.generate_edgelist(self.g, data=False)

    def set_graph_labels(self, labels: dict):
        for k, v in labels.items():
            self.g.graph[k] = v

    def _filter_degree(self, bottom_percentile=0.9):
        # TODO: we should create a graph of this
        max_degrees = max([
            v
            for _, v in self.g.degree()
        ])
        log.debug("Found max degrees to be {}".format(max_degrees))
        bottom_degrees = int(bottom_percentile*max_degrees)
        log.debug("Removing nodes with degree <= {}".format(
            bottom_degrees))
        remove = [
            node
            for node, degree in self.g.degree()
            if degree <= bottom_degrees]
        self.g.remove_nodes_from(remove)

    def _filter_isolates(self):
        log.debug("Filtering isolates")
        self.g.remove_nodes_from(list(nx.isolates(self.g)))

    def filter(self):
        self._filter_degree()
        self._filter_isolates()

    def __len__(self):
        return len(self.g)

    def update_edge_label(self, src: int, dst: int, key: str, value: str):
        prev = self.g.edges[src, dst][key]
        if isinstance(prev, list):
            prev.append(value)
        else:
            self.g.edges[src, dst][key] = value
        return self.g.edges[src, dst][key]
