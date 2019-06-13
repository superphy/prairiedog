import os
import time
import logging

from prairiedog.gref import GRef
from prairiedog.kmers import Kmers
from prairiedog.graph import Graph
from prairiedog.graph_ref import GraphRef

log = logging.getLogger("prairiedog")


class SubgraphRef(GRef):
    """
    Helper for creating a NetworkX graph, created for each genome file.
    """
    def __init__(self, km: Kmers, graph: Graph, gr: GraphRef, target: str):
        """
        """
        self.graph = graph
        self.subgraph_kmer_map = {}
        self.src = km.filepath
        self._create_graph(km, gr, target)

    def __str__(self):
        return "SubgraphRef for Kmer source {}".format(self.src)

    @staticmethod
    def _upsert_map(d: dict, value: str) -> int:
        """
        Override base class upsert as we want node ids to start from 0
        :param d:
        :param value:
        :param prev_value:
        :return:
        """
        if value not in d:
            d[value] = len(d)
        return d[value]

    def _create_graph(self, km: Kmers, gr: GraphRef, target: str) -> int:
        log.debug(
            "Starting to graph {} in pid {}".format(
                km, os.getpid()))
        st = time.time()
        c = 0
        while km.has_next:
            header1, kmer1 = km.next()
            # Create the first node
            node1_id = self._upsert_map(
                self.subgraph_kmer_map, kmer1)
            node1_label = gr.get_node_label(kmer1)
            self.graph.upsert_node(node1_id, labels={
                "feat": node1_label,
            })
            c += 1
            # The same contig still has a kmer
            while km.contig_has_next:
                header2, kmer2 = km.next()
                # Create the second node
                node2_id = self._upsert_map(
                    self.subgraph_kmer_map, kmer2)
                node2_label = gr.get_node_label(kmer2)
                self.graph.upsert_node(node2_id, labels={
                    "feat": node2_label
                })
                # Create an edge
                self.graph.add_edge(node1_id, node2_id)
                # Set node1_id to node2_id
                node1_id = node2_id
                c += 1
            # At this point, we're out of kmers on that contig
            # The loop will check if there's still kmers, and reset kmer1

        # Set graph labels
        # Target is drug target, ex. "AMP"
        graph_label = gr.get_graph_label(km, target)
        feat_dim = node1_label.shape[0]
        labels = {
            'label': graph_label,
            'feat_dim': feat_dim,
        }
        log.debug("Tagging graph with labels: {}".format(labels))
        self.graph.set_graph_labels(labels)

        en = time.time()
        log.debug("Done graphing {}, covering {} kmers in {} s".format(
            km, c, en-st))
        return c

    def save(self, f: str):
        # Drop some nodes due to size constraints
        full_length = len(self.graph)
        self.graph.filter()
        filtered_length = len(self.graph)
        log.debug("After filtering, graph size when from {} to {}".format(
            full_length, filtered_length
        ))
        self.graph.save(f)

