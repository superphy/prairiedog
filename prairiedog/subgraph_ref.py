import os
import time
import logging

from prairiedog.gref import GRef
from prairiedog.kmers import Kmers
from prairiedog.graph import Graph

log = logging.getLogger("prairiedog")


class SubgraphRef(GRef):
    """
    Helper for creating a NetworkX graph, created for each genome file.
    """
    def __init__(self, prev_node_id: int, km: Kmers, graph: Graph):
        """
        """
        # This should already be assigned by the previous Kmer, we will use
        # this id + 1 as our starting node
        self.prev_node_id = prev_node_id
        self.graph = graph
        self.subgraph_kmer_map = {}
        self.src = km.filepath
        self._create_graph(km)

    def __str__(self):
        return "SubgraphRef for Kmer source {}".format(self.src)

    def _create_graph(self, km: Kmers) -> int:
        log.debug(
            "Starting to graph {} with starting node id {} in pid {}".format(
                km, self.prev_node_id + 1, os.getpid()))
        st = time.time()
        c = 0
        while km.has_next:
            header1, kmer1 = km.next()
            # Create the first node
            node1_id = self._upsert_map(
                self.subgraph_kmer_map, kmer1, self.prev_node_id)
            self.graph.upsert_node(node1_id)
            c += 1
            # The same contig still has a kmer
            while km.contig_has_next:
                header2, kmer2 = km.next()
                # Create the second node
                node2_id = self._upsert_map(
                    self.subgraph_kmer_map, kmer2, self.prev_node_id)
                self.graph.upsert_node(node2_id)
                # Create an edge
                self.graph.add_edge(node1_id, node2_id)
                # Set node1_id to node2_id
                node1_id = node2_id
                c += 1
            # At this point, we're out of kmers on that contig
            # The loop will check if there's still kmers, and reset kmer1
        en = time.time()
        log.debug("Done graphing {}, covering {} kmers in {} s".format(
            km, c, en-st))
        return c
