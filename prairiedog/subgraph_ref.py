import os
import time
import logging

from prairiedog.gref import GRef
from prairiedog.kmers import Kmers
from prairiedog.graph import Graph
from prairiedog.graph_ref import GraphRef
from prairiedog.edge import Edge
from prairiedog.node import Node
from prairiedog.lemon_graph import LGGraph

log = logging.getLogger("prairiedog")


class SubgraphRef(GRef):
    """
    Helper for creating a NetworkX graph, created for each genome file.
    """
    def __init__(self, graph: Graph):
        """
        """
        self.graph = graph

    def __str__(self):
        return "SubgraphRef"

    def update_graph(self,
                     km: Kmers, gr: GraphRef, encode: bool = False) -> int:
        log.debug(
            "Starting to graph {} in pid {}".format(
                km, os.getpid()))
        st = time.time()
        c = 0
        while km.has_next:
            header1, kmer1 = km.next()
            # Create the first node
            node1_label = gr.node_label(kmer1) if encode else kmer1
            if not isinstance(self.graph, LGGraph):
                self.graph.upsert_node(
                    Node(value=node1_label))
            c += 1
            # Used to incrementally encode the edges
            edge_c = 0
            # The same contig still has a kmer
            while km.contig_has_next:
                header2, kmer2 = km.next()
                # Create the second node
                node2_label = gr.node_label(kmer2) if encode else kmer2
                if not isinstance(self.graph, LGGraph):
                    self.graph.upsert_node(
                        Node(node2_label))
                # Create an edge
                try:
                    self.graph.add_edge(
                        Edge(
                            src=node1_label,
                            tgt=node2_label,
                            edge_type=str(km),
                            edge_value=header2,
                            incr=edge_c
                        ),
                        echo=False
                    )
                except Exception as e:
                    log.fatal(
                        "Failed to add edge between {} and {}".format(
                            node1_label, node2_label))
                    raise e
                # Set node1_id to node2_id
                node1_label = node2_label
                c += 1
                edge_c += 1
                if c % 100000 == 0:
                    log.debug("{}/{}, {}%".format(c, len(km), c/len(km)))
            # At this point, we're out of kmers on that contig
            # The loop will check if there's still kmers, and reset kmer1
        en = time.time()
        log.debug("Done graphing {}, covering {} kmers in {} s".format(
            km, c, en - st))
        return c

    def save(self, f: str):
        # Drop some nodes due to size constraints
        # full_length = len(self.graph)
        # self.graph.filter()
        # filtered_length = len(self.graph)
        # log.debug("After filtering, graph size when from {} to {}".format(
        #     full_length, filtered_length
        # ))
        self.graph.save(f)
