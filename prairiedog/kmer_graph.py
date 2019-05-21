import time
import logging
import os
import typing
from concurrent.futures import ProcessPoolExecutor, as_completed, Future, wait


import prairiedog.config as config
from prairiedog.kmers import Kmers
from prairiedog.networkx_graph import NetworkXGraph
from prairiedog.graph_ref import GraphRef
from prairiedog.subgraph_ref import SubgraphRef

log = logging.getLogger("prairiedog")


class KmerGraph:
    def __init__(self, km_list: list, k: int = None):
        # Regular class attributes
        # List of genome files to parse into kmers
        if isinstance(km_list, list):
            self.km_list = km_list
        elif isinstance(km_list, str):
            self.km_list = [km_list]
        self.k = k
        # GraphRef
        # This is the max possible number of node ids
        max_n = 4 ** self.k * len(self.km_list)
        # We need to init NumPy arrays for node labels and attributes
        self.gr = GraphRef(max_n)
        # Load call
        self._load()

    def _parse_kmers(self) -> typing.List[Future]:
        with ProcessPoolExecutor() as pool:
            # Use the supplied K if given, otherwise default for Kmers class
            if self.k:
                kmer_futures = [
                    pool.submit(Kmers, f, self.k) for f in self.km_list]
            else:
                kmer_futures = [
                    pool.submit(Kmers, f) for f in self.km_list]
        return kmer_futures

    def _load(self):
        st = time.time()
        files_graphed = 0
        log.info("Starting to create KmerGraph in pid {}".format(os.getpid()))
        kmer_futures = self._parse_kmers()
        log.info("Parsed Kmers for all {} files".format(len(self.km_list)))
        with ProcessPoolExecutor() as pool:
            subgraph_futures = []
            # This creates parallel graphing tasks
            for future in kmer_futures:
                km = future.result()
                # Submit the graphing task to the pool
                subgraph_future = pool.submit(
                    SubgraphRef,
                    self.gr.node_id_count,
                    km,
                    NetworkXGraph()
                )
                # Increment the GraphRef node_id count
                self.gr.incr_node_id(km)
                subgraph_futures.append(subgraph_future)

            # Appends to output files as the subgraphs complete
            for future in as_completed(subgraph_futures):
                subgraph = future.result()
                files_graphed += 1
                log.info("{} / {} done, graphed {}".format(
                    files_graphed, len(self.km_list), subgraph.km))
                self.gr.append(subgraph)
        en = time.time()
        log.info(
            "KmerGraph took {} s to load {} files \
            totaling {} unique kmers/file".format(
                en-st, len(self.km_list), self.gr.node_id_count
            ))
        log.info(
            "This amounts to {} s/file or {} (unique kmers/file)/s".format(
                (en-st)/len(self.km_list), self.gr.node_id_count/(en-st)
            ))
