import time
import logging
import os

from concurrent.futures import ProcessPoolExecutor, as_completed

from prairiedog.kmers import Kmers
from prairiedog.graph import Graph

log = logging.getLogger("prairiedog")


class KmerGraph:
    def __init__(self, km_list: list, graph: Graph, k: int = None):
        # Regular class attributes
        # List of genome files to parse into kmers
        if isinstance(km_list, list):
            self.km_list = km_list
        elif isinstance(km_list, str):
            self.km_list = [km_list]
        self.graph = graph
        self.k = k
        # Load call
        self._load()

    def _create_graph(self, km: Kmers) -> int:
        log.info("Starting to graph {} in pid {}".format(km, os.getpid()))
        log.debug("Current status: {}".format(self.graph))
        st = time.time()
        c = 0
        while km.has_next:
            header1, kmer1 = km.next()
            # Create the first node
            self.graph.upsert_node(
                kmer1,
                {'src': header1}
            )
            c += 1
            # The same contig still has a kmer
            while km.contig_has_next:
                header2, kmer2 = km.next()
                # Create the second node
                self.graph.upsert_node(
                    kmer2,
                    {'src': header2}
                )
                # Create an edge
                self.graph.add_edge(kmer1, kmer2)
                # Set Kmer2 to Kmer1
                header1, kmer1 = header2, kmer2
                c += 1
            # log.debug("Done processing contig {}".format(header1))
            # At this point, we're out of kmers on that contig
            # The loop will check if there's still kmers, and reset kmer1
        en = time.time()
        log.info("Done graphing {}, covering {} kmers in {} s".format(
            km, c, en-st))
        log.debug("Current status: {}".format(self.graph))
        return c

    def _load(self):
        st = time.time()
        log.info("Starting to create KmerGraph in pid {}".format(os.getpid()))
        with ProcessPoolExecutor() as pool:
            # Use the supplied K if given, otherwise default for Kmers class
            if self.k:
                kmer_futures = [
                    pool.submit(Kmers, f, self.k) for f in self.km_list]
            else:
                kmer_futures = [
                    pool.submit(Kmers, f) for f in self.km_list]
            # This loads each Kmer into the graph as it completes parsing, and
            # blocks until all Kmers are loaded
            for future in as_completed(kmer_futures):
                # Get the Kmer instance from the result
                km = future.result()
                # Graph it
                c = self._create_graph(km)
        en = time.time()
        log.info(
            "KmerGraph took {} s to load {} files totaling {} kmers".format(
                en-st, len(self.km_list), c
            ))
        log.info("This amounts to {} s/file or {} kmers/s".format(
            (en-st)/len(self.km_list), c/(en-st)
            ))
