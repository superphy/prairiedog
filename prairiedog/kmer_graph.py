import time
import logging

from concurrent.futures import ProcessPoolExecutor, as_completed

from prairiedog.kmers import Kmers
from prairiedog.graph import Graph

log = logging.getLogger("prairiedog")


class KmerGraph:
    def __init__(self, km_list: list, graph: Graph):
        # Regular class attributes
        self.km_list = km_list  # List of genome files to parse into kmers
        self.graph = graph
        # Load call
        self._load()

    def _load(self):
        c = 0
        st = time.time()
        with ProcessPoolExecutor() as pool:
            kmer_futures = [pool.submit(Kmers, f) for f in self.km_list]
            # This loads each Kmer into the graph as it completes parsing, and
            # blocks until all Kmers are loaded
            for future in as_completed(kmer_futures):
                km = future.result()
                while km.has_next:
                    header, kmer = km.next()
                    self.graph.upsert_node(
                        kmer,
                        {'src': header}
                    )
                    c += 1
        en = time.time()
        log.info(
            "KmerGraph took {}s to load {} files totaling {} kmers.".format(
                en-st, len(self.km_list), c
            ))
        log.info("This amounts to {}s/file or {}kmers/s.".format(
            (en-st)/len(self.km_list), c/(en-st)
            ))
