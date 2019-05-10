from concurrent.futures import ProcessPoolExecutor, as_completed

from prairiedog.kmers import Kmers
from prairiedog.graph import Graph


class KmerGraph:
    def __init__(self, km_list: list, graph: Graph):
        # Regular class attributes
        self.km_list = km_list  # List of genome files to parse into kmers
        self.graph = graph
        # Load call
        self._load()

    def _load(self):
        with ProcessPoolExecutor() as pool:
            kmer_futures = [pool.submit(Kmers, f) for f in self.km_list]
            # Block until all Kmers loaded
            for km in as_completed(kmer_futures):
                while km.has_next:
                    header, kmer = km.next()
                    self.graph.upsert_node(
                        kmer,
                        {'src': header}
                    )
