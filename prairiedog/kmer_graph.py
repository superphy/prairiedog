from prairiedog.kmers import Kmers
from prairiedog.graph import Graph


class KmerGraph:
    def __init__(self, km: Kmers, graph: Graph):
        self.km = km
        self.graph = graph
        self._load()

    def _load(self):
        while self.km.has_next:
            header, kmer = self.km.next()
            self.graph.upsert_node(
                kmer,
                {'src': header}
            )
