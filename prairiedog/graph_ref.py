import os
import pathlib
import datetime
import logging
import abc
import time

from prairiedog.kmers import Kmers
from prairiedog.graph import Graph

log = logging.getLogger("prairiedog")


class GRef(metaclass=abc.ABCMeta):
    """
    Base class for graph reference classes.
    """
    @staticmethod
    def _upsert_map(d: dict, value: str, prev_value: int = 0) -> int:
        if value not in d:
            # This starts the MIC label at 1 or prev_value + 1
            d[value] = len(d) + prev_value + 1
        return d[value]


class SubgraphRef(GRef):
    """
    Helper for creating a NetworkX graph, created for each genome file.
    """
    def __init__(self, prev_node_id: int, km: Kmers, graph: Graph):
        """
        """
        # This should already be assigned by the previous Kmer
        self.prev_node_id = prev_node_id
        self.km = km
        self.graph = graph
        self.subgraph_kmer_map = {}
        self._create_graph()

    def _create_graph(self) -> int:
        log.debug("Starting to graph {} in pid {}".format(self.km, os.getpid()))
        st = time.time()
        c = 0
        while self.km.has_next:
            header1, kmer1 = self.km.next()
            # Create the first node
            node1_id = self._upsert_map(
                self.subgraph_kmer_map, kmer1, self.prev_node_id)
            self.graph.upsert_node(node1_id)
            c += 1
            # The same contig still has a kmer
            while self.km.contig_has_next:
                header2, kmer2 = self.km.next()
                # Create the second node
                node2_id = self._upsert_map(
                    self.subgraph_kmer_map, kmer1, self.prev_node_id)
                self.graph.upsert_node(node2_id)
                # Create an edge
                self.graph.add_edge(node1_id, node2_id)
                # Set Kmer2 to Kmer1
                header1, kmer1 = header2, kmer2
                c += 1
            # At this point, we're out of kmers on that contig
            # The loop will check if there's still kmers, and reset kmer1
        en = time.time()
        log.debug("Done graphing {}, covering {} kmers in {} s".format(
            self.km, c, en-st))
        return c


class GraphRef(GRef):
    """
    Helper class to track node ints, etc.
    """
    def __init__(self):
        self.node_id_count = 0
        # Reference for all files encountered
        self.file_map = {}
        self.mic_map = {}
        self.kmer_map = {}
        self._setup_folders()
        pf = '{date:%Y-%m-%d_%H-%M-%S}'.format(date=datetime.datetime.now())
        self.output_folder = '/output/{}'.format(pf)
        # Output files
        self.graph_indicator = os.path.join(
            self.output_folder, 'KMERS_graph_indicator.txt')
        self.graph_labels = os.path.join(
            self.output_folder, 'KMERS_graph_labels.txt')
        self.node_labels = os.path.join(
            self.output_folder, 'KMERS_node_labels.txt')
        self.node_attributes = os.path.join(
            self.output_folder, 'KMERS_node_attributes.txt')
        # For user reference, not used in models
        self.file_mapping = os.path.join(
            self.output_folder, 'KMERS_file_mapping.txt')
        self.mic_mapping = os.path.join(
            self.output_folder, 'KMERS_mic_mapping.txt')
        self.kmer_mapping = os.path.join(
            self.output_folder, 'KMERS_kmer_mapping.txt')

    def _setup_folders(self):
        pathlib.Path(self.output_folder).mkdir(parents=True, exist_ok=True)

    def append(self, subgraph: SubgraphRef) -> int:
        """
        Appends to relevant files. We have to do some mapping to resolve
        strings and other variables into incrementing ints for the models.
        This function is called to get the node_id for NetworkX.
        :param function:
        :param src_file:
        :param mic:
        :param kmer:
        :return:
        """
        self.kmer_count += 1

        # Append before so we start at 1
        with open(self.graph_indicator, 'a') as f:
            f.write('{}\n'.format(self._upsert_map(self.file_map, src_file)))

        with open(self.graph_labels, 'a') as f:
            f.write('{}\n'.format(self._upsert_map(self.mic_map, mic)))

        with open(self.node_labels, 'a') as f:
            f.write('{}\n'.format(function))

        with open(self.node_attributes, 'a') as f:
            # Note that we expect to hit ~4^k keys
            f.write('{}\n'.format(self._upsert_map(self.kmer_map, kmer)))

        return self.kmer_count

    def close(self):
        """
        Make sure to write out all mappings for reference
        :return:
        """
        def _write(fl, di):
            with open(fl, 'a') as fil:
                for k, v in di:
                    fil.write('{}, {}\n'.format(k, v))

        _write(self.file_mapping, self.file_map)
        _write(self.mic_mapping, self.mic_map)
        _write(self.kmer_mapping, self.kmer_map)
