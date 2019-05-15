import time
import logging
import os
import pathlib
import datetime

from concurrent.futures import ProcessPoolExecutor, as_completed

from prairiedog.kmers import Kmers
from prairiedog.graph import Graph

log = logging.getLogger("prairiedog")


class GraphRef:
    """
    Helper class to track node ints, etc.
    """
    def __init__(self):
        self.kmer_count = 0
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

    def __iadd__(self, other: int):
        self.kmer_count += other
        return self.kmer_count

    def _setup_folders(self):
        pathlib.Path(self.output_folder).mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _upsert_map(d, value) -> int:
        if value not in d:
            # This starts the MIC label at  1
            d[value] = len(d) + 1
        return d[value]

    def append(self, src_file: str, mic, function, kmer):
        """
        Appends to relevant files. We have to do some mapping to resolve
        strings and other variables into incrementing ints for the models.
        :param function:
        :param src_file:
        :param mic:
        :param kmer:
        :return:
        """
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
        log.debug("Starting to graph {} in pid {}".format(km, os.getpid()))
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
        log.debug("Done graphing {}, covering {} kmers in {} s".format(
            km, c, en-st))
        return c

    def _load(self):
        st = time.time()
        files_graphed = 0
        c = 0
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
                files_graphed += 1
                log.info("{} / {}, graphing {}".format(
                    files_graphed, len(self.km_list), km))
                c += self._create_graph(km)
        en = time.time()
        log.info(
            "KmerGraph took {} s to load {} files totaling {} kmers".format(
                en-st, len(self.km_list), c
            ))
        log.info("This amounts to {} s/file or {} kmers/s".format(
            (en-st)/len(self.km_list), c/(en-st)
            ))
