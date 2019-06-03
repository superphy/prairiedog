import os
import logging
import itertools
import typing

import pandas as pd
import numpy as np

import prairiedog.config as config
from prairiedog.kmers import Kmers
from prairiedog.gref import GRef

log = logging.getLogger("prairiedog")


class GraphRef(GRef):
    """
    Helper class to track node ints, etc.
    """
    def __init__(self, mic_csv=None):
        self.max_num_nodes = 0
        if mic_csv:
            self.MIC_DF = pd.read_csv(mic_csv, index_col=0)
        else:
            self.MIC_DF = pd.read_csv(config.MIC_CSV, index_col=0)
        self.MIC_COLUMNS = self.MIC_DF.columns

        # Used if you want to reverse the graph label to MIC value
        self.mic_map = {}  # MIC value : some int

        # Used to create unique node labels for later one-hot encoding
        self.kmer_map, self.num_unique_node_labels = GraphRef._kmer_map()

    @staticmethod
    def _one_hot(node_label: int, num_unique_node_labels: int) -> np.ndarray:
        node_label_one_hot = [0] * num_unique_node_labels
        node_label_one_hot[node_label] = 1
        return np.array(node_label_one_hot)

    @staticmethod
    def _kmer_map() -> typing.Tuple[dict, int]:
        log.debug("Computing Kmer Map to One-Hot encodings...")
        possible_kmers = [
            ''.join(x) for x in itertools.product('ATCG', repeat=config.K)]
        # While tempting to pre-calculate one-hot encodings here, it takes
        # up way too much RAM.
        d = {
            kmer: i
            for i, kmer in enumerate(possible_kmers)
        }
        log.debug("Done computing Kmer Map to One-Hot encodings")
        return d, len(possible_kmers)

    @staticmethod
    def get_short_name(km: Kmers):
        return os.path.basename(km.filepath).split('.')[0]

    def _record_graph_label(self,  km: Kmers):
        """
        Graph labels are the MIC label. This has to be computed for all our
        chosen genomes so everything is indexed before we create graphs.
        :param km:
        :return:
        """
        short_name = GraphRef.get_short_name(km)
        series = self.MIC_DF.loc[short_name, :]
        for label in self.MIC_COLUMNS:
            mic = series[label]
            self._upsert_map(self.mic_map, mic)

    def index_kmers(self, km: Kmers):
        """
        :param km:
        :return:
        """
        log.info("Indexing {} ...".format(km))

        if km.unique_kmers > self.max_num_nodes:
            self.max_num_nodes = km.unique_kmers

        self._record_graph_label(km)

        log.info("Done indexing {}".format(km))

    def get_graph_label(self, km: Kmers, column: str) -> int:
        short_name = GraphRef.get_short_name(km)
        series = self.MIC_DF.loc[short_name, :]
        mic = series[column]
        return mic

    def get_node_label(self, kmer: str) -> np.ndarray:
        kmer_id = self.kmer_map[kmer]
        one_hot = GraphRef._one_hot(kmer_id, self.num_unique_node_labels)
        return one_hot
